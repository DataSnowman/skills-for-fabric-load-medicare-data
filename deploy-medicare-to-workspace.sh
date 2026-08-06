#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# deploy-medicare-to-workspace.sh
#
# Deploy Medicare Part D data into an EXISTING Fabric Workspace.
# Use this when you already have a Resource Group, Fabric Capacity, and
# Workspace provisioned. This script creates a Lakehouse, uploads data,
# deploys notebooks, and loads into a Delta table.
#
# Prerequisites:
#   - Azure CLI installed (az --version)
#   - Logged in (az login)
#   - Python 3 available
#   - An existing Fabric Workspace (with Contributor or higher access)
#   - Local zip files and notebook .ipynb files at the paths below
#
# Usage:
#   1. Set WS_ID (and optionally LAKEHOUSE_NAME) in config/variables.env
#   2. chmod +x deploy-medicare-to-workspace.sh
#   3. ./deploy-medicare-to-workspace.sh
# =============================================================================

# ─── CONFIGURATION ───────────────────────────────────────────────────────────
# Edit config/variables.env — the single place to set WS_ID and LAKEHOUSE_NAME,
# shared with the PowerShell scripts. It is loaded automatically below.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VARS_FILE="$SCRIPT_DIR/config/variables.env"

if [[ -f "$VARS_FILE" ]]; then
  set -a              # export every KEY=value we source
  # shellcheck disable=SC1090
  source "$VARS_FILE"
  set +a
fi

# Defaults (used only if not set in variables.env)
WS_ID="${WS_ID:-}"                           # REQUIRED — your existing Fabric workspace GUID
LAKEHOUSE_NAME="${LAKEHOUSE_NAME:-MedicarePartD}"

# Local paths to zip files and notebooks
ZIP_SOURCE_DIR="$SCRIPT_DIR/data/DemoZippedFiles"
NOTEBOOK_DIR="$SCRIPT_DIR/notebooks"

FILE_PREFIX="Medicare_Part_D_Prescribers_by_Provider_and_Drug"

# Years to process — auto-detected from zip files present in ZIP_SOURCE_DIR
# Drop in 1 to 11 zip files and this will pick them all up automatically
YEARS=()
for _zip in "$ZIP_SOURCE_DIR"/${FILE_PREFIX}_*.zip; do
  [[ -f "$_zip" ]] || continue
  _year=$(basename "$_zip" .zip | grep -oE '[0-9]{4}$')
  [[ -n "$_year" ]] && YEARS+=("$_year")
done
IFS=$'\n' YEARS=($(sort <<<"${YEARS[*]}")); unset IFS

# ─── HELPER FUNCTIONS ────────────────────────────────────────────────────────

log()  { echo ""; echo "=== $1 ==="; }
info() { echo "  → $1"; }
fail() { echo "  ✗ FAILED: $1"; exit 1; }
ok()   { echo "  ✓ $1"; }

# Cross-platform temp directory
TMPDIR="${TMPDIR:-${TEMP:-/tmp}}"

poll_job() {
  local ws_id=$1 item_id=$2 job_id=$3 label=$4 max_polls=${5:-120} interval=${6:-30}
  info "Polling $label (job $job_id)..."
  for i in $(seq 1 "$max_polls"); do
    STATUS=$(az rest --resource "https://api.fabric.microsoft.com" \
      --url "https://api.fabric.microsoft.com/v1/workspaces/$ws_id/items/$item_id/jobs/instances/$job_id" \
      --query "status" --output tsv 2>&1)
    echo "    [$i] $STATUS"
    case "$STATUS" in
      Completed) ok "$label completed"; return 0 ;;
      Failed|Cancelled) fail "$label ended with status: $STATUS" ;;
    esac
    sleep "$interval"
  done
  fail "$label timed out after $((max_polls * interval)) seconds"
}

submit_notebook_job() {
  local ws_id=$1 nb_id=$2
  local resp
  resp=$(az rest --method post \
    --resource "https://api.fabric.microsoft.com" \
    --url "https://api.fabric.microsoft.com/v1/workspaces/$ws_id/items/$nb_id/jobs/instances?jobType=RunNotebook" \
    --body '{}' \
    --verbose 2>&1)
  echo "$resp" | grep "'Location'" | grep -oE '[0-9a-f-]{36}' | tail -1
}

# Returns the job id of an already in-flight (NotStarted/Running) run of this
# notebook, if any, so we never submit a duplicate job on retry/resume.
get_active_job() {
  local ws_id=$1 item_id=$2
  az rest --resource "https://api.fabric.microsoft.com" \
    --url "https://api.fabric.microsoft.com/v1/workspaces/$ws_id/items/$item_id/jobs/instances?jobType=RunNotebook" \
    --query "value[?status=='NotStarted' || status=='Running'].id | [0]" \
    --output tsv 2>/dev/null || echo ""
}

# Runs a notebook, reusing an in-flight job instead of submitting a duplicate.
# If job-id parsing after submission fails transiently, we poll for the job
# to appear rather than blindly resubmitting (which would create a real
# duplicate Spark run).
run_notebook_job() {
  local ws_id=$1 nb_id=$2 label=$3 max_polls=$4 interval=$5
  local job_id
  job_id=$(get_active_job "$ws_id" "$nb_id")
  if [[ -n "$job_id" ]]; then
    info "$label already has an in-flight job, reattaching instead of resubmitting"
  else
    job_id=$(submit_notebook_job "$ws_id" "$nb_id")
    if [[ -z "$job_id" ]]; then
      info "Could not parse job id for $label submission, checking for the job before retrying..."
      for i in {1..6}; do
        sleep 5
        job_id=$(get_active_job "$ws_id" "$nb_id")
        [[ -n "$job_id" ]] && break
      done
      # Only resubmit if we're sure no job was actually created.
      [[ -n "$job_id" ]] || job_id=$(submit_notebook_job "$ws_id" "$nb_id")
    fi
  fi
  [[ -n "$job_id" ]] || fail "Could not submit $label job"
  poll_job "$ws_id" "$nb_id" "$job_id" "$label" "$max_polls" "$interval"
}

# ─── STEP 0: PREFLIGHT ──────────────────────────────────────────────────────

log "Step 0 — Preflight checks"

az account show > /dev/null 2>&1 || fail "Not logged in. Run 'az login' first."
ADMIN_EMAIL=$(az account show --query user.name --output tsv)
ok "Logged in as $ADMIN_EMAIL"

[[ -n "$WS_ID" ]] || fail "WS_ID is empty. Set your existing Workspace ID in the CONFIGURATION section."

# Verify workspace exists and is accessible
WS_NAME=$(az rest --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID" \
  --query "displayName" --output tsv 2>&1) || fail "Cannot access workspace $WS_ID. Check the ID and your permissions."
ok "Workspace: $WS_NAME ($WS_ID)"

# Verify the workspace's assigned capacity is Active (never creates one)
CAPACITY_ID=$(az rest --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID" \
  --query "capacityId" --output tsv 2>&1) || fail "Could not read capacity for workspace $WS_ID"
if [[ -n "$CAPACITY_ID" && "$CAPACITY_ID" != "None" ]]; then
  CAPACITY_STATE=$(az rest --resource "https://api.fabric.microsoft.com" \
    --url "https://api.fabric.microsoft.com/v1/capacities" \
    --query "value[?id=='$CAPACITY_ID'].state | [0]" --output tsv 2>&1)
  [[ "$CAPACITY_STATE" == "Active" ]] || fail "Workspace capacity $CAPACITY_ID is not Active (state: $CAPACITY_STATE). Resume it before deploying; this script will not create/modify capacities."
  ok "Workspace capacity is Active ($CAPACITY_ID)"
else
  fail "Workspace $WS_ID has no assigned capacity. Assign an existing capacity before deploying."
fi

[[ -d "$ZIP_SOURCE_DIR" ]] || fail "Zip directory not found: $ZIP_SOURCE_DIR"
ZIP_COUNT=$(ls "$ZIP_SOURCE_DIR"/*.zip 2>/dev/null | wc -l | tr -d ' ')
[[ ${#YEARS[@]} -gt 0 ]] || fail "No zip files matching ${FILE_PREFIX}_YYYY.zip found in $ZIP_SOURCE_DIR"
ok "Found $ZIP_COUNT zip file(s) in $ZIP_SOURCE_DIR — years: ${YEARS[*]}"

[[ -d "$NOTEBOOK_DIR" ]] || fail "Notebook directory not found: $NOTEBOOK_DIR"
[[ -f "$NOTEBOOK_DIR/UnzipMedicareFiles.ipynb" ]] || fail "UnzipMedicareFiles.ipynb not found"
[[ -f "$NOTEBOOK_DIR/LoadMedicarePartDfiles.ipynb" ]] || fail "LoadMedicarePartDfiles.ipynb not found"
ok "Both notebooks found"

# ─── STEP 1: CREATE LAKEHOUSE ───────────────────────────────────────────────

log "Step 1 — Create Lakehouse ($LAKEHOUSE_NAME)"

LH_ID=$(az rest --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items" \
  --query "value[?displayName=='$LAKEHOUSE_NAME' && type=='Lakehouse'].id | [0]" --output tsv 2>/dev/null || echo "")

if [[ -n "$LH_ID" ]]; then
  ok "Lakehouse already exists, reusing: $LH_ID"
else
  LH_ID=$(az rest --method post \
    --resource "https://api.fabric.microsoft.com" \
    --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items" \
    --body "{\"displayName\": \"$LAKEHOUSE_NAME\", \"type\": \"Lakehouse\", \"creationPayload\": {\"enableSchemas\": true}}" \
    --query "id" --output tsv)
  ok "Lakehouse created: $LH_ID"
fi

# ─── STEP 2: UPLOAD ZIP FILES TO ONELAKE ────────────────────────────────────

log "Step 2 — Upload zip files to OneLake (blob endpoint)"

STORAGE_TOKEN=$(az account get-access-token \
  --resource "https://storage.azure.com" \
  --query accessToken --output tsv)

UPLOAD_FAILURES=0
UPLOAD_SKIPPED=0
for ZIP_FILE in "$ZIP_SOURCE_DIR"/*.zip; do
  FILENAME=$(basename "$ZIP_FILE")
  LOCAL_SIZE=$(stat -f%z "$ZIP_FILE" 2>/dev/null || stat --printf="%s" "$ZIP_FILE")
  SIZE_MB=$(( LOCAL_SIZE / 1024 / 1024 ))

  # Skip re-upload if a blob of the same size already exists (recovers from
  # transient re-runs without re-uploading hundreds of MB unnecessarily; a
  # size mismatch — e.g. a prior partial upload — triggers a re-upload).
  REMOTE_SIZE=$(curl -s -I \
    -H "Authorization: Bearer $STORAGE_TOKEN" \
    -H "x-ms-version: 2023-01-03" \
    "https://onelake.blob.fabric.microsoft.com/$WS_ID/$LH_ID/Files/medicare/$FILENAME" \
    | tr -d '\r' | { grep -i '^content-length:' || true; } | awk '{print $2}')

  if [[ -n "$REMOTE_SIZE" && "$REMOTE_SIZE" == "$LOCAL_SIZE" ]]; then
    echo "  Skipping $FILENAME (${SIZE_MB}MB) — already uploaded"
    UPLOAD_SKIPPED=$((UPLOAD_SKIPPED + 1))
    continue
  fi

  echo -n "  Uploading $FILENAME (${SIZE_MB}MB)... "

  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT \
    -H "Authorization: Bearer $STORAGE_TOKEN" \
    -H "x-ms-version: 2023-01-03" \
    -H "x-ms-blob-type: BlockBlob" \
    --data-binary @"$ZIP_FILE" \
    "https://onelake.blob.fabric.microsoft.com/$WS_ID/$LH_ID/Files/medicare/$FILENAME")

  echo "$HTTP_CODE"
  if [[ "$HTTP_CODE" != "201" ]]; then
    echo -n "  Retrying $FILENAME... "
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT \
      -H "Authorization: Bearer $STORAGE_TOKEN" \
      -H "x-ms-version: 2023-01-03" \
      -H "x-ms-blob-type: BlockBlob" \
      --data-binary @"$ZIP_FILE" \
      "https://onelake.blob.fabric.microsoft.com/$WS_ID/$LH_ID/Files/medicare/$FILENAME")
    echo "$HTTP_CODE"
  fi
  [[ "$HTTP_CODE" == "201" ]] || UPLOAD_FAILURES=$((UPLOAD_FAILURES + 1))
done

[[ $UPLOAD_FAILURES -eq 0 ]] || fail "$UPLOAD_FAILURES file(s) failed to upload"
ok "All zip files present in OneLake ($UPLOAD_SKIPPED skipped, already uploaded)"

# ─── STEP 3: PREPARE AND DEPLOY NOTEBOOKS ───────────────────────────────────

log "Step 3 — Prepare and deploy notebooks with lakehouse binding"

python3 << PYEOF
import json, base64, uuid, os

ws_id = "$WS_ID"
lh_id = "$LH_ID"
lh_name = "$LAKEHOUSE_NAME"
nb_dir = "$NOTEBOOK_DIR"
tmpdir = "$TMPDIR"
years = [$(IFS=,; echo "${YEARS[*]}")]
file_prefix = "$FILE_PREFIX"

# Lakehouse dependency block to inject into notebook metadata
lakehouse_deps = {
    "lakehouse": {
        "default_lakehouse": lh_id,
        "default_lakehouse_name": lh_name,
        "default_lakehouse_workspace_id": ws_id,
        "known_lakehouses": [{"id": lh_id}]
    }
}

# --- UnzipMedicareFiles: update zip_files list to all years ---
with open(os.path.join(nb_dir, 'UnzipMedicareFiles.ipynb'), 'r') as f:
    unzip_nb = json.load(f)

zip_list = [f"/lakehouse/default/Files/medicare/{file_prefix}_{y}.zip" for y in years]
zip_lines = ",\n".join([f"    '{z}'" for z in zip_list])

for cell in unzip_nb['cells']:
    src = ''.join(cell['source'])
    if 'zip_files' in src and 'zipfile' in src:
        src_lines = src.split('\n')
        new_lines = []
        skip = False
        for line in src_lines:
            if 'zip_files = [' in line:
                new_lines.append('zip_files = [')
                new_lines.append(zip_lines)
                new_lines.append(']')
                skip = True
                continue
            if skip:
                if line.strip() == ']':
                    skip = False
                continue
            new_lines.append(line)
        cell['source'] = [l + '\n' for l in new_lines]
        if cell['source'][-1].strip() == '':
            cell['source'] = cell['source'][:-1]
        break

unzip_nb['metadata']['dependencies'] = lakehouse_deps

# --- LoadMedicarePartDfiles: update full_files list to all years ---
with open(os.path.join(nb_dir, 'LoadMedicarePartDfiles.ipynb'), 'r') as f:
    load_nb = json.load(f)

csv_list = [f"{file_prefix}_{y}.csv" for y in years]
csv_lines = ",\n".join([f"    '{c}'" for c in csv_list])

for cell in load_nb['cells']:
    src = ''.join(cell['source'])
    if 'full_files' in src and 'loadFullDataFromSource' in src:
        src_lines = src.split('\n')
        new_lines = []
        skip = False
        for line in src_lines:
            if 'full_files = [' in line:
                new_lines.append('full_files = [')
                new_lines.append(csv_lines)
                new_lines.append(']')
                skip = True
                continue
            if skip:
                if line.strip() == ']':
                    skip = False
                continue
            new_lines.append(line)
        cell['source'] = [l + '\n' for l in new_lines]
        if cell['source'][-1].strip() == '':
            cell['source'] = cell['source'][:-1]
        break

load_nb['metadata']['dependencies'] = lakehouse_deps

# --- Build deploy bodies ---
for nb, name in [(unzip_nb, 'UnzipMedicareFiles'), (load_nb, 'LoadMedicarePartDfiles')]:
    nb_b64 = base64.b64encode(json.dumps(nb).encode()).decode()
    body = {
        "displayName": name,
        "type": "Notebook",
        "definition": {
            "format": "ipynb",
            "parts": [
                {"path": "artifact.content.ipynb", "payload": nb_b64, "payloadType": "InlineBase64"}
            ]
        }
    }
    with open(f'{tmpdir}/{name}_deploy_body.json', 'w') as f:
        json.dump(body, f)
    print(f"  ✓ {name} deploy body ready")

# --- Build updateDefinition bodies (for lakehouse binding after create) ---
for nb, name in [(unzip_nb, 'UnzipMedicareFiles'), (load_nb, 'LoadMedicarePartDfiles')]:
    nb_b64 = base64.b64encode(json.dumps(nb).encode()).decode()
    platform = {
        "metadata": {"type": "SparkNotebook", "displayName": name},
        "config": {"version": "2.0", "logicalId": str(uuid.uuid4())}
    }
    platform_b64 = base64.b64encode(json.dumps(platform).encode()).decode()
    body = {
        "definition": {
            "format": "ipynb",
            "parts": [
                {"path": "artifact.content.ipynb", "payload": nb_b64, "payloadType": "InlineBase64"},
                {"path": ".platform", "payload": platform_b64, "payloadType": "InlineBase64"}
            ]
        }
    }
    with open(f'{tmpdir}/{name}_update_body.json', 'w') as f:
        json.dump(body, f)
    print(f"  ✓ {name} update body ready")

PYEOF

deploy_or_update_notebook() {
  local name=$1
  local nb_id
  nb_id=$(az rest --resource "https://api.fabric.microsoft.com" \
    --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/notebooks" \
    --query "value[?displayName=='$name'].id | [0]" --output tsv 2>/dev/null || echo "")

  if [[ -n "$nb_id" ]]; then
    info "$name already exists, updating definition..." >&2
    az rest --method post \
      --resource "https://api.fabric.microsoft.com" \
      --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/notebooks/$nb_id/updateDefinition" \
      --body "$(cat $TMPDIR/${name}_update_body.json)" > /dev/null 2>&1
    ok "$name updated: $nb_id" >&2
  else
    info "Deploying $name..." >&2
    az rest --method post \
      --resource "https://api.fabric.microsoft.com" \
      --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items" \
      --body "$(cat $TMPDIR/${name}_deploy_body.json)" > /dev/null 2>&1

    # Retry until notebook appears (may take a few seconds after creation)
    for i in {1..10}; do
      nb_id=$(az rest --resource "https://api.fabric.microsoft.com" \
        --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/notebooks" \
        --query "value[?displayName=='$name'].id | [0]" --output tsv 2>/dev/null || echo "")
      [[ -n "$nb_id" ]] && break
      sleep 5
    done
    [[ -n "$nb_id" ]] || { echo "  ✗ FAILED: Could not retrieve ID for $name after deployment" >&2; exit 1; }
    ok "$name deployed: $nb_id" >&2

    info "Binding $name to lakehouse..." >&2
    az rest --method post \
      --resource "https://api.fabric.microsoft.com" \
      --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/notebooks/$nb_id/updateDefinition" \
      --body "$(cat $TMPDIR/${name}_update_body.json)" > /dev/null 2>&1
    ok "$name bound to lakehouse" >&2
  fi
  echo "$nb_id"
}

UNZIP_NB_ID=$(deploy_or_update_notebook "UnzipMedicareFiles")
LOAD_NB_ID=$(deploy_or_update_notebook "LoadMedicarePartDfiles")

# Checks whether every year's raw CSV has already been unzipped into the
# lakehouse, so a re-run doesn't redo (or duplicate-run) the unzip job.
all_raw_csvs_present() {
  local listing
  listing=$(curl -s -H "Authorization: Bearer $STORAGE_TOKEN" \
    -H "x-ms-version: 2023-01-03" \
    "https://onelake.blob.fabric.microsoft.com/$WS_ID/$LH_ID/Files?restype=container&comp=list&prefix=medicare&maxresults=200")
  for Y in "${YEARS[@]}"; do
    echo "$listing" | grep -q "${FILE_PREFIX}_${Y}.csv" || return 1
  done
  return 0
}

# ─── STEP 4: RUN UNZIP NOTEBOOK ─────────────────────────────────────────────

log "Step 4 — Run UnzipMedicareFiles notebook"

STORAGE_TOKEN=$(az account get-access-token \
  --resource "https://storage.azure.com" \
  --query accessToken --output tsv)

if all_raw_csvs_present; then
  ok "Raw CSVs for all years (${YEARS[*]}) already present, skipping unzip run"
else
  run_notebook_job "$WS_ID" "$UNZIP_NB_ID" "UnzipMedicareFiles" 60 30
fi

# ─── STEP 5: RUN LOAD NOTEBOOK ──────────────────────────────────────────────

log "Step 5 — Run LoadMedicarePartDfiles notebook"

run_notebook_job "$WS_ID" "$LOAD_NB_ID" "LoadMedicarePartDfiles" 120 30

# ─── STEP 6: VERIFY ─────────────────────────────────────────────────────────

log "Step 6 — Verify Delta table"

STORAGE_TOKEN=$(az account get-access-token \
  --resource "https://storage.azure.com" \
  --query accessToken --output tsv)

TABLE_CHECK=$(curl -s -H "Authorization: Bearer $STORAGE_TOKEN" \
  -H "x-ms-version: 2023-01-03" \
  "https://onelake.blob.fabric.microsoft.com/$WS_ID/$LH_ID/Tables?restype=container&comp=list&prefix=mcpd&maxresults=5")

if echo "$TABLE_CHECK" | grep -q "medicarepartd"; then
  ok "Delta table mcpd.medicarepartd exists"
else
  fail "Delta table not found"
fi

# ─── SUMMARY ─────────────────────────────────────────────────────────────────

log "DEPLOYMENT COMPLETE"
echo ""
echo "  Workspace: $WS_NAME ($WS_ID)"
echo "  Lakehouse: $LAKEHOUSE_NAME ($LH_ID)"
echo "  Unzip NB:  UnzipMedicareFiles ($UNZIP_NB_ID)"
echo "  Load NB:   LoadMedicarePartDfiles ($LOAD_NB_ID)"
echo "  Table:     mcpd.medicarepartd"
echo ""
echo "  To query in Fabric SQL:"
echo "    SELECT [year], count(*) as numberofrows"
echo "    FROM [$LAKEHOUSE_NAME].[mcpd].[medicarepartd]"
echo "    GROUP BY [year]"
echo ""

# Clean up temp files
rm -f "$TMPDIR"/*_deploy_body.json "$TMPDIR"/*_update_body.json
