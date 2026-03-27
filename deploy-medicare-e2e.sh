#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# deploy-medicare-e2e.sh
#
# End-to-end script to provision an Azure Resource Group, Fabric Capacity,
# Workspace, and Lakehouse, then upload Medicare Part D zip files, deploy
# notebooks, and load data into a Delta table.
#
# Prerequisites:
#   - Azure CLI installed (az --version)
#   - Logged in (az login)
#   - Python 3 available
#   - Local zip files and notebook .ipynb files at the paths below
#
# Usage:
#   1. Edit the CONFIGURATION section below
#   2. chmod +x deploy-medicare-e2e.sh
#   3. ./deploy-medicare-e2e.sh
# =============================================================================

# ─── CONFIGURATION ───────────────────────────────────────────────────────────
# Values are loaded from config/variables.md (bash code block).
# You can override any value by setting it here after the source block.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VARS_FILE="$SCRIPT_DIR/config/variables.md"

if [[ -f "$VARS_FILE" ]]; then
  # Extract the bash code block from variables.md using Python (BSD sed-safe)
  _vars=$(python3 - "$VARS_FILE" <<'PYEOF'
import sys, re
content = open(sys.argv[1]).read()
blocks = re.findall(r'```bash\n(.*?)```', content, re.DOTALL)
for block in blocks:
    for line in block.splitlines():
        line = line.strip()
        if re.match(r'^[A-Z_]+=', line) and not line.startswith('#'):
            print(line)
PYEOF
)
  eval "$_vars" 2>/dev/null || true
fi

# ── Override / set defaults for values not in variables.md ──
RESOURCE_GROUP="${RESOURCE_GROUP:-FabricCapacityWestUS3}"
LOCATION="${LOCATION:-westus3}"
SKU="${SKU:-F4}"
CAPACITY_NAME="${CAPACITY_NAME:-westus3f4skillsfghcpclineo}"
WORKSPACE_NAME="${WORKSPACE_NAME:-MedicareSkillsF4ghcpclineo}"
LAKEHOUSE_NAME="${LAKEHOUSE_NAME:-MedicareSkillsF4TerminalLHghcpclineo}"

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
[[ ${#YEARS[@]} -gt 0 ]] || { echo "  ✗ FAILED: No zip files matching ${FILE_PREFIX}_YYYY.zip found in $ZIP_SOURCE_DIR"; exit 1; }

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
  az rest --method post \
    --resource "https://api.fabric.microsoft.com" \
    --url "https://api.fabric.microsoft.com/v1/workspaces/$ws_id/items/$nb_id/jobs/instances?jobType=RunNotebook" \
    --body '{}' \
    --verbose 2>&1 | grep "'Location'" | grep -oE '[0-9a-f-]{36}' | tail -1
}

# ─── STEP 0: PREFLIGHT ──────────────────────────────────────────────────────

log "Step 0 — Preflight checks"

az account show > /dev/null 2>&1 || fail "Not logged in. Run 'az login' first."
SUBSCRIPTION_ID=$(az account show --query id --output tsv)
ADMIN_EMAIL=$(az account show --query user.name --output tsv)
ok "Logged in as $ADMIN_EMAIL (subscription: $SUBSCRIPTION_ID)"

[[ -d "$ZIP_SOURCE_DIR" ]] || fail "Zip directory not found: $ZIP_SOURCE_DIR"
ZIP_COUNT=$(ls "$ZIP_SOURCE_DIR"/*.zip 2>/dev/null | wc -l | tr -d ' ')
ok "Found $ZIP_COUNT zip file(s) in $ZIP_SOURCE_DIR — years: ${YEARS[*]}"

[[ -d "$NOTEBOOK_DIR" ]] || fail "Notebook directory not found: $NOTEBOOK_DIR"
[[ -f "$NOTEBOOK_DIR/UnzipMedicareFiles.ipynb" ]] || fail "UnzipMedicareFiles.ipynb not found"
[[ -f "$NOTEBOOK_DIR/LoadMedicarePartDfiles.ipynb" ]] || fail "LoadMedicarePartDfiles.ipynb not found"
ok "Both notebooks found"

# ─── STEP 1: CREATE RESOURCE GROUP ──────────────────────────────────────────

log "Step 1 — Create Resource Group ($RESOURCE_GROUP in $LOCATION)"

EXISTING_RG=$(az group show --name "$RESOURCE_GROUP" --query "name" --output tsv 2>/dev/null || echo "")
if [[ -n "$EXISTING_RG" ]]; then
  ok "Resource Group already exists, skipping creation"
else
  az group create --name "$RESOURCE_GROUP" --location "$LOCATION" --output none
  ok "Resource Group created"
fi

# ─── STEP 2: CREATE FABRIC CAPACITY ─────────────────────────────────────────

log "Step 2 — Create Fabric Capacity ($CAPACITY_NAME, $SKU in $LOCATION)"

EXISTING_STATE=$(az rest \
  --url "https://management.azure.com/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Fabric/capacities/$CAPACITY_NAME?api-version=2023-11-01" \
  --query "properties.state" --output tsv 2>/dev/null || echo "")

if [[ -n "$EXISTING_STATE" ]]; then
  ok "Capacity already exists (state: $EXISTING_STATE)"
  if [[ "$EXISTING_STATE" == "Paused" ]]; then
    info "Resuming paused capacity..."
    az rest --method post \
      --url "https://management.azure.com/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Fabric/capacities/$CAPACITY_NAME/resume?api-version=2023-11-01" \
      > /dev/null 2>&1 || fail "Could not resume capacity"
  fi
else
  az rest --method put \
    --url "https://management.azure.com/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Fabric/capacities/$CAPACITY_NAME?api-version=2023-11-01" \
    --body "{
      \"location\": \"$LOCATION\",
      \"sku\": {\"name\": \"$SKU\", \"tier\": \"Fabric\"},
      \"properties\": {
        \"administration\": {
          \"members\": [\"$ADMIN_EMAIL\"]
        }
      }
    }" > /dev/null 2>&1 || fail "Could not create capacity"
fi

# Wait for provisioning/resuming
info "Waiting for capacity to be ready..."
for i in {1..30}; do
  STATE=$(az rest \
    --url "https://management.azure.com/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Fabric/capacities/$CAPACITY_NAME?api-version=2023-11-01" \
    --query "properties.state" --output tsv 2>&1)
  echo "    [$i] $STATE"
  [[ "$STATE" == "Active" ]] && break
  sleep 10
done
[[ "$STATE" == "Active" ]] || fail "Capacity not active: $STATE"

# Get Fabric-scoped capacity ID
FABRIC_CAPACITY_ID=$(az rest \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/capacities" \
  --query "value[?displayName=='$CAPACITY_NAME'].id | [0]" --output tsv)

ok "Capacity ID: $FABRIC_CAPACITY_ID"

# ─── STEP 3: CREATE WORKSPACE ───────────────────────────────────────────────

log "Step 3 — Create Workspace ($WORKSPACE_NAME)"

WS_ID=$(az rest --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces" \
  --query "value[?displayName=='$WORKSPACE_NAME'].id | [0]" --output tsv 2>/dev/null || echo "")

if [[ -n "$WS_ID" ]]; then
  ok "Workspace already exists: $WS_ID"
else
  WS_ID=$(az rest --method post \
    --resource "https://api.fabric.microsoft.com" \
    --url "https://api.fabric.microsoft.com/v1/workspaces" \
    --body "{\"displayName\": \"$WORKSPACE_NAME\", \"capacityId\": \"$FABRIC_CAPACITY_ID\"}" \
    --query "id" --output tsv)
  ok "Workspace created: $WS_ID"

  # Verify capacity assignment
  info "Verifying capacity assignment..."
  for i in {1..10}; do
    PROGRESS=$(az rest --resource "https://api.fabric.microsoft.com" \
      --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID" \
      --query "capacityAssignmentProgress" --output tsv)
    [[ "$PROGRESS" == "Completed" ]] && break
    sleep 5
  done
  [[ "$PROGRESS" == "Completed" ]] || fail "Capacity assignment not completed: $PROGRESS"
  ok "Capacity assignment completed"
fi

# ─── STEP 4: CREATE LAKEHOUSE ───────────────────────────────────────────────

log "Step 4 — Create Lakehouse ($LAKEHOUSE_NAME)"

LH_ID=$(az rest --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items" \
  --query "value[?displayName=='$LAKEHOUSE_NAME' && type=='Lakehouse'].id | [0]" --output tsv 2>/dev/null || echo "")

if [[ -n "$LH_ID" ]]; then
  ok "Lakehouse already exists: $LH_ID"
else
  LH_ID=$(az rest --method post \
    --resource "https://api.fabric.microsoft.com" \
    --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items" \
    --body "{\"displayName\": \"$LAKEHOUSE_NAME\", \"type\": \"Lakehouse\", \"creationPayload\": {\"enableSchemas\": true}}" \
    --query "id" --output tsv)
  ok "Lakehouse created: $LH_ID"
fi

# ─── STEP 5: UPLOAD ZIP FILES TO ONELAKE ────────────────────────────────────

log "Step 5 — Upload zip files to OneLake (blob endpoint)"

STORAGE_TOKEN=$(az account get-access-token \
  --resource "https://storage.azure.com" \
  --query accessToken --output tsv)

UPLOAD_FAILURES=0
for ZIP_FILE in "$ZIP_SOURCE_DIR"/*.zip; do
  FILENAME=$(basename "$ZIP_FILE")
  SIZE_MB=$(( $(stat -f%z "$ZIP_FILE" 2>/dev/null || stat --printf="%s" "$ZIP_FILE") / 1024 / 1024 ))
  echo -n "  Uploading $FILENAME (${SIZE_MB}MB)... "

  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT \
    -H "Authorization: Bearer $STORAGE_TOKEN" \
    -H "x-ms-version: 2023-01-03" \
    -H "x-ms-blob-type: BlockBlob" \
    --data-binary @"$ZIP_FILE" \
    "https://onelake.blob.fabric.microsoft.com/$WS_ID/$LH_ID/Files/medicare/$FILENAME")

  echo "$HTTP_CODE"
  [[ "$HTTP_CODE" == "201" ]] || UPLOAD_FAILURES=$((UPLOAD_FAILURES + 1))
done

[[ $UPLOAD_FAILURES -eq 0 ]] || fail "$UPLOAD_FAILURES file(s) failed to upload"
ok "All zip files uploaded"

# ─── STEP 6: PREPARE AND DEPLOY NOTEBOOKS ───────────────────────────────────

log "Step 6 — Prepare and deploy notebooks with lakehouse binding"

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
      --body @$TMPDIR/${name}_update_body.json > /dev/null 2>&1
    ok "$name updated: $nb_id" >&2
  else
    info "Deploying $name..." >&2
    az rest --method post \
      --resource "https://api.fabric.microsoft.com" \
      --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items" \
      --body @$TMPDIR/${name}_deploy_body.json > /dev/null 2>&1

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
      --body @$TMPDIR/${name}_update_body.json > /dev/null 2>&1
    ok "$name bound to lakehouse" >&2
  fi
  echo "$nb_id"
}

UNZIP_NB_ID=$(deploy_or_update_notebook "UnzipMedicareFiles")
LOAD_NB_ID=$(deploy_or_update_notebook "LoadMedicarePartDfiles")

# ─── STEP 7: RUN UNZIP NOTEBOOK ─────────────────────────────────────────────

log "Step 7 — Run UnzipMedicareFiles notebook"

UNZIP_JOB_ID=$(submit_notebook_job "$WS_ID" "$UNZIP_NB_ID")
[[ -n "$UNZIP_JOB_ID" ]] || fail "Could not submit unzip notebook job"
poll_job "$WS_ID" "$UNZIP_NB_ID" "$UNZIP_JOB_ID" "UnzipMedicareFiles" 60 30

# ─── STEP 8: RUN LOAD NOTEBOOK ──────────────────────────────────────────────

log "Step 8 — Run LoadMedicarePartDfiles notebook"

LOAD_JOB_ID=$(submit_notebook_job "$WS_ID" "$LOAD_NB_ID")
[[ -n "$LOAD_JOB_ID" ]] || fail "Could not submit load notebook job"
poll_job "$WS_ID" "$LOAD_NB_ID" "$LOAD_JOB_ID" "LoadMedicarePartDfiles" 120 30

# ─── STEP 9: VERIFY ─────────────────────────────────────────────────────────

log "Step 9 — Verify Delta table"

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
echo "  Capacity:  $CAPACITY_NAME  ($FABRIC_CAPACITY_ID)"
echo "  Workspace: $WORKSPACE_NAME ($WS_ID)"
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
