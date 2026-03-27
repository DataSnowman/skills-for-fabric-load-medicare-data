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
# Edit these values before running

RESOURCE_GROUP="FabricCapacityWestUS3"
LOCATION="westus3"
SKU="F4"
CAPACITY_NAME="westus3f4skillsfghcpcli"
WORKSPACE_NAME="MedicareSkillsF4ghcpcli"
LAKEHOUSE_NAME="MedicareSkillsF4TerminalLHghcpcli"

# Local paths to zip files and notebooks
ZIP_SOURCE_DIR="/Users/darwinschweitzer/sourceData/MedicarePartD/data/DemoZippedFiles"
NOTEBOOK_DIR="/Users/darwinschweitzer/sourceData/MedicarePartD/code/notebook"

# All 11 years of Medicare Part D files
YEARS=(2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023)
FILE_PREFIX="Medicare_Part_D_Prescribers_by_Provider_and_Drug"

# ─── HELPER FUNCTIONS ────────────────────────────────────────────────────────

log()  { echo ""; echo "=== $1 ==="; }
info() { echo "  → $1"; }
fail() { echo "  ✗ FAILED: $1"; exit 1; }
ok()   { echo "  ✓ $1"; }

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
ok "Found $ZIP_COUNT zip files in $ZIP_SOURCE_DIR"

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
  }" > /dev/null 2>&1 || fail "Could not create capacity (may already exist or name conflict)"

# Wait for provisioning
info "Waiting for capacity to provision..."
for i in {1..30}; do
  STATE=$(az rest \
    --url "https://management.azure.com/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Fabric/capacities/$CAPACITY_NAME?api-version=2023-11-01" \
    --query "properties.provisioningState" --output tsv 2>&1)
  echo "    [$i] $STATE"
  [[ "$STATE" == "Succeeded" ]] && break
  sleep 10
done
[[ "$STATE" == "Succeeded" ]] || fail "Capacity provisioning did not succeed: $STATE"

# Get Fabric-scoped capacity ID
FABRIC_CAPACITY_ID=$(az rest \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/capacities" \
  --query "value[?displayName=='$CAPACITY_NAME'].id | [0]" --output tsv)

ok "Capacity ID: $FABRIC_CAPACITY_ID"

# ─── STEP 3: CREATE WORKSPACE ───────────────────────────────────────────────

log "Step 3 — Create Workspace ($WORKSPACE_NAME)"

WS_ID=$(az rest --method post \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces" \
  --body "{\"displayName\": \"$WORKSPACE_NAME\", \"capacityId\": \"$FABRIC_CAPACITY_ID\"}" \
  --query "id" --output tsv)

ok "Workspace ID: $WS_ID"

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

# ─── STEP 4: CREATE LAKEHOUSE ───────────────────────────────────────────────

log "Step 4 — Create Lakehouse ($LAKEHOUSE_NAME)"

LH_ID=$(az rest --method post \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items" \
  --body "{\"displayName\": \"$LAKEHOUSE_NAME\", \"type\": \"Lakehouse\", \"creationPayload\": {\"enableSchemas\": true}}" \
  --query "id" --output tsv)

ok "Lakehouse ID: $LH_ID"

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
    with open(f'/tmp/{name}_deploy_body.json', 'w') as f:
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
    with open(f'/tmp/{name}_update_body.json', 'w') as f:
        json.dump(body, f)
    print(f"  ✓ {name} update body ready")

PYEOF

# Deploy UnzipMedicareFiles
info "Deploying UnzipMedicareFiles..."
az rest --method post \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items" \
  --body @/tmp/UnzipMedicareFiles_deploy_body.json > /dev/null 2>&1

UNZIP_NB_ID=$(az rest --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/notebooks" \
  --query "value[?displayName=='UnzipMedicareFiles'].id | [0]" --output tsv)
ok "UnzipMedicareFiles deployed: $UNZIP_NB_ID"

# Deploy LoadMedicarePartDfiles
info "Deploying LoadMedicarePartDfiles..."
az rest --method post \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items" \
  --body @/tmp/LoadMedicarePartDfiles_deploy_body.json > /dev/null 2>&1

LOAD_NB_ID=$(az rest --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/notebooks" \
  --query "value[?displayName=='LoadMedicarePartDfiles'].id | [0]" --output tsv)
ok "LoadMedicarePartDfiles deployed: $LOAD_NB_ID"

# Bind both to lakehouse via updateDefinition
info "Binding notebooks to lakehouse..."
az rest --method post \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/notebooks/$UNZIP_NB_ID/updateDefinition" \
  --body @/tmp/UnzipMedicareFiles_update_body.json > /dev/null 2>&1
ok "UnzipMedicareFiles bound to lakehouse"

az rest --method post \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/notebooks/$LOAD_NB_ID/updateDefinition" \
  --body @/tmp/LoadMedicarePartDfiles_update_body.json > /dev/null 2>&1
ok "LoadMedicarePartDfiles bound to lakehouse"

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
rm -f /tmp/*_deploy_body.json /tmp/*_update_body.json
