# Load Medicare Part D Data — End-to-End Guide

This guide documents the full process of provisioning Microsoft Fabric infrastructure and loading [Medicare Part D Prescribers by Provider and Drug](https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug) data (2013–2023) into a Delta table using the Azure CLI and Fabric REST APIs.

> This project uses skills and patterns from [microsoft/skills-for-fabric](https://github.com/microsoft/skills-for-fabric).

## Prerequisites

- **Azure CLI** installed (`az --version`)
- **Logged in** to Azure (`az login`)
- **Python 3** available (for notebook preparation)
- **Azure subscription** with permissions to create Resource Groups and Fabric capacities
- **Local data files**:
  - 11 Medicare Part D zip files in a local directory
  - `UnzipMedicareFiles.ipynb` and `LoadMedicarePartDfiles.ipynb` notebooks

---

## Quick Start — One-Shot Automation

The fastest way to run the full deployment is with the E2E script. It handles all 9 steps automatically:

```bash
# 1. Clone this repo
git clone https://github.com/DataSnowman/skills-for-fabric-load-medicare-data.git
cd skills-for-fabric-load-medicare-data

# 2. Edit the CONFIGURATION section in the script
#    (resource group, capacity name, SKU, local file paths, etc.)
vi deploy-medicare-e2e.sh

# 3. Login to Azure
az login

# 4. Run it
chmod +x deploy-medicare-e2e.sh
./deploy-medicare-e2e.sh
```

The script runs these steps sequentially with polling, error handling, and a summary at the end:

| Step | What it does |
|---|---|
| 0 | Preflight checks (az login, files exist) |
| 1 | Create Azure Resource Group (skips if exists) |
| 2 | Create Fabric Capacity + wait for provisioning |
| 3 | Create Workspace + verify capacity assignment |
| 4 | Create Lakehouse (schemas enabled) |
| 5 | Upload all zip files to OneLake (blob endpoint) |
| 6 | Deploy both notebooks with lakehouse binding |
| 7 | Run UnzipMedicareFiles notebook + poll until complete |
| 8 | Run LoadMedicarePartDfiles notebook + poll until complete |
| 9 | Verify Delta table exists |

> **⚠️ Cost Warning:** This creates a billable Fabric capacity. Pause or delete the capacity when not in use.

---

## Repo Structure

```
├── README.md                         # This file
├── deploy-medicare-e2e.sh            # One-shot E2E automation script
├── .gitignore
├── config/
│   └── variables.md                  # All configurable names, IDs, and paths
├── docs/
│   ├── buildfabricworkspace.md       # Step-by-step infrastructure provisioning
│   ├── LoadMedicareData.md           # Step-by-step data loading workflow
│   └── updateDefinitionNotebookEndpoint.md
└── notebooks/
    ├── UnzipMedicareFiles.ipynb      # Spark notebook to extract zip files
    ├── LoadMedicarePartDfiles.ipynb   # Spark notebook to load CSVs into Delta
    └── TestEnvNotebook.ipynb         # Environment test notebook
```

---

## Configuration

All configuration is managed in [`config/variables.md`](config/variables.md). Key values:

```bash
# Azure
RESOURCE_GROUP="FabricCapacityWestUS3"
LOCATION="westus3"
SKU="F4"

# Fabric
CAPACITY_NAME="westus3f4skillsfghcpcli"
WORKSPACE_NAME="MedicareSkillsF4ghcpcli"
LAKEHOUSE_NAME="MedicareSkillsF4TerminalLHghcpcli"

# Local paths
ZIP_SOURCE_DIR="/Users/darwinschweitzer/sourceData/MedicarePartD/data/DemoZippedFiles"
NOTEBOOK_DIR="/Users/darwinschweitzer/sourceData/MedicarePartD/code/notebook"
```

Auto-populate subscription and admin email after `az login`:

```bash
SUBSCRIPTION_ID=$(az account show --query id --output tsv)
ADMIN_EMAIL=$(az account show --query user.name --output tsv)
```

---

# Step-by-Step Reference

> The sections below document each step in detail for manual execution or troubleshooting. If you used the one-shot script above, you can skip to [Verification](#step-9--verify-delta-table) or [Troubleshooting](#troubleshooting).

---

## Step 1 — Create Resource Group

Creates the Azure Resource Group if it doesn't already exist:

```bash
az group create --name "$RESOURCE_GROUP" --location "$LOCATION"
```

> If you already have a Resource Group, just set `RESOURCE_GROUP` in `config/variables.md` to its name — the E2E script will skip creation.

---

## Step 2 — Create Fabric Capacity

Creates an F4 Fabric capacity in your resource group. Capacity names must be globally unique, lowercase alphanumeric.

```bash
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
  }"
```

Wait for `provisioningState` to become `Succeeded`, then get the Fabric-scoped capacity GUID:

```bash
FABRIC_CAPACITY_ID=$(az rest \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/capacities" \
  --query "value[?displayName=='$CAPACITY_NAME'].id | [0]" --output tsv)
```

---

## Step 3 — Create Workspace

```bash
WS_ID=$(az rest --method post \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces" \
  --body "{\"displayName\": \"$WORKSPACE_NAME\", \"capacityId\": \"$FABRIC_CAPACITY_ID\"}" \
  --query "id" --output tsv)
```

Verify capacity assignment is `Completed` before proceeding:

```bash
az rest --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID" \
  --query "{capacityAssignmentProgress:capacityAssignmentProgress, capacityId:capacityId}"
```

---

## Step 4 — Create Lakehouse

Creates a schema-enabled Lakehouse:

```bash
LH_ID=$(az rest --method post \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items" \
  --body "{\"displayName\": \"$LAKEHOUSE_NAME\", \"type\": \"Lakehouse\", \"creationPayload\": {\"enableSchemas\": true}}" \
  --query "id" --output tsv)
```

---

## Step 5 — Upload Zip Files to OneLake

> **Important:** Use the **blob endpoint** (`onelake.blob.fabric.microsoft.com`), not the DFS endpoint. The DFS endpoint returns `IncorrectEndpointError` for blob-style PUT operations.

> **Important:** Use a `storage.azure.com` token, not a Fabric API token.

```bash
STORAGE_TOKEN=$(az account get-access-token \
  --resource "https://storage.azure.com" \
  --query accessToken --output tsv)

for ZIP_FILE in "$ZIP_SOURCE_DIR"/*.zip; do
  FILENAME=$(basename "$ZIP_FILE")
  curl -s -o /dev/null -w "%{http_code}" -X PUT \
    -H "Authorization: Bearer $STORAGE_TOKEN" \
    -H "x-ms-version: 2023-01-03" \
    -H "x-ms-blob-type: BlockBlob" \
    --data-binary @"$ZIP_FILE" \
    "https://onelake.blob.fabric.microsoft.com/$WS_ID/$LH_ID/Files/medicare/$FILENAME"
done
```

Expected response: `201` for each file. Upload time depends on file size and network speed (~700–800MB per file).

### Data Files Reference

| Year | Zip Filename | Size |
|---|---|---|
| 2013 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2013.zip` | ~701MB |
| 2014 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2014.zip` | ~717MB |
| 2015 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2015.zip` | ~730MB |
| 2016 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2016.zip` | ~745MB |
| 2017 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2017.zip` | ~752MB |
| 2018 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2018.zip` | ~748MB |
| 2019 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2019.zip` | ~752MB |
| 2020 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2020.zip` | ~748MB |
| 2021 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2021.zip` | ~749MB |
| 2022 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2022.zip` | ~781MB |
| 2023 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2023.zip` | ~811MB |

---

## Step 6 — Deploy and Bind Notebooks

### How Lakehouse Binding Works

The lakehouse connection must be embedded in the **notebook's `metadata.dependencies`** section before deploying. This is the same structure Fabric uses when you attach a lakehouse in the UI.

```json
"dependencies": {
  "lakehouse": {
    "default_lakehouse": "<LH_ID>",
    "default_lakehouse_name": "<LAKEHOUSE_NAME>",
    "default_lakehouse_workspace_id": "<WS_ID>",
    "known_lakehouses": [{ "id": "<LH_ID>" }]
  }
}
```

> **Why not PATCH?** The `PATCH /notebooks/{id}` endpoint with a `defaultLakehouse` body does not work — it returns "no valid field to update". Instead, use `POST /notebooks/{id}/updateDefinition` with the lakehouse in the notebook metadata.

### Deployment Process

1. **Update notebook file lists** — Both notebooks ship with a subset of files. Update `zip_files` (unzip notebook) and `full_files` (load notebook) to include all 11 years.

2. **Inject lakehouse dependency** — Add the `metadata.dependencies` block above to each notebook's JSON.

3. **Create the notebook** — POST to `/v1/workspaces/{WS_ID}/items` with the base64-encoded notebook:

   ```bash
   az rest --method post \
     --resource "https://api.fabric.microsoft.com" \
     --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items" \
     --body @/tmp/notebook_body.json
   ```

   > **Note:** Do **not** include `"type"` in the definition body — only in the create body. Including it in both causes an `InvalidInput` error.

4. **Update definition with lakehouse binding** — POST to the `updateDefinition` endpoint:

   ```bash
   az rest --method post \
     --resource "https://api.fabric.microsoft.com" \
     --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/notebooks/$NB_ID/updateDefinition" \
     --body @/tmp/notebook_update_body.json
   ```

### Notebooks

| Notebook | Purpose |
|---|---|
| `UnzipMedicareFiles` | Extracts all 11 zip files from `Files/medicare/` to `Files/medicare/raw/` |
| `LoadMedicarePartDfiles` | Reads CSVs from `Files/medicare/raw/`, adds `filename` and `year` columns, writes to `mcpd.medicarepartd` Delta table |

---

## Step 7 — Run Unzip Notebook

Submit the notebook job and poll for completion:

```bash
# Submit
JOB_ID=$(az rest --method post \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items/$UNZIP_NB_ID/jobs/instances?jobType=RunNotebook" \
  --body '{}' \
  --verbose 2>&1 | grep "'Location'" | grep -oE '[0-9a-f-]{36}' | tail -1)

# Poll
STATUS=$(az rest --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items/$UNZIP_NB_ID/jobs/instances/$JOB_ID" \
  --query "status" --output tsv)
```

> **Note:** The `RunNotebook` endpoint returns `202 Accepted` with the job URL in the `Location` header. The job ID is the last GUID in that URL.

---

## Step 8 — Run Load Notebook

Same pattern as Step 6, using `$LOAD_NB_ID`. This step takes longer as it processes ~8GB of CSV data into Delta format.

```bash
JOB_ID=$(az rest --method post \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items/$LOAD_NB_ID/jobs/instances?jobType=RunNotebook" \
  --body '{}' \
  --verbose 2>&1 | grep "'Location'" | grep -oE '[0-9a-f-]{36}' | tail -1)
```

---

## Step 9 — Verify Delta Table

Query the table in Fabric SQL:

```sql
SELECT [year], count(*) as numberofrows
FROM [<LakehouseName>].[mcpd].[medicarepartd]
GROUP BY [year]
```

### Expected Row Counts

| Year | Rows |
|---|---|
| 2013 | 23,645,873 |
| 2014 | 24,120,618 |
| 2015 | 24,524,894 |
| 2016 | 24,964,300 |
| 2017 | 25,209,130 |
| 2018 | 25,311,600 |
| 2019 | 25,401,870 |
| 2020 | 25,209,729 |
| 2021 | 25,231,862 |
| 2022 | 25,869,521 |
| 2023 | 26,794,878 |
| **Total** | **~275,284,275** |

---

## Delta Table Schema

| Column | Type | Description |
|---|---|---|
| `Prscrbr_NPI` | integer | Prescriber NPI |
| `Prscrbr_Last_Org_Name` | string | Last/Org name |
| `Prscrbr_First_Name` | string | First name |
| `Prscrbr_City` | string | City |
| `Prscrbr_State_Abrvtn` | string | State abbreviation |
| `Prscrbr_State_FIPS` | integer | State FIPS code |
| `Prscrbr_Type` | string | Prescriber type |
| `Prscrbr_Type_Src` | string | Type source |
| `Brnd_Name` | string | Brand name |
| `Gnrc_Name` | string | Generic name |
| `Tot_Clms` | integer | Total claims |
| `Tot_30day_Fills` | integer | Total 30-day fills |
| `Tot_Day_Suply` | integer | Total day supply |
| `Tot_Drug_Cst` | float | Total drug cost |
| `Tot_Benes` | integer | Total beneficiaries |
| `GE65_Sprsn_Flag` | string | 65+ suppression flag |
| `GE65_Tot_Clms` | integer | 65+ total claims |
| `GE65_Tot_30day_Fills` | integer | 65+ total 30-day fills |
| `GE65_Tot_Drug_Cst` | float | 65+ total drug cost |
| `GE65_Tot_Day_Suply` | integer | 65+ total day supply |
| `GE65_Bene_Sprsn_Flag` | string | 65+ beneficiary suppression flag |
| `GE65_Tot_Benes` | integer | 65+ total beneficiaries |
| `filename` | string | Source file path (added by notebook) |
| `year` | string | 4-digit year extracted from filename (added by notebook) |

---

## Troubleshooting

| Issue | Cause | Fix |
|---|---|---|
| `IncorrectEndpointError` on upload | Using DFS endpoint for blob PUT | Switch to `onelake.blob.fabric.microsoft.com` |
| `401` on OneLake upload | Wrong token audience | Use `storage.azure.com` token, not Fabric API token |
| `InvalidInput: Type` on notebook deploy | `"type"` included in definition body | Remove `"type"` from the update definition body |
| `UpdateArtifactRequest should have at least one valid field` | Using PATCH to bind lakehouse | Use `updateDefinition` with lakehouse in notebook metadata |
| `Service is not ready to be updated` on capacity create | Capacity name already exists in a transitional state | Use a different capacity name or wait |
| Notebook runs but no data in table | File lists not updated | Ensure `zip_files` and `full_files` include all 11 years |
| Duplicate rows after re-run | Notebook uses `append` mode | Use `overwrite` mode or truncate table first |

---

## Notes

- The notebooks use **append** mode — re-running will duplicate rows. Switch to `overwrite` or add dedup logic for reruns.
- The `year` column is extracted from the last 4 characters of the filename before `.csv`.
- Fabric F4 capacity is billed while active. Pause or delete the capacity when not in use.
- Two CMS naming patterns exist for older files. See `variables.md` for details.
