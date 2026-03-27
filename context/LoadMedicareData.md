# Load Medicare Part D Data into TerminlLH

> **Shared Variables**: Load all IDs, names, and paths before running any commands.
> ```bash
> # Load shared variables
> source config/variables.md   # or copy-paste the values from config/variables.md into your terminal
> ```

End-to-end workflow to upload a zipped Medicare Part D dataset from your desktop to the `TerminlLH` Lakehouse, unzip it via a Fabric Pipeline, and load the CSVs into a Delta table using a Spark Notebook.

---

## Reference IDs

| Resource | Name | ID |
|---|---|---|
| Workspace | `skills-for-fabric` | `3c8b0517-a8ef-4c81-ad69-91d7860e36df` |
| Lakehouse | `TerminlLH` | `68ad9840-d518-4b4a-9efd-7fc4585f162b` |

---

## Variables

```bash
WS_ID="3c8b0517-a8ef-4c81-ad69-91d7860e36df"
LH_ID="68ad9840-d518-4b4a-9efd-7fc4585f162b"
ZIP_FILE="<your-zip-filename>.zip"            # e.g. MedicarePartD.zip — file on ~/Desktop/
NOTEBOOK_LOCAL_PATH="/Users/darwinschweitzer/sourceData/MedicarePartD/code/notebook/LoadMedicarePartDfiles.ipynb"
NOTEBOOK_NAME="LoadMedicarePartDfiles"
```

---

## Step 1 — Upload Zip File from Desktop to TerminlLH Files

Uses the OneLake DFS API. Requires a `storage.azure.com` token (different from the Fabric API token).

```bash
ZIP_FILE="<your-zip-filename>.zip"
WS_ID="3c8b0517-a8ef-4c81-ad69-91d7860e36df"
LH_ID="68ad9840-d518-4b4a-9efd-7fc4585f162b"

# Get OneLake storage token (must use storage.azure.com audience)
STORAGE_TOKEN=$(az account get-access-token \
  --resource "https://storage.azure.com" \
  --query accessToken --output tsv)

# Upload zip to Files/medicare/ in TerminlLH
curl -s -X PUT \
  -H "Authorization: Bearer $STORAGE_TOKEN" \
  -H "x-ms-version: 2023-01-03" \
  -H "x-ms-blob-type: BlockBlob" \
  --data-binary @"$HOME/Desktop/$ZIP_FILE" \
  "https://onelake.dfs.fabric.microsoft.com/$WS_ID/$LH_ID/Files/medicare/$ZIP_FILE"

echo "Upload complete: Files/medicare/$ZIP_FILE"
```

> **Note:** OneLake DFS requires the `storage.azure.com` token — using the Fabric API token here will return 401.

---

## Step 2 — Create Fabric Pipeline to Unzip the File

Creates a pipeline with a Copy Activity that reads the zip file and decompresses it to `Files/medicare/raw/`.

```bash
ZIP_FILE="<your-zip-filename>.zip"
WS_ID="3c8b0517-a8ef-4c81-ad69-91d7860e36df"
LH_ID="68ad9840-d518-4b4a-9efd-7fc4585f162b"

# Encode pipeline definition
PIPELINE_DEF=$(cat << EOF
{
  "name": "UnzipMedicareFiles",
  "properties": {
    "activities": [
      {
        "name": "UnzipToRaw",
        "type": "Copy",
        "typeProperties": {
          "source": {
            "type": "BinarySource",
            "storeSettings": {
              "type": "LakehouseReadSettings",
              "recursive": false
            },
            "formatSettings": {
              "type": "BinaryReadSettings",
              "compressionProperties": {
                "type": "ZipDeflateReadSettings",
                "preserveZipFileNameAsFolder": false
              }
            }
          },
          "sink": {
            "type": "BinarySink",
            "storeSettings": {
              "type": "LakehouseWriteSettings"
            }
          },
          "inputs": [
            {
              "referenceName": "SourceZip",
              "type": "DatasetReference"
            }
          ],
          "outputs": [
            {
              "referenceName": "SinkRaw",
              "type": "DatasetReference"
            }
          ]
        }
      }
    ]
  }
}
EOF
)

PIPELINE_DEF_B64=$(echo "$PIPELINE_DEF" | base64)

cat > /tmp/pipeline_body.json << EOF
{
  "displayName": "UnzipMedicareFiles",
  "type": "DataPipeline",
  "definition": {
    "parts": [
      {
        "path": "pipeline-content.json",
        "payload": "$PIPELINE_DEF_B64",
        "payloadType": "InlineBase64"
      }
    ]
  }
}
EOF

PIPELINE_ID=$(az rest --method post \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items" \
  --body @/tmp/pipeline_body.json \
  --query "id" --output tsv)

echo "Pipeline ID: $PIPELINE_ID"
```

> **Alternative — Unzip via Python in a Notebook cell:**
> If the pipeline approach is complex, you can add an unzip step directly in a notebook before loading:
> ```python
> import zipfile, os
> zip_path = "/lakehouse/default/Files/medicare/<your-zip-filename>.zip"
> extract_to = "/lakehouse/default/Files/medicare/raw/"
> os.makedirs(extract_to, exist_ok=True)
> with zipfile.ZipFile(zip_path, 'r') as z:
>     z.extractall(extract_to)
> print("Unzip complete")
> ```

---

## Step 3 — Run the Pipeline (Unzip Job)

```bash
WS_ID="3c8b0517-a8ef-4c81-ad69-91d7860e36df"
PIPELINE_ID="<pipeline-id-from-step-2>"

# Submit pipeline run
JOB_RESPONSE=$(az rest --method post \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items/$PIPELINE_ID/jobs/instances?jobType=Pipeline" \
  --body '{}')

echo "$JOB_RESPONSE"
JOB_ID=$(echo "$JOB_RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('id',''))" 2>/dev/null || echo "check response above")

echo "Pipeline Job ID: $JOB_ID"
```

**Poll until Completed:**
```bash
WS_ID="3c8b0517-a8ef-4c81-ad69-91d7860e36df"
PIPELINE_ID="<pipeline-id>"
JOB_ID="<job-id>"

for i in {1..20}; do
  STATUS=$(az rest --resource "https://api.fabric.microsoft.com" \
    --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items/$PIPELINE_ID/jobs/instances/$JOB_ID" \
    --query "status" --output tsv)
  echo "[$i] Pipeline status: $STATUS"
  if [[ "$STATUS" == "Succeeded" || "$STATUS" == "Failed" || "$STATUS" == "Cancelled" ]]; then break; fi
  sleep 15
done
```

---

## Step 4 — Deploy the Spark Notebook to TerminlLH

Reads the local `.ipynb` file, encodes it, and deploys it to the `skills-for-fabric` workspace bound to `TerminlLH`.

```bash
WS_ID="3c8b0517-a8ef-4c81-ad69-91d7860e36df"
LH_ID="68ad9840-d518-4b4a-9efd-7fc4585f162b"
NOTEBOOK_LOCAL_PATH="/Users/darwinschweitzer/sourceData/MedicarePartD/code/notebook/LoadMedicarePartDfiles.ipynb"
NOTEBOOK_NAME="LoadMedicarePartDfiles"

# Encode notebook as base64
NB_B64=$(base64 -i "$NOTEBOOK_LOCAL_PATH")

# Build request body with default lakehouse binding
cat > /tmp/notebook_body.json << EOF
{
  "displayName": "$NOTEBOOK_NAME",
  "type": "Notebook",
  "definition": {
    "format": "ipynb",
    "parts": [
      {
        "path": "artifact.content.ipynb",
        "payload": "$NB_B64",
        "payloadType": "InlineBase64"
      },
      {
        "path": ".platform",
        "payload": "$(echo "{\"metadata\":{\"type\":\"SparkNotebook\",\"displayName\":\"$NOTEBOOK_NAME\"},\"config\":{\"version\":\"2.0\",\"logicalId\":\"$(uuidgen | tr '[:upper:]' '[:lower:]')\"}}" | base64)",
        "payloadType": "InlineBase64"
      }
    ]
  }
}
EOF

NOTEBOOK_ID=$(az rest --method post \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items" \
  --body @/tmp/notebook_body.json \
  --query "id" --output tsv)

echo "Notebook ID: $NOTEBOOK_ID"
```

**Bind notebook to TerminlLH (default lakehouse):**
```bash
WS_ID="3c8b0517-a8ef-4c81-ad69-91d7860e36df"
LH_ID="68ad9840-d518-4b4a-9efd-7fc4585f162b"
NOTEBOOK_ID="<notebook-id-from-above>"

cat > /tmp/bind_body.json << EOF
{
  "defaultLakehouse": {
    "id": "$LH_ID",
    "workspaceId": "$WS_ID"
  }
}
EOF

az rest --method patch \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/notebooks/$NOTEBOOK_ID" \
  --body @/tmp/bind_body.json
```

---

## Step 5 — Run the Notebook to Load CSVs into Delta Table

The notebook reads from `Files/medicare/raw/*.csv` and writes to `Tables/medicarepartd` (Delta format).

```bash
WS_ID="3c8b0517-a8ef-4c81-ad69-91d7860e36df"
NOTEBOOK_ID="<notebook-id>"

# Check for recent jobs first (prevent duplicates)
RECENT=$(az rest --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items/$NOTEBOOK_ID/jobs/instances?jobType=RunNotebook" \
  --query "value[0].{status:status, id:id}" --output json)
echo "Most recent job: $RECENT"

# Submit notebook run
JOB_URL=$(az rest --method post \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items/$NOTEBOOK_ID/jobs/instances?jobType=RunNotebook" \
  --body '{}' \
  --headers '{"Content-Length": "2"}' \
  -o none -v 2>&1 | grep "Location:" | awk '{print $3}' | tr -d '\r')

echo "Job location: $JOB_URL"
```

**Poll notebook job until complete:**
```bash
WS_ID="3c8b0517-a8ef-4c81-ad69-91d7860e36df"
NOTEBOOK_ID="<notebook-id>"
JOB_ID="<job-id>"

for i in {1..60}; do
  STATUS=$(az rest --resource "https://api.fabric.microsoft.com" \
    --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items/$NOTEBOOK_ID/jobs/instances/$JOB_ID" \
    --query "status" --output tsv)
  echo "[$i] Notebook status: $STATUS"
  if [[ "$STATUS" == "Succeeded" || "$STATUS" == "Failed" || "$STATUS" == "Cancelled" ]]; then break; fi
  sleep 30
done
```

---

## Step 6 — Verify Delta Table

```bash
WS_ID="3c8b0517-a8ef-4c81-ad69-91d7860e36df"
LH_ID="68ad9840-d518-4b4a-9efd-7fc4585f162b"

az rest --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/lakehouses/$LH_ID/tables" \
  --query "value[].{name:name, format:format, location:location}" \
  --output table
```

Expected output: a table named `medicarepartd` in Delta format.

---

## Notebook Schema Reference

The notebook loads Medicare Part D Prescribers by Provider and Drug files (2013–2021).

### CSV Source Schema (`Files/medicare/raw/`)
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
| `GE65_*` | various | 65+ age group metrics |

### Delta Table Schema (`Tables/medicarepartd`)
Same as above plus:
| Column | Type | Description |
|---|---|---|
| `filename` | string | Source file path |
| `year` | string | Extracted 4-digit year from filename |

### Expected CSV Files
```
Medicare_Part_D_Prescribers_by_Provider_and_Drug_Dataset_2013.csv
Medicare_Part_D_Prescribers_by_Provider_and_Drug_Dataset_2014.csv
Medicare_Part_D_Prescribers_by_Provider_and_Drug_Dataset_2015.csv
Medicare_Part_D_Prescribers_by_Provider_and_Drug_Dataset_2016.csv
Medicare_Part_D_Prescribers_by_Provider_and_Drug_Dataset_2017.csv
Medicare_Part_D_Prescribers_by_Provider_and_Drug_2018.csv
Medicare_Part_D_Prescribers_by_Provider_and_Drug_2019.csv
Medicare_Part_D_Prescribers_by_Provider_and_Drug_2020.csv
Medicare_Part_D_Prescribers_by_Provider_and_Drug_2021.csv
```

---

## Notes

- The notebook appends to `Tables/medicarepartd` — re-running will duplicate rows. Add a dedup step or use `overwrite` mode if re-running.
- The `year` column is extracted from the last 4 characters of the filename before the `.csv` extension.
- Ensure the zip file extracts directly to `Files/medicare/raw/` (not a subdirectory) so the notebook paths match.
- OneLake file uploads require the `storage.azure.com` token — **not** the Fabric API token.
