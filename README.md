# Load Medicare Part D Data — End-to-End Guide

This guide documents the full process of provisioning Microsoft Fabric infrastructure and loading [Medicare Part D Prescribers by Provider and Drug](https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug) data (2013–2023, ~275 M rows) into a Delta table using the Azure CLI and Fabric REST APIs.

> This project was built using [Claude Code](https://docs.anthropic.com/en/docs/claude-code) and [GitHub Copilot CLI](https://docs.github.com/en/copilot) with skills and context from [microsoft/skills-for-fabric](https://github.com/microsoft/skills-for-fabric).

## Background — From GUI Clicks to Terminal Velocity

Three years ago this same Medicare dataset was loaded into a Fabric Lakehouse through the portal UI — manually creating workspaces, uploading files, and clicking through notebooks one at a time. That original walkthrough is still available here:

🔗 **[DataSnowman/fabriclakehouse](https://github.com/DataSnowman/fabriclakehouse/tree/main)** — the 2023 GUI-based approach

This repo is the **Terminal Velocity** edition. Instead of portal clicks, every step — from creating the Azure Resource Group and Fabric Capacity to deploying notebooks and loading ~275 million rows — runs as a single shell script driven by AI coding agents in a terminal. What previously took an afternoon of point-and-click now executes end-to-end in minutes with one command.

The key enabler is **AI in the terminal**: tools like [GitHub Copilot CLI](https://docs.github.com/en/copilot) and [Claude Code](https://docs.anthropic.com/en/docs/claude-code) can read the context files in this repo, orchestrate Azure CLI and Fabric REST API calls, troubleshoot errors in real time, and iterate until the pipeline succeeds — all without leaving the command line. Pair that with the reusable skills from [microsoft/skills-for-fabric](https://github.com/microsoft/skills-for-fabric) and you get a fully automated, repeatable deployment that any developer can clone and run.

## Prerequisites

- **GitHub Copilot CLI** [Installing GitHub Copilot CLI](https://docs.github.com/en/copilot/how-tos/copilot-cli/set-up-copilot-cli/install-copilot-cli) or **Claude Code** [Getting Started Quickstart](https://code.claude.com/docs/en/quickstart)
- **Azure CLI** installed (`az --version`)
- **Logged in** to Azure (`az login`)
- **A supported shell** — one of:
  - **Bash** (for the `*.sh` scripts) — macOS Terminal, Linux, or Windows WSL/Git Bash. Requires **Python 3.9+** (`python3 --version`).
  - **PowerShell 7+** (for the `*.ps1` scripts) — native on **Windows** (also runs on macOS/Linux). Uses `curl.exe` (bundled with Windows 10+) and needs **no Python**.
- **Microsoft Fabric** — one of the following:
  - **Full deployment:** An Azure subscription with permissions to create Resource Groups and [Fabric capacities](https://learn.microsoft.com/en-us/fabric/enterprise/licenses) (F4 or higher — F2 does not have sufficient Spark resources for these notebooks)
  - **Existing workspace:** Contributor (or higher) access to an existing Fabric workspace on an F4+ capacity
- **Medicare Part D data files** — 1 to 11 zip files (the scripts auto-detect whichever years are present):
  - [Download data](https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug/data) | [Data dictionary](https://data.cms.gov/resources/medicare-part-d-prescribers-by-provider-and-drug-data-dictionary)
  - The zips are **not** in the repo (they're gitignored). See [Getting the Data](#getting-the-data) for how to fetch them — whether you're running locally or in a Codespace.
  - `UnzipMedicareFiles.ipynb` and `LoadMedicarePartDfiles.ipynb` notebooks (included in `notebooks/`)

> **Windows users:** You have two options. Run the native **PowerShell** scripts
> (`deploy-medicare-e2e.ps1` / `deploy-medicare-to-workspace.ps1`) directly in PowerShell 7+
> — no WSL required — **or** run the **bash** scripts (`*.sh`) under [WSL](https://learn.microsoft.com/en-us/windows/wsl/install)
> or Git Bash. Both paths read the same `config/variables.env`.

## Cross-Platform Support

This repo is fully multi-OS. Every deployment path ships in **both** bash and PowerShell:

| Path | Bash (macOS / Linux / WSL / Git Bash) | PowerShell 7+ (Windows / macOS / Linux) |
|---|---|---|
| Full deployment | `deploy-medicare-e2e.sh` | `deploy-medicare-e2e.ps1` |
| Existing workspace | `deploy-medicare-to-workspace.sh` | `deploy-medicare-to-workspace.ps1` |

Both families read configuration from the same [`config/variables.env`](config/variables.env),
auto-detect whichever zip years are present in `data/DemoZippedFiles/`, and produce identical
results. The PowerShell scripts are self-contained (no Python required) and share helpers from
`medicare-common.ps1`.

---

## Things to Consider

If you only have time to load one or two of the zipfiles and not all 11 just download one or two and put them in the /data/DemoZippedFiles directory (or any other local directory) and just tell GitHub Copilot CLI or Claude Code that you only want to load way 2023 and 2022 and where they are.

Also mention the md files that are in the context folder because buildfabricworkspace.md and LoadMedicareData.md will let the CLI or Claude code know what it needs to do.

## Configuration

All configuration lives in [`config/variables.env`](config/variables.env) — the single file you edit, shared by both the PowerShell and bash scripts.

**Most people — deploy into an existing workspace.** You only need `WS_ID`:

| Variable | Required | Description |
|---|---|---|
| `WS_ID` | ✅ | Existing Fabric workspace GUID (from the workspace URL) |
| `LAKEHOUSE_NAME` | optional | Lakehouse to create (defaults to `MedicarePartD`) |

```bash
WS_ID=""                          # REQUIRED — your existing Fabric workspace GUID
LAKEHOUSE_NAME="MedicarePartD"
```

**Admins only — full deployment** (`deploy-medicare-e2e.*`) also creates the Resource Group, Fabric Capacity, and Workspace. Those extra variables (`RESOURCE_GROUP`, `LOCATION`, `SKU`, `CAPACITY_NAME`, `WORKSPACE_NAME`) live in a separate section of `config/variables.env` — ignore them if you're using an existing workspace.

---

## Quick Start

> 🚀 **Zero-setup option:** This repo includes a [Dev Container](.devcontainer/README.md).
> Open it in **GitHub Codespaces** (**Code → Codespaces → Create codespace on main**) to get
> Azure CLI, PowerShell, GitHub CLI, Copilot CLI, and Python/Jupyter pre-installed — no local
> setup required. See the [Dev Container Quick Start](.devcontainer/README.md) for details.

### Step 1 — Clone the Repo

```bash
git clone https://github.com/DataSnowman/skills-for-fabric-load-medicare-data.git
```

### Step 2 — Change into the Repo Directory

```bash
cd skills-for-fabric-load-medicare-data
```

### Step 3 — (Optional) Open in VS Code

If you prefer to edit the markdown files in VS Code:

```bash
code .
```

### Step 4 — Edit Configuration

Open `config/variables.env` and set **`WS_ID`** to your existing Fabric workspace GUID:

| Variable | What to set |
|---|---|
| `WS_ID` | Your existing Fabric workspace GUID (**required**) |
| `LAKEHOUSE_NAME` | The Lakehouse to create (optional — defaults to `MedicarePartD`) |

> To find your Workspace ID: open the workspace in the Fabric portal — the ID is in the URL: `https://app.fabric.microsoft.com/groups/<WORKSPACE_ID>/...`
>
> **Doing a full deployment instead?** (creates the Resource Group, Capacity, and Workspace for you — admins only.) Set `RESOURCE_GROUP`, `LOCATION`, `SKU`, `CAPACITY_NAME`, and `WORKSPACE_NAME` in the full-deployment section of `config/variables.env`.

You also need the Medicare Part D zip file(s) in `data/DemoZippedFiles/`. How you get them there depends on where you're running — see [Getting the Data](#getting-the-data) just below.

### Getting the Data

The zip files are large and **gitignored**, so they are never committed to the repo. That means a
fresh clone (local) or a fresh Codespace starts with an empty `data/DemoZippedFiles/` folder — you
fetch the zips yourself.

> **⚠️ Filename matters.** The scripts auto-detect years by looking for files named
> **`Medicare_Part_D_Prescribers_by_Provider_and_Drug_YYYY.zip`** (e.g. `..._2023.zip`). The CMS
> per-year **Download** button already produces this name. If a file lands with a different name,
> rename it to match or it will be skipped.

**How to grab a year's download link (both audiences):** open the
[CMS data page](https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug/data),
pick a year, then **right-click the Download button → Copy link address**.

#### Local VS Code (on your machine)

Use the browser **Download** button for each year you want, then move the `.zip` files into the
repo's `data/DemoZippedFiles/` folder. (You can also `curl -L -o "data/DemoZippedFiles/Medicare_Part_D_Prescribers_by_Provider_and_Drug_2023.zip" "<copied-URL>"` if you prefer the terminal.)
Because the folder is a normal local path, this is the same directory the scripts read.

#### GitHub Codespaces / Dev Container

There's no local browser inside the container, so download **directly into the Codespace** with the
copied CMS link — this keeps the transfer cloud-to-cloud (CMS → Codespace) instead of routing
through your home connection:

```bash
curl -L -o "data/DemoZippedFiles/Medicare_Part_D_Prescribers_by_Provider_and_Drug_2023.zip" \
  "<paste-the-CMS-download-URL-here>"
```

Repeat for each year, then verify with `ls -lh data/DemoZippedFiles/*.zip`. Full options (including
uploading zips you already have, or letting an AI agent do the download) are in the
[Dev Container Quick Start](.devcontainer/README.md#-getting-the-medicare-data-into-the-codespace).

> **Tip:** For a quick demo, just grab one or two years (e.g. 2023 and 2022). A full 11-file set is ~8 GB.

### Step 5 — Login to Azure

```bash
az login
```

### Step 6 — Choose How to Run It

Pick one of the options below. **Bash** and **PowerShell** versions are equivalent — use whichever fits your OS.

#### Option A: Full Deployment (new infrastructure)

Creates Resource Group → Capacity → Workspace → Lakehouse → loads data.

**Bash (macOS / Linux / WSL / Git Bash):**
```bash
chmod +x deploy-medicare-e2e.sh
./deploy-medicare-e2e.sh
```

**PowerShell 7+ (Windows / macOS / Linux):**
```powershell
pwsh ./deploy-medicare-e2e.ps1
```

#### Option B: Existing Workspace (Contributor access)

Uses your existing workspace. Only needs `WS_ID` set in `config/variables.env`.

**Bash (macOS / Linux / WSL / Git Bash):**
```bash
chmod +x deploy-medicare-to-workspace.sh
./deploy-medicare-to-workspace.sh
```

**PowerShell 7+ (Windows / macOS / Linux):**
```powershell
pwsh ./deploy-medicare-to-workspace.ps1
```

#### Option C: AI Coding Agent (GitHub Copilot CLI or Claude Code)

Let an AI agent read the context files and execute each step interactively in the terminal.

**GitHub Copilot CLI:**
```bash
copilot
```

OR

**Claude Code:**
```bash
claude
```

Once the agent is running, give it a prompt that references the context and config files. For example:

**Full deployment (new infrastructure + data load):**
```
Read config/variables.env for the configuration values, then follow
context/buildfabricworkspace.md to create the Fabric infrastructure
and context/LoadMedicareData.md to upload and load the Medicare data.
```

**Existing workspace (data load only):**
```
Read config/variables.env for the workspace ID and paths, then follow
context/LoadMedicareData.md to upload the zip files, deploy the
notebooks, and load the data into the Lakehouse.
```

**Just a specific step (e.g., notebook binding troubleshooting):**
```
Read context/updateDefinitionNotebookEndpoint.md — I need help
attaching a Lakehouse to a notebook via the updateDefinition API.
```

**Just a specific zipfile (e.g., Just load 2022 zipfile):**
```
Read context/LoadMedicareData.md — Just load the 2022 zipfile
and make the appropriate change to the LoadMedicarePartDfiles.ipynb.
```

> **Tip:** You don't need to copy-paste commands yourself — the agent reads the context files, fills in your variables, and executes each step in the terminal. If something fails, it will troubleshoot and retry automatically.

### What each script does (Options A & B)

Bash and PowerShell versions perform the same steps.

| Step | Full (`deploy-medicare-e2e.sh` / `.ps1`) | Existing (`deploy-medicare-to-workspace.sh` / `.ps1`) |
|---|---|---|
| Preflight checks | ✅ | ✅ |
| Create Resource Group | ✅ | — |
| Create Fabric Capacity | ✅ | — |
| Create Workspace | ✅ | — |
| Create Lakehouse | ✅ | ✅ (Step 1) |
| Upload zip files | ✅ | ✅ (Step 2) |
| Deploy & bind notebooks | ✅ | ✅ (Step 3) |
| Run Unzip notebook | ✅ | ✅ (Step 4) |
| Run Load notebook | ✅ | ✅ (Step 5) |
| Verify Delta table | ✅ | ✅ (Step 6) |

> **⚠️ Cost Warning:** This creates a billable Fabric capacity. Pause or delete the capacity when not in use.

### Troubleshooting with skills-for-fabric

The [microsoft/skills-for-fabric](https://github.com/microsoft/skills-for-fabric) repo contains the Fabric-specific skills and MCP server setup that were used to create the context files in this repo. If you need to troubleshoot or extend the deployment:

1. Clone `skills-for-fabric` alongside this repo
2. Use its skills (e.g., `spark-authoring-cli`, `fabric-workspace-cli`) for deeper Fabric API guidance
3. See its `mcp-setup/` folder for MCP server configuration

---

## Repo Structure

```
├── README.md                              # This file
├── deploy-medicare-e2e.sh                 # Full deployment — bash (creates all infrastructure)
├── deploy-medicare-e2e.ps1                # Full deployment — PowerShell (Windows-native)
├── deploy-medicare-to-workspace.sh        # Existing workspace — bash (Contributor access)
├── deploy-medicare-to-workspace.ps1       # Existing workspace — PowerShell (Windows-native)
├── medicare-common.ps1                    # Shared helpers for the PowerShell scripts
├── pyproject.toml                         # Python project config (for uv; bash scripts only)
├── .gitignore
├── config/
│   └── variables.env                       # The one file you edit: WS_ID (+ optional full-deploy settings)
├── context/                               # AI agent context files (Claude Code / Copilot CLI)
│   ├── buildfabricworkspace.md            # Step-by-step infrastructure provisioning
│   ├── LoadMedicareData.md                # Step-by-step data loading workflow
│   └── updateDefinitionNotebookEndpoint.md
└── notebooks/
    ├── UnzipMedicareFiles.ipynb           # Spark notebook to extract zip files
    ├── LoadMedicarePartDfiles.ipynb        # Spark notebook to load CSVs into Delta
    └── TestEnvNotebook.ipynb              # Environment test notebook
```


## Checking if things worked

When the script completes successfully you might get something that 
looks like this in the terminal.

All steps succeeded:

| Resource  | Name                                    | ID         |
 |-----------|----------------------------------------|------------|
 | Capacity  | westus3f4skillsfghcpcliubunto           | 8110829b-  |
 | Workspace | MedicareSkillsF4ghcpcliubuntu           | b8eee3e8-  |
 | Lakehouse | MedicareSkillsF4TerminalLHghcpcliubuntu | f56e57b3-  |
 | Table     | mcpd.medicarepartd                      | ✅ verified |

---

Here are some images of the Fabric screen shots


Fabric Capacity


![capacity](https://raw.githubusercontent.com/datasnowman/SKILLS-FOR-FABRIC-LOAD-MEDICARE-DATA/main/images/capacity.png)


Fabric Workspace


![workspace](https://raw.githubusercontent.com/datasnowman/SKILLS-FOR-FABRIC-LOAD-MEDICARE-DATA/main/images/workspace.png)


Fabric Lakehouse Files and Tables


![lakehouse](https://raw.githubusercontent.com/datasnowman/SKILLS-FOR-FABRIC-LOAD-MEDICARE-DATA/main/images/lakehouse.png)


Fabric Notebooks


![nbunzip](https://raw.githubusercontent.com/datasnowman/SKILLS-FOR-FABRIC-LOAD-MEDICARE-DATA/main/images/nbunzip.png)


![nbload](https://raw.githubusercontent.com/datasnowman/SKILLS-FOR-FABRIC-LOAD-MEDICARE-DATA/main/images/nbload.png)


Fabric SQL Analytics Endpoint


![sqlep](https://raw.githubusercontent.com/datasnowman/SKILLS-FOR-FABRIC-LOAD-MEDICARE-DATA/main/images/sqlep.png)

To verify the row count in Fabric SQL:
```
 SELECT [year], count(*) as numberofrows
 FROM [<NameOfLakehouse>].[mcpd].[medicarepartd]
 GROUP BY [year]
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

> If you already have a Resource Group, just set `RESOURCE_GROUP` in `config/variables.env` to its name — the E2E script will skip creation.

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
- Two CMS naming patterns exist for older files. See [`context/LoadMedicareData.md`](context/LoadMedicareData.md) for details.
