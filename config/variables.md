# Shared Variables

This file is the single source of truth for both the **bash** scripts (`*.sh`) and the
**PowerShell** scripts (`*.ps1`). Edit the values in the `bash` code block below — both script
families read the same `KEY="value"` lines automatically, so you only maintain them once.

> **Two deployment paths (each has a bash and a PowerShell version):**
> - **Full deployment** (`deploy-medicare-e2e.sh` / `deploy-medicare-e2e.ps1`) — Fill in all variables below
> - **Existing workspace** (`deploy-medicare-to-workspace.sh` / `deploy-medicare-to-workspace.ps1`) — Only need `WS_ID`, `LAKEHOUSE_NAME`, and the data/notebook paths

```bash
# Azure Subscription
SUBSCRIPTION_ID=""                          # auto-populated via: az account show --query id --output tsv
ADMIN_EMAIL=""                              # auto-populated via: az account show --query user.name --output tsv

# ── Full deployment only (skip if using existing workspace) ──

# Resource Group & Location
RESOURCE_GROUP="FabricCapacityWestUS3"      # Created automatically if it doesn't exist
LOCATION="westus3"
SKU="F4"

# Fabric Capacity
CAPACITY_NAME="fabricappsf4"
FABRIC_CAPACITY_ID=""

# Fabric Workspace
WORKSPACE_NAME="ZavaPharma"

# ── Both deployment paths ──

# Workspace ID (auto-populated by full deploy; REQUIRED for existing workspace)
WS_ID="4b284d1a-ac92-442a-b575-fa9a025d59a0"                                    # e.g. "dc7ad9cf-c461-4204-8b73-6c1fcb4aff18"

# Lakehouse
LAKEHOUSE_NAME="MedicarePartD"
LH_ID=""
DELTA_SCHEMA="mcpd"
DELTA_TABLE="medicarepartd"

# Data Paths (local)
ZIP_SOURCE_DIR="data/DemoZippedFiles"
NOTEBOOK_LOCAL_PATH="notebooks"

# OneLake Paths
ONELAKE_ZIP_PATH="Files/medicare"
ONELAKE_RAW_PATH="Files/medicare/raw"

# Notebooks (Fabric)
NOTEBOOK_NAME="LoadMedicarePartDfiles"
LOAD_NB_ID=""
UNZIP_NOTEBOOK_NAME="UnzipMedicareFiles"
UNZIP_NB_ID=""
TEST_NB_ID=""
```

## Auto-populate Subscription and Admin Email

Run this once after `az login` to set the auto-populated values:

```bash
SUBSCRIPTION_ID=$(az account show --query id --output tsv)
ADMIN_EMAIL=$(az account show --query user.name --output tsv)
```

## Medicare Part D — File Reference

The full CMS dataset spans **2013–2024**. The scripts auto-detect whichever
`Medicare_Part_D_..._YYYY.zip` files are present in `$ZIP_SOURCE_DIR` — drop in 1 or all of
them. All current zips use the **new CMS naming** (no `Dataset_`).

> **Demo set shipped in this repo:** `data/DemoZippedFiles/` currently contains **2022, 2023,
> and 2024** (3 files). Add more years to that folder to load additional years — no code or
> config changes are needed, the scripts pick them up automatically.

> **Two naming patterns exist depending on download source:**
> - **Old downloads (pre-2024):** `Medicare_Part_D_Prescribers_by_Provider_and_Drug_Dataset_YYYY.zip` → extracts to `..._Dataset_YYYY.csv`
> - **New CMS downloads:** `Medicare_Part_D_Prescribers_by_Provider_and_Drug_YYYY.zip` → extracts to `..._YYYY.csv`
>
> **Always check the zip name before updating the `full_files` list in the load notebook.**
> The CSV name inside the zip matches the zip name (just swap `.zip` → `.csv`).

### All Available Zip Files

| Year | Zip Filename | CSV Filename (inside zip) | Loaded | Rows |
|---|---|---|---|---|
| 2013 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2013.zip` | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2013.csv` | ✅ (old zip) | 23,645,873 |
| 2014 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2014.zip` | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2014.csv` | ✅ | 24,120,618 |
| 2015 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2015.zip` | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2015.csv` | ✅ (old zip) | 24,524,894 |
| 2016 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2016.zip` | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2016.csv` | ❌ | — |
| 2017 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2017.zip` | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2017.csv` | ❌ | — |
| 2018 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2018.zip` | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2018.csv` | ❌ | — |
| 2019 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2019.zip` | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2019.csv` | ❌ | — |
| 2020 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2020.zip` | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2020.csv` | ❌ | — |
| 2021 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2021.zip` | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2021.csv` | ❌ | — |
| 2022 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2022.zip` | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2022.csv` | ✅ (in demo set) | — |
| 2023 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2023.zip` | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2023.csv` | ✅ (in demo set) | — |
| 2024 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2024.zip` | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2024.csv` | ✅ (in demo set) | — |

> **Note on 2013 and 2015:** These were originally loaded from old `Dataset_` zips. The new zips at the path above will produce CSVs without `Dataset_` in the name. If re-loading 2013 or 2015 from the new zips, use the non-`Dataset_` CSV name in the `full_files` list.

### Load Notebook `full_files` — Full CMS Range (2013–2024)

> The deploy scripts **auto-generate** this list from the zips present in `$ZIP_SOURCE_DIR`,
> so you normally don't edit it by hand. Shown here for reference. With the shipped demo set,
> the generated list contains only 2022, 2023, and 2024.

```python
full_files = [
    'Medicare_Part_D_Prescribers_by_Provider_and_Drug_2013.csv',
    'Medicare_Part_D_Prescribers_by_Provider_and_Drug_2014.csv',
    'Medicare_Part_D_Prescribers_by_Provider_and_Drug_2015.csv',
    'Medicare_Part_D_Prescribers_by_Provider_and_Drug_2016.csv',
    'Medicare_Part_D_Prescribers_by_Provider_and_Drug_2017.csv',
    'Medicare_Part_D_Prescribers_by_Provider_and_Drug_2018.csv',
    'Medicare_Part_D_Prescribers_by_Provider_and_Drug_2019.csv',
    'Medicare_Part_D_Prescribers_by_Provider_and_Drug_2020.csv',
    'Medicare_Part_D_Prescribers_by_Provider_and_Drug_2021.csv',
    'Medicare_Part_D_Prescribers_by_Provider_and_Drug_2022.csv',
    'Medicare_Part_D_Prescribers_by_Provider_and_Drug_2023.csv',
    'Medicare_Part_D_Prescribers_by_Provider_and_Drug_2024.csv'
]
```
