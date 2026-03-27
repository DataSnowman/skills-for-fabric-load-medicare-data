# Shared Variables

Copy-paste these into your terminal before running any commands in `buildfabricworkspace.md` or `LoadMedicareData.md`, or source this file directly.

```bash
# Azure Subscription
SUBSCRIPTION_ID=""                          # auto-populated via: az account show --query id --output tsv
ADMIN_EMAIL=""                              # auto-populated via: az account show --query user.name --output tsv

# Resource Group & Location
RESOURCE_GROUP="FabricCapacityWestUS3"      # Created automatically if it doesn't exist
LOCATION="westus3"
SKU="F4"

# Fabric Capacity
CAPACITY_NAME="westus3f4skillsfghcpcli"
FABRIC_CAPACITY_ID=""

# Fabric Workspace
WORKSPACE_NAME="MedicareSkillsF4ghcpcli"
WS_ID=""

# Lakehouse
LAKEHOUSE_NAME="MedicareSkillsF4TerminalLHghcpcli2"
LH_ID=""
DELTA_SCHEMA="mcpd"
DELTA_TABLE="medicarepartd"

# Data Paths (local)
ZIP_SOURCE_DIR="/Users/darwinschweitzer/sourceData/MedicarePartD/data/DemoZippedFiles"
NOTEBOOK_LOCAL_PATH="/Users/darwinschweitzer/sourceData/MedicarePartD/code/notebook"

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

## Medicare Part D — Complete File Reference

All 11 zip files are at `$ZIP_SOURCE_DIR`. All current zips use the **new CMS naming** (no `Dataset_`).

> **Two naming patterns exist depending on download source:**
> - **Old downloads (pre-2024):** `Medicare_Part_D_Prescribers_by_Provider_and_Drug_Dataset_YYYY.zip` → extracts to `..._Dataset_YYYY.csv`
> - **New CMS downloads:** `Medicare_Part_D_Prescribers_by_Provider_and_Drug_YYYY.zip` → extracts to `..._YYYY.csv`
>
> **Always check the zip name before updating the `full_files` list in the load notebook.**
> The CSV name inside the zip matches the zip name (just swap `.zip` → `.csv`).

### All Available Zip Files (local)

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
| 2022 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2022.zip` | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2022.csv` | ❌ | — |
| 2023 | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2023.zip` | `Medicare_Part_D_Prescribers_by_Provider_and_Drug_2023.csv` | ✅ | — |

> **Note on 2013 and 2015:** These were originally loaded from old `Dataset_` zips. The new zips at the path above will produce CSVs without `Dataset_` in the name. If re-loading 2013 or 2015 from the new zips, use the non-`Dataset_` CSV name in the `full_files` list.

### Load Notebook `full_files` — All 11 Years

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
    'Medicare_Part_D_Prescribers_by_Provider_and_Drug_2023.csv'
]
```
