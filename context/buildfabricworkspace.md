# Build Fabric Workspace Template

> **Shared Variables**: Load all IDs, names, and paths before running any commands.
> ```bash
> # Load shared variables
> source config/variables.md   # or copy-paste the values from config/variables.md into your terminal
> ```

Use this template to provision a new Microsoft Fabric Capacity, Workspace, and Lakehouse via Azure CLI.

## Prerequisites

- Azure CLI installed (`az --version`)
- Logged in to Azure (`az login`)
- An Azure subscription with permissions to create Resource Groups and Fabric capacities

---

## Step 1 — Login

```bash
az login
```

---

## Step 2 — Create Resource Group

```bash
RESOURCE_GROUP="<your-resource-group>"       # e.g. FabricCapacityWestUS3
LOCATION="<azure-region>"                    # e.g. westus3

az group create --name "$RESOURCE_GROUP" --location "$LOCATION"
```

> If you already have a Resource Group, skip this step and set `RESOURCE_GROUP` to its name.

---

## Step 3 — Create Fabric Capacity

> Capacity names must be globally unique, lowercase alphanumeric only.

```bash
SUBSCRIPTION_ID=$(az account show --query id --output tsv)
ADMIN_EMAIL=$(az account show --query user.name --output tsv)
RESOURCE_GROUP="<your-resource-group>"       # e.g. FabricCapacityWestUS3
CAPACITY_NAME="<your-capacity-name>"         # e.g. westus3f2viaskills
LOCATION="<azure-region>"                    # e.g. westus3
SKU="<sku>"                                  # F4 minimum (F2 lacks sufficient Spark resources)

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

---

## Step 4 — Get Capacity ID

```bash
SUBSCRIPTION_ID=$(az account show --query id --output tsv)
RESOURCE_GROUP="<your-resource-group>"
CAPACITY_NAME="<your-capacity-name>"

CAPACITY_ID=$(az rest \
  --url "https://management.azure.com/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Fabric/capacities/$CAPACITY_NAME?api-version=2023-11-01" \
  --query "properties.provisioningState" --output tsv && \
  az rest \
  --url "https://management.azure.com/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Fabric/capacities/$CAPACITY_NAME?api-version=2023-11-01" \
  --query "id" --output tsv)

# Get Fabric-scoped capacity ID (GUID used by Fabric REST API)
FABRIC_CAPACITY_ID=$(az rest \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/capacities" \
  --query "value[?displayName=='$CAPACITY_NAME'].id | [0]" --output tsv)

echo "Fabric Capacity ID: $FABRIC_CAPACITY_ID"
```

---

## Step 5 — Create Workspace

```bash
WORKSPACE_NAME="<your-workspace-name>"       # e.g. skills-for-fabric

WS_ID=$(az rest --method post \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces" \
  --body "{\"displayName\": \"$WORKSPACE_NAME\", \"capacityId\": \"$FABRIC_CAPACITY_ID\"}" \
  --query "id" --output tsv)

echo "Workspace ID: $WS_ID"
```

---

## Step 6 — Verify Capacity Assignment

```bash
az rest --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID" \
  --query "{capacityAssignmentProgress:capacityAssignmentProgress, capacityId:capacityId}"
```

Wait until `capacityAssignmentProgress` is `"Completed"` before proceeding.

---

## Step 7 — Create Lakehouse

```bash
LAKEHOUSE_NAME="<your-lakehouse-name>"       # e.g. TerminlLH

LH_ID=$(az rest --method post \
  --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items" \
  --body "{\"displayName\": \"$LAKEHOUSE_NAME\", \"type\": \"Lakehouse\", \"creationPayload\": {\"enableSchemas\": true}}" \
  --query "id" --output tsv)

echo "Lakehouse ID: $LH_ID"
```

---

## Step 8 — Verify Lakehouse

```bash
az rest --resource "https://api.fabric.microsoft.com" \
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/lakehouses/$LH_ID" \
  --query "{id:id, displayName:displayName}"
```

---

## Reference Values (fill in after provisioning)

| Resource         | Name | ID |
|------------------|------|----|
| Resource Group   |      |    |
| Capacity         |      |    |
| Workspace        |      |    |
| Lakehouse        |      |    |

---

## Next Steps

- Load data into the Lakehouse via a Spark notebook or Livy session
- Create Bronze/Silver/Gold schemas for medallion architecture
- See `skills/spark-authoring-cli/resources/data-engineering-patterns.md` for ingestion patterns
