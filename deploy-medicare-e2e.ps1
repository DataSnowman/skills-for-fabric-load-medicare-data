<#
.SYNOPSIS
    End-to-end provision + load of Medicare Part D data into Microsoft Fabric (native PowerShell).

.DESCRIPTION
    Windows-native equivalent of deploy-medicare-e2e.sh. Provisions an Azure Resource Group,
    Fabric Capacity, Workspace, and Lakehouse, then uploads the local zip files to OneLake,
    deploys + binds both notebooks, runs the unzip and load notebooks, and verifies the Delta
    table. Idempotent: existing resources are detected and reused.

    Values are read from config/variables.md (single source of truth, shared with the bash
    scripts). Override any of them with the parameters below.

.PREREQUISITES
    PowerShell 7+ (pwsh), Azure CLI (az) logged in via 'az login', curl.exe (Windows 10+),
    an Azure subscription with permission to create Resource Groups and Fabric capacities
    (F4 or higher - F2 lacks sufficient Spark resources).

.EXAMPLE
    pwsh ./deploy-medicare-e2e.ps1
    pwsh ./deploy-medicare-e2e.ps1 -WorkspaceName "MedicareSkillsF4win" -Sku F4
#>
[CmdletBinding()]
param(
    [string]$ResourceGroup,
    [string]$Location,
    [string]$Sku,
    [string]$CapacityName,
    [string]$WorkspaceName,
    [string]$LakehouseName
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $ScriptDir 'medicare-common.ps1')

# ─── CONFIGURATION (loaded from config/variables.md, override via parameters) ──

$VarsFile     = Join-Path $ScriptDir 'config/variables.md'
$vars         = Get-MedicareVariables -VarsFile $VarsFile
$ZipSourceDir = Join-Path $ScriptDir 'data/DemoZippedFiles'
$NotebookDir  = Join-Path $ScriptDir 'notebooks'
$TmpDir       = Join-Path $ScriptDir '.tmp'

if ([string]::IsNullOrWhiteSpace($ResourceGroup)) { $ResourceGroup = Get-VarOrDefault $vars 'RESOURCE_GROUP' 'FabricCapacityWestUS3' }
if ([string]::IsNullOrWhiteSpace($Location))      { $Location      = Get-VarOrDefault $vars 'LOCATION' 'westus3' }
if ([string]::IsNullOrWhiteSpace($Sku))           { $Sku           = Get-VarOrDefault $vars 'SKU' 'F4' }
if ([string]::IsNullOrWhiteSpace($CapacityName))  { $CapacityName  = Get-VarOrDefault $vars 'CAPACITY_NAME' 'westus3f4skillsfghcpcliwin' }
if ([string]::IsNullOrWhiteSpace($WorkspaceName)) { $WorkspaceName = Get-VarOrDefault $vars 'WORKSPACE_NAME' 'MedicareSkillsF4ghcpcliwin' }
if ([string]::IsNullOrWhiteSpace($LakehouseName)) { $LakehouseName = Get-VarOrDefault $vars 'LAKEHOUSE_NAME' 'MedicareSkillsF4TerminalLHghcpcliwin' }

# ─── STEP 0: PREFLIGHT ────────────────────────────────────────────────────────

Write-Log 'Step 0 - Preflight checks'
Test-Prerequisites

$subscriptionId = Invoke-Az @('account','show','--query','id','--output','tsv')
$adminEmail     = Invoke-Az @('account','show','--query','user.name','--output','tsv')
if ([string]::IsNullOrWhiteSpace($subscriptionId)) { Write-Fail "Not logged in. Run 'az login' first." }
Write-Ok "Logged in as $adminEmail (subscription: $subscriptionId)"

if (-not (Test-Path -LiteralPath $ZipSourceDir)) { Write-Fail "Zip directory not found: $ZipSourceDir" }
$years = Get-ZipYears -ZipSourceDir $ZipSourceDir
if (-not $years -or $years.Count -eq 0) {
    Write-Fail "No zip files matching ${FilePrefix}_YYYY.zip found in $ZipSourceDir"
}
$zipCount = (Get-ChildItem -LiteralPath $ZipSourceDir -Filter '*.zip').Count
Write-Ok "Found $zipCount zip file(s) - years: $($years -join ', ')"

foreach ($nb in @('UnzipMedicareFiles.ipynb','LoadMedicarePartDfiles.ipynb')) {
    if (-not (Test-Path -LiteralPath (Join-Path $NotebookDir $nb))) { Write-Fail "$nb not found in $NotebookDir" }
}
Write-Ok 'Both notebooks found'

# ─── STEP 1: CREATE RESOURCE GROUP ────────────────────────────────────────────

Write-Log "Step 1 - Create Resource Group ($ResourceGroup in $Location)"

$existingRg = Invoke-Az @('group','show','--name',$ResourceGroup,'--query','name','--output','tsv')
if (-not [string]::IsNullOrWhiteSpace($existingRg)) {
    Write-Ok 'Resource Group already exists, skipping creation'
} else {
    Invoke-Az @('group','create','--name',$ResourceGroup,'--location',$Location,'--output','none') | Out-Null
    Write-Ok 'Resource Group created'
}

# ─── STEP 2: CREATE FABRIC CAPACITY ───────────────────────────────────────────

Write-Log "Step 2 - Create Fabric Capacity ($CapacityName, $Sku in $Location)"

$mgmtUrl = "https://management.azure.com/subscriptions/$subscriptionId/resourceGroups/$ResourceGroup/providers/Microsoft.Fabric/capacities/$CapacityName?api-version=2023-11-01"

$existingState = Invoke-Az @('rest','--url',$mgmtUrl,'--query','properties.state','--output','tsv')
if (-not [string]::IsNullOrWhiteSpace($existingState)) {
    Write-Ok "Capacity already exists (state: $existingState)"
    if ($existingState -eq 'Paused') {
        Write-Info 'Resuming paused capacity...'
        $resumeUrl = "https://management.azure.com/subscriptions/$subscriptionId/resourceGroups/$ResourceGroup/providers/Microsoft.Fabric/capacities/$CapacityName/resume?api-version=2023-11-01"
        Invoke-Az @('rest','--method','post','--url',$resumeUrl) | Out-Null
    }
} else {
    $capBody = @{
        location   = $Location
        sku        = @{ name = $Sku; tier = 'Fabric' }
        properties = @{ administration = @{ members = @($adminEmail) } }
    } | ConvertTo-Json -Depth 5 -Compress
    $created = Invoke-AzRestBody -Method put -Url $mgmtUrl -Body $capBody
    if ($null -eq $created) { Write-Fail 'Could not create capacity' }
}

Write-Info 'Waiting for capacity to be ready...'
$state = ''
for ($i = 1; $i -le 30; $i++) {
    $state = Invoke-Az @('rest','--url',$mgmtUrl,'--query','properties.state','--output','tsv')
    Write-Host "    [$i] $state"
    if ($state -eq 'Active') { break }
    Start-Sleep -Seconds 10
}
if ($state -ne 'Active') { Write-Fail "Capacity not active: $state" }

$fabricCapacityId = Invoke-Az @('rest','--resource',$FabricApi,'--url',"$FabricApi/v1/capacities",
    '--query',"value[?displayName=='$CapacityName'].id | [0]",'--output','tsv')
Write-Ok "Capacity ID: $fabricCapacityId"

# ─── STEP 3: CREATE WORKSPACE ─────────────────────────────────────────────────

Write-Log "Step 3 - Create Workspace ($WorkspaceName)"

$wsId = Invoke-Az @('rest','--resource',$FabricApi,'--url',"$FabricApi/v1/workspaces",
    '--query',"value[?displayName=='$WorkspaceName'].id | [0]",'--output','tsv')

if (-not [string]::IsNullOrWhiteSpace($wsId)) {
    Write-Ok "Workspace already exists: $wsId"
} else {
    $wsBody = @{ displayName = $WorkspaceName; capacityId = $fabricCapacityId } | ConvertTo-Json -Compress
    $wsId = Invoke-AzRestBody -Method post -Resource $FabricApi -Url "$FabricApi/v1/workspaces" -Body $wsBody -Query 'id'
    if ([string]::IsNullOrWhiteSpace($wsId)) { Write-Fail 'Could not create workspace' }
    Write-Ok "Workspace created: $wsId"

    Write-Info 'Verifying capacity assignment...'
    $progress = ''
    for ($i = 1; $i -le 10; $i++) {
        $progress = Invoke-Az @('rest','--resource',$FabricApi,'--url',"$FabricApi/v1/workspaces/$wsId",
            '--query','capacityAssignmentProgress','--output','tsv')
        if ($progress -eq 'Completed') { break }
        Start-Sleep -Seconds 5
    }
    if ($progress -ne 'Completed') { Write-Fail "Capacity assignment not completed: $progress" }
    Write-Ok 'Capacity assignment completed'
}

# ─── STEP 4: CREATE LAKEHOUSE ─────────────────────────────────────────────────

Write-Log "Step 4 - Create Lakehouse ($LakehouseName)"

$lhId = Invoke-Az @('rest','--resource',$FabricApi,'--url',"$FabricApi/v1/workspaces/$wsId/items",
    '--query',"value[?displayName=='$LakehouseName' && type=='Lakehouse'].id | [0]",'--output','tsv')

if (-not [string]::IsNullOrWhiteSpace($lhId)) {
    Write-Ok "Lakehouse already exists: $lhId"
} else {
    $lhBody = @{ displayName = $LakehouseName; type = 'Lakehouse'; creationPayload = @{ enableSchemas = $true } } | ConvertTo-Json -Compress
    $lhId = Invoke-AzRestBody -Method post -Resource $FabricApi -Url "$FabricApi/v1/workspaces/$wsId/items" -Body $lhBody -Query 'id'
    if ([string]::IsNullOrWhiteSpace($lhId)) { Write-Fail 'Could not create lakehouse' }
    Write-Ok "Lakehouse created: $lhId"
}

# ─── STEP 5: UPLOAD ZIP FILES TO ONELAKE ──────────────────────────────────────

Write-Log 'Step 5 - Upload zip files to OneLake (blob endpoint)'
Send-ZipsToOneLake -ZipSourceDir $ZipSourceDir -WsId $wsId -LhId $lhId

# ─── STEP 6: PREPARE AND DEPLOY NOTEBOOKS ─────────────────────────────────────

Write-Log 'Step 6 - Prepare and deploy notebooks with lakehouse binding'
New-NotebookBodies -NotebookDir $NotebookDir -TmpDir $TmpDir -WsId $wsId -LhId $lhId -LhName $LakehouseName -Years $years
$unzipNbId = Deploy-Notebook -Name 'UnzipMedicareFiles'     -WsId $wsId -TmpDir $TmpDir
$loadNbId  = Deploy-Notebook -Name 'LoadMedicarePartDfiles' -WsId $wsId -TmpDir $TmpDir

# ─── STEP 7: RUN UNZIP NOTEBOOK ───────────────────────────────────────────────

Write-Log 'Step 7 - Run UnzipMedicareFiles notebook'
$unzipJobId = Submit-NotebookJob -WsId $wsId -NbId $unzipNbId
if ([string]::IsNullOrWhiteSpace($unzipJobId)) { Write-Fail 'Could not submit unzip notebook job' }
Wait-FabricJob -WsId $wsId -ItemId $unzipNbId -JobId $unzipJobId -Label 'UnzipMedicareFiles' -MaxPolls 60 -IntervalSec 30

# ─── STEP 8: RUN LOAD NOTEBOOK ────────────────────────────────────────────────

Write-Log 'Step 8 - Run LoadMedicarePartDfiles notebook'
$loadJobId = Submit-NotebookJob -WsId $wsId -NbId $loadNbId
if ([string]::IsNullOrWhiteSpace($loadJobId)) { Write-Fail 'Could not submit load notebook job' }
Wait-FabricJob -WsId $wsId -ItemId $loadNbId -JobId $loadJobId -Label 'LoadMedicarePartDfiles' -MaxPolls 120 -IntervalSec 30

# ─── STEP 9: VERIFY ───────────────────────────────────────────────────────────

Write-Log 'Step 9 - Verify Delta table'
Test-DeltaTable -WsId $wsId -LhId $lhId

# ─── SUMMARY ──────────────────────────────────────────────────────────────────

Write-Log 'DEPLOYMENT COMPLETE'
Write-Host ''
Write-Host "  Capacity:  $CapacityName  ($fabricCapacityId)"
Write-Host "  Workspace: $WorkspaceName ($wsId)"
Write-Host "  Lakehouse: $LakehouseName ($lhId)"
Write-Host "  Unzip NB:  UnzipMedicareFiles ($unzipNbId)"
Write-Host "  Load NB:   LoadMedicarePartDfiles ($loadNbId)"
Write-Host "  Table:     mcpd.medicarepartd"
Write-Host ''
Write-Host '  To query in Fabric SQL:'
Write-Host "    SELECT [year], count(*) as numberofrows"
Write-Host "    FROM [$LakehouseName].[mcpd].[medicarepartd]"
Write-Host "    GROUP BY [year]"
Write-Host ''

Remove-Item -LiteralPath $TmpDir -Recurse -Force -ErrorAction SilentlyContinue
