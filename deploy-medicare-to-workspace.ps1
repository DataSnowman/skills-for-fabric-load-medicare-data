<#
.SYNOPSIS
    Deploy Medicare Part D data into an EXISTING Microsoft Fabric workspace (native PowerShell).

.DESCRIPTION
    Windows-native equivalent of deploy-medicare-to-workspace.sh. Use this when you already
    have a Resource Group, Fabric Capacity, and Workspace provisioned. Creates a Lakehouse,
    uploads the local zip files to OneLake, deploys + binds both notebooks, runs the unzip and
    load notebooks, and verifies the resulting Delta table.

    Values are read from config/variables.env (single source of truth, shared with the bash
    scripts). Override any of them with the parameters below.

.PARAMETER WsId
    Existing Fabric workspace GUID. Required (via parameter or config/variables.env).

.PARAMETER LakehouseName
    Name of the Lakehouse to create in the workspace.

.PREREQUISITES
    PowerShell 7+ (pwsh), Azure CLI (az) logged in via 'az login', curl.exe (Windows 10+).

.EXAMPLE
    pwsh ./deploy-medicare-to-workspace.ps1
    pwsh ./deploy-medicare-to-workspace.ps1 -WsId "dc7ad9cf-..." -LakehouseName "MedicarePartD"
#>
[CmdletBinding()]
param(
    [string]$WsId,
    [string]$LakehouseName
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $ScriptDir 'medicare-common.ps1')

# ─── CONFIGURATION ────────────────────────────────────────────────────────────

$VarsFile      = Join-Path $ScriptDir 'config/variables.env'
$vars          = Get-MedicareVariables -VarsFile $VarsFile
$ZipSourceDir  = Join-Path $ScriptDir 'data/DemoZippedFiles'
$NotebookDir   = Join-Path $ScriptDir 'notebooks'
$TmpDir        = Join-Path $ScriptDir '.tmp'

if ([string]::IsNullOrWhiteSpace($WsId))          { $WsId          = Get-VarOrDefault $vars 'WS_ID' }
if ([string]::IsNullOrWhiteSpace($LakehouseName)) { $LakehouseName = Get-VarOrDefault $vars 'LAKEHOUSE_NAME' 'MedicareSkillsTerminalLH' }

# ─── STEP 0: PREFLIGHT ────────────────────────────────────────────────────────

Write-Log 'Step 0 - Preflight checks'
Test-Prerequisites

$account = Invoke-Az @('account','show','--query','user.name','--output','tsv')
if ([string]::IsNullOrWhiteSpace($account)) { Write-Fail "Not logged in. Run 'az login' first." }
Write-Ok "Logged in as $account"

if ([string]::IsNullOrWhiteSpace($WsId)) {
    Write-Fail "WS_ID is empty. Set it in config/variables.env or pass -WsId."
}

$wsName = Invoke-Az @('rest','--resource',$FabricApi,'--url',"$FabricApi/v1/workspaces/$WsId",
    '--query','displayName','--output','tsv')
if ([string]::IsNullOrWhiteSpace($wsName)) {
    Write-Fail "Cannot access workspace $WsId. Check the ID and your permissions."
}
Write-Ok "Workspace: $wsName ($WsId)"

if (-not (Test-Path -LiteralPath $ZipSourceDir)) { Write-Fail "Zip directory not found: $ZipSourceDir" }
$zipCount = (Get-ChildItem -LiteralPath $ZipSourceDir -Filter '*.zip' -ErrorAction SilentlyContinue).Count
if ($zipCount -eq 0) { Write-Fail "No zip files found in $ZipSourceDir" }
$years = Get-ZipYears -ZipSourceDir $ZipSourceDir
if (-not $years -or $years.Count -eq 0) {
    Write-Fail "No zip files matching ${FilePrefix}_YYYY.zip found in $ZipSourceDir"
}
Write-Ok "Found $zipCount zip file(s) - years: $($years -join ', ')"

foreach ($nb in @('UnzipMedicareFiles.ipynb','LoadMedicarePartDfiles.ipynb')) {
    if (-not (Test-Path -LiteralPath (Join-Path $NotebookDir $nb))) { Write-Fail "$nb not found in $NotebookDir" }
}
Write-Ok 'Both notebooks found'

# ─── STEP 1: CREATE LAKEHOUSE ─────────────────────────────────────────────────

Write-Log "Step 1 - Create Lakehouse ($LakehouseName)"

$lhId = Invoke-Az @('rest','--resource',$FabricApi,'--url',"$FabricApi/v1/workspaces/$WsId/items",
    '--query',"value[?displayName=='$LakehouseName' && type=='Lakehouse'].id | [0]",'--output','tsv')

if (-not [string]::IsNullOrWhiteSpace($lhId)) {
    Write-Ok "Lakehouse already exists: $lhId"
} else {
    $lhBody = @{ displayName = $LakehouseName; type = 'Lakehouse'; creationPayload = @{ enableSchemas = $true } } | ConvertTo-Json -Compress
    $lhId = Invoke-AzRestBody -Method post -Resource $FabricApi -Url "$FabricApi/v1/workspaces/$WsId/items" -Body $lhBody -Query 'id'
    if ([string]::IsNullOrWhiteSpace($lhId)) { Write-Fail 'Could not create lakehouse' }
    Write-Ok "Lakehouse created: $lhId"
}

# ─── STEP 2: UPLOAD ZIP FILES TO ONELAKE ──────────────────────────────────────

Write-Log 'Step 2 - Upload zip files to OneLake (blob endpoint)'
Send-ZipsToOneLake -ZipSourceDir $ZipSourceDir -WsId $WsId -LhId $lhId

# ─── STEP 3: PREPARE AND DEPLOY NOTEBOOKS ─────────────────────────────────────

Write-Log 'Step 3 - Prepare and deploy notebooks with lakehouse binding'
New-NotebookBodies -NotebookDir $NotebookDir -TmpDir $TmpDir -WsId $WsId -LhId $lhId -LhName $LakehouseName -Years $years
$unzipNbId = Deploy-Notebook -Name 'UnzipMedicareFiles'     -WsId $WsId -TmpDir $TmpDir
$loadNbId  = Deploy-Notebook -Name 'LoadMedicarePartDfiles' -WsId $WsId -TmpDir $TmpDir

# ─── STEP 4: RUN UNZIP NOTEBOOK ───────────────────────────────────────────────

Write-Log 'Step 4 - Run UnzipMedicareFiles notebook'
$unzipJobId = Submit-NotebookJob -WsId $WsId -NbId $unzipNbId
if ([string]::IsNullOrWhiteSpace($unzipJobId)) { Write-Fail 'Could not submit unzip notebook job' }
Wait-FabricJob -WsId $WsId -ItemId $unzipNbId -JobId $unzipJobId -Label 'UnzipMedicareFiles' -MaxPolls 60 -IntervalSec 30

# ─── STEP 5: RUN LOAD NOTEBOOK ────────────────────────────────────────────────

Write-Log 'Step 5 - Run LoadMedicarePartDfiles notebook'
$loadJobId = Submit-NotebookJob -WsId $WsId -NbId $loadNbId
if ([string]::IsNullOrWhiteSpace($loadJobId)) { Write-Fail 'Could not submit load notebook job' }
Wait-FabricJob -WsId $WsId -ItemId $loadNbId -JobId $loadJobId -Label 'LoadMedicarePartDfiles' -MaxPolls 120 -IntervalSec 30

# ─── STEP 6: VERIFY ───────────────────────────────────────────────────────────

Write-Log 'Step 6 - Verify Delta table'
Test-DeltaTable -WsId $WsId -LhId $lhId

# ─── SUMMARY ──────────────────────────────────────────────────────────────────

Write-Log 'DEPLOYMENT COMPLETE'
Write-Host ''
Write-Host "  Workspace: $wsName ($WsId)"
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

# Clean up temp files
Remove-Item -LiteralPath $TmpDir -Recurse -Force -ErrorAction SilentlyContinue
