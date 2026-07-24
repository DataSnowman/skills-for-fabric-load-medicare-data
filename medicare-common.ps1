<#
.SYNOPSIS
    Shared helper functions for the native-PowerShell Medicare-on-Fabric deploy scripts.

.DESCRIPTION
    Dot-sourced by deploy-medicare-e2e.ps1 and deploy-medicare-to-workspace.ps1.
    Pure PowerShell (no bash/Python dependency): uses the Azure CLI (az), curl.exe for
    streamed OneLake uploads, and native JSON handling for notebook preparation.

    Requires: PowerShell 7+ (pwsh), Azure CLI (az), curl.exe (bundled with Windows 10+).
#>

Set-StrictMode -Version Latest

$FabricApi  = 'https://api.fabric.microsoft.com'
$StorageRes = 'https://storage.azure.com'
$OneLakeBlob = 'https://onelake.blob.fabric.microsoft.com'
$FilePrefix = 'Medicare_Part_D_Prescribers_by_Provider_and_Drug'

# ─── Logging helpers (mirror the bash log/info/ok/fail helpers) ───────────────

function Write-Log  { param([string]$Message) Write-Host ''; Write-Host "=== $Message ===" }
function Write-Info { param([string]$Message) Write-Host "  -> $Message" }
function Write-Ok   { param([string]$Message) Write-Host "  [OK] $Message" -ForegroundColor Green }
function Write-Fail {
    param([string]$Message)
    Write-Host "  [X] FAILED: $Message" -ForegroundColor Red
    exit 1
}

# ─── Prerequisite checks ──────────────────────────────────────────────────────

function Test-Prerequisites {
    if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
        Write-Fail "Azure CLI 'az' not found on PATH. Install from https://aka.ms/azure-cli"
    }
    if (-not (Get-Command curl.exe -ErrorAction SilentlyContinue)) {
        Write-Fail "curl.exe not found on PATH. It ships with Windows 10+ / PowerShell 7+."
    }
}

# ─── variables.md parser (single source of truth, shared with the bash scripts) ─

function Get-MedicareVariables {
    <#
        Parses config/variables.md and returns a hashtable of KEY -> value for every
        line of the form  KEY="value"  (trailing "# comments" are ignored). Lines that
        assign via $(...) command substitution are skipped, matching the bash loader.
    #>
    param([Parameter(Mandatory)][string]$VarsFile)

    $vars = @{}
    if (-not (Test-Path $VarsFile)) { return $vars }

    foreach ($line in Get-Content -LiteralPath $VarsFile) {
        $trimmed = $line.Trim()
        if ($trimmed.StartsWith('#')) { continue }
        # KEY="value"  — value is captured from inside the first pair of double quotes
        if ($trimmed -match '^([A-Z_][A-Z0-9_]*)="([^"]*)"') {
            $vars[$Matches[1]] = $Matches[2]
        }
    }
    return $vars
}

function Get-VarOrDefault {
    param(
        [hashtable]$Vars,
        [string]$Name,
        [string]$Default = ''
    )
    if ($Vars.ContainsKey($Name) -and -not [string]::IsNullOrWhiteSpace($Vars[$Name])) {
        return $Vars[$Name]
    }
    return $Default
}

# ─── az helpers ───────────────────────────────────────────────────────────────

function Invoke-Az {
    <# Runs 'az' and returns trimmed stdout. Returns '' on failure. #>
    param([Parameter(Mandatory)][string[]]$AzArgs)
    $out = & az @AzArgs 2>$null
    if ($LASTEXITCODE -ne 0) { return '' }
    return ($out | Out-String).Trim()
}

function Invoke-AzRestBody {
    <#
        Runs 'az rest' with a JSON request body. On Windows the az launcher is a .cmd that
        strips the quotes from an inline --body '{...}', producing malformed JSON, so the body
        MUST be passed from a temp file (--body @file). Surfaces az errors and returns '' on
        failure. Omit -Resource for ARM (management.azure.com) calls.
    #>
    param(
        [Parameter(Mandatory)][string]$Method,
        [Parameter(Mandatory)][string]$Url,
        [Parameter(Mandatory)][string]$Body,
        [string]$Query,
        [string]$Resource
    )
    $tmp = [System.IO.Path]::GetTempFileName()
    try {
        Set-Content -LiteralPath $tmp -Value $Body -Encoding utf8
        $azArgs = @('rest','--method',$Method,'--url',$Url,'--body',"@$tmp")
        if ($Resource) { $azArgs += @('--resource',$Resource) }
        if ($Query)    { $azArgs += @('--query',$Query,'--output','tsv') }
        $out = & az @azArgs 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host ("    az error: " + (($out | Out-String).Trim()))
            return ''
        }
        return ($out | Out-String).Trim()
    } finally {
        Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    }
}

function Get-StorageToken {
    $token = Invoke-Az @('account','get-access-token','--resource',$StorageRes,'--query','accessToken','--output','tsv')
    if ([string]::IsNullOrWhiteSpace($token)) { Write-Fail "Could not acquire a storage.azure.com token." }
    return $token
}

# ─── Year detection ───────────────────────────────────────────────────────────

function Get-ZipYears {
    <# Returns a sorted int[] of years detected from FILE_PREFIX_YYYY.zip files. #>
    param([Parameter(Mandatory)][string]$ZipSourceDir)

    $years = @()
    Get-ChildItem -LiteralPath $ZipSourceDir -Filter "$($FilePrefix)_*.zip" -ErrorAction SilentlyContinue |
        ForEach-Object {
            if ($_.BaseName -match '(\d{4})$') { $years += [int]$Matches[1] }
        }
    return ($years | Sort-Object -Unique)
}

# ─── OneLake upload (streamed via curl.exe, idempotent HEAD skip) ─────────────

function Send-ZipsToOneLake {
    param(
        [Parameter(Mandatory)][string]$ZipSourceDir,
        [Parameter(Mandatory)][string]$WsId,
        [Parameter(Mandatory)][string]$LhId
    )

    $token = Get-StorageToken
    $auth  = "Authorization: Bearer $token"
    $failures = 0
    $skipped  = 0

    $zips = Get-ChildItem -LiteralPath $ZipSourceDir -Filter '*.zip' -ErrorAction SilentlyContinue
    if (-not $zips) { Write-Fail "No zip files found in $ZipSourceDir" }

    foreach ($zip in $zips) {
        $name   = $zip.Name
        $sizeMb = [int]($zip.Length / 1MB)
        $url    = "$OneLakeBlob/$WsId/$LhId/Files/medicare/$name"

        $headCode = & curl.exe -s -o NUL -w '%{http_code}' -I `
            -H $auth -H 'x-ms-version: 2023-01-03' $url
        if ($headCode -eq '200') {
            Write-Host "  Skipping $name (${sizeMb}MB) - already uploaded"
            $skipped++
            continue
        }

        Write-Host "  Uploading $name (${sizeMb}MB)... " -NoNewline
        $httpCode = & curl.exe -s -o NUL -w '%{http_code}' -X PUT `
            -H $auth -H 'x-ms-version: 2023-01-03' -H 'x-ms-blob-type: BlockBlob' `
            --data-binary "@$($zip.FullName)" $url
        Write-Host $httpCode
        if ($httpCode -ne '201') { $failures++ }
    }

    if ($failures -gt 0) { Write-Fail "$failures file(s) failed to upload" }
    Write-Ok "All zip files uploaded ($skipped skipped, already present)"
}

# ─── Notebook preparation (native JSON, mirrors the bash Python block) ────────

function Set-NotebookListBlock {
    <#
        Replaces a  <listName> = [ ... ]  block inside a notebook cell's source with a
        freshly generated list. Mutates the passed-in notebook object in place.
    #>
    param(
        [Parameter(Mandatory)]$Notebook,
        [Parameter(Mandatory)][string]$ListName,
        [Parameter(Mandatory)][string]$MarkerText,   # a second token that must be present in the target cell
        [Parameter(Mandatory)][string[]]$Items
    )

    $itemLines = ($Items | ForEach-Object { "    '$_'" }) -join ",`n"
    $replacement = "$ListName = [`n$itemLines`n]"

    foreach ($cell in $Notebook.cells) {
        $src = -join $cell.source
        if ($src -match [regex]::Escape($ListName) -and $src -match [regex]::Escape($MarkerText)) {
            $pattern = "(?s)$([regex]::Escape($ListName)) = \[.*?\]"
            $newText = [regex]::Replace($src, $pattern, [System.Text.RegularExpressions.MatchEvaluator]{ param($m) $replacement })
            # Preserve the array-of-lines source style (each element keeps its trailing \n)
            $lines = $newText -split "(?<=`n)" | Where-Object { $_ -ne '' }
            $cell.source = [string[]]$lines
            return
        }
    }
    Write-Fail "Could not find a cell containing '$ListName' and '$MarkerText' to update."
}

function New-LakehouseDependency {
    param([string]$WsId,[string]$LhId,[string]$LhName)
    return [ordered]@{
        lakehouse = [ordered]@{
            default_lakehouse              = $LhId
            default_lakehouse_name         = $LhName
            default_lakehouse_workspace_id = $WsId
            known_lakehouses               = @(@{ id = $LhId })
        }
    }
}

function New-NotebookBodies {
    <#
        Reads the two notebooks, injects the year lists + lakehouse binding, and writes
        the deploy + updateDefinition request bodies to $TmpDir. Returns nothing.
    #>
    param(
        [Parameter(Mandatory)][string]$NotebookDir,
        [Parameter(Mandatory)][string]$TmpDir,
        [Parameter(Mandatory)][string]$WsId,
        [Parameter(Mandatory)][string]$LhId,
        [Parameter(Mandatory)][string]$LhName,
        [Parameter(Mandatory)][int[]]$Years
    )

    New-Item -ItemType Directory -Force -Path $TmpDir | Out-Null
    $deps = New-LakehouseDependency -WsId $WsId -LhId $LhId -LhName $LhName

    # --- UnzipMedicareFiles: zip_files list ---
    $unzipNb = Get-Content -LiteralPath (Join-Path $NotebookDir 'UnzipMedicareFiles.ipynb') -Raw | ConvertFrom-Json
    $zipItems = $Years | ForEach-Object { "/lakehouse/default/Files/medicare/$($FilePrefix)_$($_).zip" }
    Set-NotebookListBlock -Notebook $unzipNb -ListName 'zip_files' -MarkerText 'zipfile' -Items $zipItems
    $unzipNb.metadata.dependencies = $deps

    # --- LoadMedicarePartDfiles: full_files list ---
    $loadNb = Get-Content -LiteralPath (Join-Path $NotebookDir 'LoadMedicarePartDfiles.ipynb') -Raw | ConvertFrom-Json
    $csvItems = $Years | ForEach-Object { "$($FilePrefix)_$($_).csv" }
    Set-NotebookListBlock -Notebook $loadNb -ListName 'full_files' -MarkerText 'loadFullDataFromSource' -Items $csvItems
    $loadNb.metadata.dependencies = $deps

    foreach ($pair in @(@{ nb = $unzipNb; name = 'UnzipMedicareFiles' }, @{ nb = $loadNb; name = 'LoadMedicarePartDfiles' })) {
        $name  = $pair.name
        $nbJson = $pair.nb | ConvertTo-Json -Depth 100 -Compress
        $nbB64  = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($nbJson))

        # deploy body (create item)
        $deployBody = [ordered]@{
            displayName = $name
            type        = 'Notebook'
            definition  = [ordered]@{
                format = 'ipynb'
                parts  = @(@{ path = 'artifact.content.ipynb'; payload = $nbB64; payloadType = 'InlineBase64' })
            }
        }
        $deployBody | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath (Join-Path $TmpDir "$($name)_deploy_body.json") -Encoding utf8

        # updateDefinition body (attach .platform so the lakehouse binding sticks)
        $platform = [ordered]@{
            metadata = [ordered]@{ type = 'SparkNotebook'; displayName = $name }
            config   = [ordered]@{ version = '2.0'; logicalId = [guid]::NewGuid().ToString() }
        }
        $platformB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(($platform | ConvertTo-Json -Depth 100 -Compress)))
        $updateBody = [ordered]@{
            definition = [ordered]@{
                format = 'ipynb'
                parts  = @(
                    @{ path = 'artifact.content.ipynb'; payload = $nbB64;      payloadType = 'InlineBase64' },
                    @{ path = '.platform';              payload = $platformB64; payloadType = 'InlineBase64' }
                )
            }
        }
        $updateBody | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath (Join-Path $TmpDir "$($name)_update_body.json") -Encoding utf8
        Write-Ok "$name request bodies ready"
    }
}

function Deploy-Notebook {
    <#
        Creates the notebook if missing (then binds the lakehouse), or updates its
        definition if it already exists. Returns the notebook id.
    #>
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$WsId,
        [Parameter(Mandatory)][string]$TmpDir
    )

    $deployBody = Join-Path $TmpDir "$($Name)_deploy_body.json"
    $updateBody = Join-Path $TmpDir "$($Name)_update_body.json"

    $nbId = Invoke-Az @('rest','--resource',$FabricApi,'--url',"$FabricApi/v1/workspaces/$WsId/notebooks",
        '--query',"value[?displayName=='$Name'].id | [0]",'--output','tsv')

    if (-not [string]::IsNullOrWhiteSpace($nbId)) {
        Write-Info "$Name already exists, updating definition..."
        Invoke-Az @('rest','--method','post','--resource',$FabricApi,
            '--url',"$FabricApi/v1/workspaces/$WsId/notebooks/$nbId/updateDefinition",
            '--body',"@$updateBody") | Out-Null
        Write-Ok "$Name updated: $nbId"
        return $nbId
    }

    Write-Info "Deploying $Name..."
    Invoke-Az @('rest','--method','post','--resource',$FabricApi,
        '--url',"$FabricApi/v1/workspaces/$WsId/items",'--body',"@$deployBody") | Out-Null

    for ($i = 0; $i -lt 10; $i++) {
        $nbId = Invoke-Az @('rest','--resource',$FabricApi,'--url',"$FabricApi/v1/workspaces/$WsId/notebooks",
            '--query',"value[?displayName=='$Name'].id | [0]",'--output','tsv')
        if (-not [string]::IsNullOrWhiteSpace($nbId)) { break }
        Start-Sleep -Seconds 5
    }
    if ([string]::IsNullOrWhiteSpace($nbId)) { Write-Fail "Could not retrieve ID for $Name after deployment" }
    Write-Ok "$Name deployed: $nbId"

    Write-Info "Binding $Name to lakehouse..."
    Invoke-Az @('rest','--method','post','--resource',$FabricApi,
        '--url',"$FabricApi/v1/workspaces/$WsId/notebooks/$nbId/updateDefinition",
        '--body',"@$updateBody") | Out-Null
    Write-Ok "$Name bound to lakehouse"
    return $nbId
}

# ─── Notebook job submission + polling ────────────────────────────────────────

function Submit-NotebookJob {
    param([Parameter(Mandatory)][string]$WsId,[Parameter(Mandatory)][string]$NbId)

    $out = & az rest --method post --resource $FabricApi `
        --url "$FabricApi/v1/workspaces/$WsId/items/$NbId/jobs/instances?jobType=RunNotebook" `
        --body '{}' --verbose 2>&1 | Out-String

    $locLine = ($out -split "`n" | Where-Object { $_ -match "'Location'\s*:" } | Select-Object -Last 1)
    $guids = [regex]::Matches($locLine, '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}')
    if ($guids.Count -gt 0) { return $guids[$guids.Count - 1].Value }
    return ''
}

function Wait-FabricJob {
    param(
        [Parameter(Mandatory)][string]$WsId,
        [Parameter(Mandatory)][string]$ItemId,
        [Parameter(Mandatory)][string]$JobId,
        [Parameter(Mandatory)][string]$Label,
        [int]$MaxPolls = 120,
        [int]$IntervalSec = 30
    )
    Write-Info "Polling $Label (job $JobId)..."
    for ($i = 1; $i -le $MaxPolls; $i++) {
        $status = Invoke-Az @('rest','--resource',$FabricApi,
            '--url',"$FabricApi/v1/workspaces/$WsId/items/$ItemId/jobs/instances/$JobId",
            '--query','status','--output','tsv')
        Write-Host "    [$i] $status"
        switch ($status) {
            'Completed' { Write-Ok "$Label completed"; return }
            'Failed'    { Write-Fail "$Label ended with status: $status" }
            'Cancelled' { Write-Fail "$Label ended with status: $status" }
        }
        Start-Sleep -Seconds $IntervalSec
    }
    Write-Fail "$Label timed out after $($MaxPolls * $IntervalSec) seconds"
}

# ─── Delta table verification ─────────────────────────────────────────────────

function Test-DeltaTable {
    param([Parameter(Mandatory)][string]$WsId,[Parameter(Mandatory)][string]$LhId)

    $token = Get-StorageToken
    $body = & curl.exe -s -H "Authorization: Bearer $token" -H 'x-ms-version: 2023-01-03' `
        "$OneLakeBlob/$WsId/$LhId/Tables?restype=container&comp=list&prefix=mcpd&maxresults=5" | Out-String
    if ($body -match 'medicarepartd') {
        Write-Ok "Delta table mcpd.medicarepartd exists"
    } else {
        Write-Fail "Delta table not found"
    }
}
