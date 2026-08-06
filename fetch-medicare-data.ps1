<#
.SYNOPSIS
    Download Medicare Part D data and package it into the zip files the deploy scripts expect.

.DESCRIPTION
    Windows-native equivalent of fetch-medicare-data.sh.

    The CMS dataset page's "Download" button is JavaScript-driven, so downloading .../data
    (or .../data/YYYY) only saves a ~3 KB HTML page, NOT a zip. Instead this script downloads
    the underlying static CSV (which IS directly downloadable) and repackages it into a zip whose
    inner file is renamed to the exact name the load notebook requires:
    Medicare_Part_D_Prescribers_by_Provider_and_Drug_YYYY.csv

    Each CSV is 3.7-4.1 GB and is deleted right after zipping, so peak disk stays under ~5 GB.

.PARAMETER Years
    Years to fetch. Default: 2024, 2023, 2022.

.PARAMETER Force
    Re-download even if a valid (large) zip already exists.

.PREREQUISITES
    PowerShell 7+ (pwsh). Uses Invoke-WebRequest and .NET System.IO.Compression.

.EXAMPLE
    pwsh ./fetch-medicare-data.ps1
    pwsh ./fetch-medicare-data.ps1 -Years 2024,2023
    pwsh ./fetch-medicare-data.ps1 -Years 2024 -Force
#>
[CmdletBinding()]
param(
    [int[]]$Years = @(2024, 2023, 2022),
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$DestDir    = Join-Path $ScriptDir 'data/DemoZippedFiles'
$FilePrefix = 'Medicare_Part_D_Prescribers_by_Provider_and_Drug'

function Write-Log  { param($m) Write-Host "`n=== $m ===" -ForegroundColor Cyan }
function Write-Info { param($m) Write-Host "  -> $m" }
function Write-Ok   { param($m) Write-Host "  [OK] $m" -ForegroundColor Green }
function Fail       { param($m) Write-Host "  [FAILED] $m" -ForegroundColor Red; exit 1 }

# Static CSV URLs (verified working). These data.cms.gov/sites/default/files/... paths can
# change when CMS republishes. If a year 404s, get the current link from a browser: open the
# dataset page, DevTools -> Network, click Download, copy the .csv URL.
#   https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug
$CsvUrls = @{
    2024 = 'https://data.cms.gov/sites/default/files/2026-05/0ae165f4-eb44-495d-8cac-67f4571b6b83/MUP_DPR_RY26_P04_V10_DY24_NPIBN.csv'
    2023 = 'https://data.cms.gov/sites/default/files/2025-04/0d5915ce-002c-4d87-bde8-24ffb08bb6cc/MUP_DPR_RY25_P04_V10_DY23_NPIBN.csv'
    2022 = 'https://data.cms.gov/sites/default/files/2024-05/18f82097-61a6-4889-9941-9a0b6ad7523c/MUP_DPR_RY24_P04_V10_DY22_NPIBN.csv'
}

Write-Log 'Preflight'
New-Item -ItemType Directory -Force -Path $DestDir | Out-Null
Write-Ok "Destination: $DestDir"
Write-Info "Years: $($Years -join ', ')"

Add-Type -AssemblyName System.IO.Compression.FileSystem

foreach ($Y in $Years) {
    Write-Log "Year $Y"
    if (-not $CsvUrls.ContainsKey($Y)) {
        Fail "No known CSV URL for $Y. Add one to `$CsvUrls in this script (see the browser DevTools tip above)."
    }
    $url     = $CsvUrls[$Y]
    $base    = "${FilePrefix}_${Y}"
    $csvPath = Join-Path $DestDir "$base.csv"
    $zipPath = Join-Path $DestDir "$base.zip"

    if ((-not $Force) -and (Test-Path $zipPath) -and ((Get-Item $zipPath).Length -gt 10MB)) {
        Write-Ok "$base.zip already present ($([math]::Round((Get-Item $zipPath).Length/1MB)) MB) - skipping (use -Force to redownload)"
        continue
    }

    Write-Info 'Downloading CSV...'
    try {
        Invoke-WebRequest -Uri $url -OutFile $csvPath -UseBasicParsing
    } catch {
        Fail "download failed for $Y (URL may have rotated - see DevTools tip above): $($_.Exception.Message)"
    }

    $csize = (Get-Item $csvPath).Length
    if ($csize -lt 1MB) {
        Remove-Item $csvPath -Force
        Fail "downloaded file for $Y is only $([math]::Round($csize/1KB)) KB - not the real CSV (URL likely rotated)"
    }
    Write-Ok "CSV downloaded ($([math]::Round($csize/1MB)) MB)"

    Write-Info 'Zipping (a few minutes for ~4 GB)...'
    if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
    $zip = $null
    try {
        $zip   = [System.IO.Compression.ZipFile]::Open($zipPath, 'Create')
        [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile($zip, $csvPath, "$base.csv") | Out-Null
    } finally {
        if ($zip) { $zip.Dispose() }
    }

    Remove-Item $csvPath -Force   # free disk; keep only the zip
    Write-Ok "$base.zip ready ($([math]::Round((Get-Item $zipPath).Length/1MB)) MB, inner file $base.csv)"
}

Write-Log 'Done'
Get-ChildItem "$DestDir/*.zip" | Select-Object Name, @{n='SizeMB';e={[math]::Round($_.Length/1MB)}} | Format-Table -AutoSize
Write-Info 'Next: pwsh ./deploy-medicare-to-workspace.ps1'
