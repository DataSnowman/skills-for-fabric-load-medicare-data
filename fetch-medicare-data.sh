#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# fetch-medicare-data.sh
#
# Download Medicare Part D "by Provider and Drug" data and package it into the
# zip files the deploy scripts expect, directly into data/DemoZippedFiles/.
#
# WHY THIS EXISTS:
#   The CMS dataset page's "Download" button is JavaScript-driven, so `curl`
#   against .../data (or .../data/YYYY) only saves a ~3 KB HTML page, NOT a zip.
#   Instead we download the underlying static CSV (curl-able) and repackage it
#   into a zip whose inner file is renamed to the exact name the load notebook
#   requires: Medicare_Part_D_Prescribers_by_Provider_and_Drug_YYYY.csv
#
# Prerequisites: curl, python3 (both present in the Dev Container / Codespace).
#
# Usage:
#   ./fetch-medicare-data.sh                 # default years: 2024 2023 2022
#   ./fetch-medicare-data.sh 2024 2023       # only these years
#   FORCE=1 ./fetch-medicare-data.sh 2024    # re-download even if zip exists
#
# Each CSV is 3.7-4.1 GB; each CSV is deleted right after zipping, so peak disk
# stays under ~5 GB (fits a default 32 GB Codespace). Zipping takes a few
# minutes per year.
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEST_DIR="$SCRIPT_DIR/data/DemoZippedFiles"
FILE_PREFIX="Medicare_Part_D_Prescribers_by_Provider_and_Drug"
FORCE="${FORCE:-0}"

# ─── HELPERS ─────────────────────────────────────────────────────────────────
log()  { echo ""; echo "=== $1 ==="; }
info() { echo "  → $1"; }
fail() { echo "  ✗ FAILED: $1"; exit 1; }
ok()   { echo "  ✓ $1"; }

# ─── STATIC CSV URLs (verified working via curl) ─────────────────────────────
# These data.cms.gov/sites/default/files/... paths can change when CMS
# republishes. If a year 404s, get the current link from a browser:
#   open the dataset page, DevTools -> Network, click Download, copy the .csv URL
#   https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug
csv_url_for() {
  case "$1" in
    2024) echo "https://data.cms.gov/sites/default/files/2026-05/0ae165f4-eb44-495d-8cac-67f4571b6b83/MUP_DPR_RY26_P04_V10_DY24_NPIBN.csv" ;;
    2023) echo "https://data.cms.gov/sites/default/files/2025-04/0d5915ce-002c-4d87-bde8-24ffb08bb6cc/MUP_DPR_RY25_P04_V10_DY23_NPIBN.csv" ;;
    2022) echo "https://data.cms.gov/sites/default/files/2024-05/18f82097-61a6-4889-9941-9a0b6ad7523c/MUP_DPR_RY24_P04_V10_DY22_NPIBN.csv" ;;
    *)    echo "" ;;
  esac
}

# ─── PREFLIGHT ───────────────────────────────────────────────────────────────
log "Preflight"
command -v curl    >/dev/null 2>&1 || fail "curl not found"
command -v python3 >/dev/null 2>&1 || fail "python3 not found"
mkdir -p "$DEST_DIR"
ok "curl and python3 available; destination: $DEST_DIR"

YEARS=("$@")
if [[ ${#YEARS[@]} -eq 0 ]]; then
  YEARS=(2024 2023 2022)
fi
info "Years: ${YEARS[*]}"

# ─── DOWNLOAD + REPACKAGE ────────────────────────────────────────────────────
for Y in "${YEARS[@]}"; do
  log "Year $Y"
  URL="$(csv_url_for "$Y")"
  if [[ -z "$URL" ]]; then
    fail "No known CSV URL for $Y. Add one to csv_url_for() in this script (see the browser DevTools tip above)."
  fi

  BASE="$FILE_PREFIX"_"$Y"
  CSV_PATH="$DEST_DIR/$BASE.csv"
  ZIP_PATH="$DEST_DIR/$BASE.zip"

  # Skip if a real (large) zip already exists, unless FORCE=1
  if [[ "$FORCE" != "1" && -f "$ZIP_PATH" ]]; then
    SIZE=$(stat -f%z "$ZIP_PATH" 2>/dev/null || stat --printf="%s" "$ZIP_PATH")
    if [[ "$SIZE" -gt 10000000 ]]; then
      ok "$BASE.zip already present ($((SIZE / 1024 / 1024)) MB) — skipping (set FORCE=1 to redownload)"
      continue
    fi
    info "Existing $BASE.zip is only $((SIZE / 1024)) KB (likely bad) — re-fetching"
  fi

  info "Downloading CSV..."
  curl -fL -o "$CSV_PATH" "$URL" || fail "download failed for $Y (URL may have rotated — see DevTools tip above)"

  # Reject tiny HTML responses (the classic 3 KB failure)
  CSIZE=$(stat -f%z "$CSV_PATH" 2>/dev/null || stat --printf="%s" "$CSV_PATH")
  if [[ "$CSIZE" -lt 1000000 ]]; then
    rm -f "$CSV_PATH"
    fail "downloaded file for $Y is only $((CSIZE / 1024)) KB — not the real CSV (URL likely rotated)"
  fi
  ok "CSV downloaded ($((CSIZE / 1024 / 1024)) MB)"

  info "Zipping (a few minutes for ~4 GB)..."
  python3 -c "import zipfile,sys; z=zipfile.ZipFile(sys.argv[2],'w',zipfile.ZIP_DEFLATED); z.write(sys.argv[1], arcname=sys.argv[1].split('/')[-1]); z.close()" "$CSV_PATH" "$ZIP_PATH" \
    || fail "zip failed for $Y"

  rm -f "$CSV_PATH"   # free disk; keep only the zip
  ZSIZE=$(stat -f%z "$ZIP_PATH" 2>/dev/null || stat --printf="%s" "$ZIP_PATH")
  ok "$BASE.zip ready ($((ZSIZE / 1024 / 1024)) MB, inner file $BASE.csv)"
done

log "Done"
info "Files in $DEST_DIR:"
ls -lh "$DEST_DIR"/*.zip
echo ""
info "Next: run ./deploy-medicare-to-workspace.sh"
