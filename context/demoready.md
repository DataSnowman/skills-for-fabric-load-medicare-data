# Demo Ready — Multi-OS (Windows/PowerShell) Enablement

> **Status: ✅ COMPLETE and validated by a real end-to-end deployment on Windows.**
> This document captures the full context of the work that made this repo multi-OS, the
> runtime issues discovered while running it live, and the exact state of the successful demo.

---

## 1. Objective

The repo was authored on a MacBook and deployed Medicare Part D data into Microsoft Fabric
using two **bash** scripts (`deploy-medicare-e2e.sh`, `deploy-medicare-to-workspace.sh`). The
README stated *"Native PowerShell is not supported."* The goal was to make the repo run
**natively on Windows** (PowerShell) as well as macOS/Linux — without breaking the bash path.

**Approach chosen (confirmed with the user):**
- Add native PowerShell (`.ps1`) scripts **alongside** the bash scripts.
- Cover **both** deployment paths (full e2e + existing workspace).
- Keep `config/variables.md` as the **single source of truth** read by both bash and PowerShell.
- Pure PowerShell (no Python/bash dependency): `az` CLI, `[Convert]::ToBase64String` for
  base64, `curl.exe` for streamed OneLake uploads, native JSON for notebook prep.

---

## 2. What was added / changed

### New files
| File | Purpose |
|---|---|
| `deploy-medicare-e2e.ps1` | Native full deploy: Resource Group → Fabric Capacity → Workspace → Lakehouse → upload → notebooks → load → verify. Idempotent. |
| `deploy-medicare-to-workspace.ps1` | Native existing-workspace deploy (create lakehouse → upload → notebooks → load → verify). |
| `medicare-common.ps1` | Shared helpers dot-sourced by both `.ps1` scripts (variables.md parser, logging, OneLake upload, notebook JSON prep, job polling, `az` wrappers). |
| `.gitattributes` | Forces `*.sh` to check out **LF** so bash scripts stay valid on Windows clones where `core.autocrlf=true`. |

### Modified files
| File | Change |
|---|---|
| `deploy-medicare-to-workspace.sh` | **Bug fix:** replaced hardcoded macOS notebook path (`/Users/darwinschweitzer/...`) with `"$SCRIPT_DIR/notebooks"` (added `SCRIPT_DIR`). |
| `config/variables.md` | Reflects the 3 shipped zips (2022/2023/2024), added a 2024 reference row + `full_files` entry, made guidance OS-neutral, documented that both bash & PowerShell read this file. |
| `context/LoadMedicareData.md` | Removed macOS-only paths (`~/Desktop`, `/Users/...`) → repo-relative; added cross-platform base64/PowerShell notes and a pointer to the `.ps1` scripts. |
| `README.md` | Removed *"Native PowerShell is not supported"*; added a Cross-Platform Support section, PowerShell prerequisites (PowerShell 7+, `az`, `curl.exe`), and PowerShell run commands for both options; updated repo-structure listing. |

---

## 3. Design notes

- **Single source of truth:** `Get-MedicareVariables` in `medicare-common.ps1` parses the same
  `KEY="value"` lines from the `bash` code block in `config/variables.md` that the bash loader
  consumes — no duplicated config.
- **Year auto-detection:** both script families detect whichever
  `Medicare_Part_D_..._YYYY.zip` files are present in `data/DemoZippedFiles/` (currently
  2022, 2023, 2024) and generate the notebook `zip_files` / `full_files` lists accordingly.
- **Notebook prep is native:** PowerShell loads each `.ipynb`, regex-replaces the list block,
  injects `metadata.dependencies.lakehouse` (the binding trick from
  `updateDefinitionNotebookEndpoint.md`), and re-serializes with `ConvertTo-Json -Depth 100`.
- **Large uploads stream:** uploads use `curl.exe --data-binary @file` (streams multi-GB zips)
  rather than `Invoke-WebRequest` (which buffers the whole file in memory). Idempotent via a
  HEAD check that skips blobs already present (HTTP 200).

---

## 4. Runtime bugs found & fixed during the live run (Windows / `az.cmd`)

These were discovered only by actually running the deployment on Windows and are now fixed in
the committed scripts:

1. **`az rest` inline JSON body fails on Windows.**
   `--body '{...}'` gets its quotes stripped by the `az.cmd` launcher, producing malformed
   JSON → `UnsupportedMediaType` / `InvalidInput` ("Unexpected character encountered while
   parsing value: M. Path 'displayName'").
   **Fix:** added `Invoke-AzRestBody` in `medicare-common.ps1` which writes the body to a temp
   file and passes `--body @file`. All lakehouse/workspace/capacity create calls in both `.ps1`
   scripts were converted to use it.

2. **`Submit-NotebookJob` parsed the wrong verbose line.**
   The `az rest --verbose` output contains both the real `'Location': 'https://.../jobs/instances/<jobId>'`
   header **and** a later `'Access-Control-Expose-Headers': 'RequestId,Location,Retry-After'`
   line. The original `-match 'Location' | Select -Last 1` grabbed the CORS line (no GUID) →
   "Could not submit notebook job".
   **Fix:** tightened the filter to the quoted header key `'Location'\s*:` so the job GUID is
   parsed correctly.

> Both of these are Windows-specific `az.cmd` behaviors; the bash scripts on macOS/Linux are
> unaffected.

---

## 5. Successful demo run (facts)

- **Account/tenant:** `darsch@MngEnvMCAP763632.onmicrosoft.com`
  (subscription `ME-MngEnvMCAP763632-darsch-1`, tenant `d0fa34a0-...`).
  > Note: the `darsch@microsoft.com` corp account could **not** see the target workspace — the
  > `MngEnvMCAP763632` tenant account is required.
- **Script run:** `pwsh -NoProfile -File .\deploy-medicare-to-workspace.ps1`
- **Result — every step passed:**

| Step | Outcome |
|---|---|
| Preflight (login, workspace access, zips, notebooks) | ✅ |
| Create/detect Lakehouse | ✅ `MedicarePartD` |
| Upload 3 zips (2022/2023/2024, ~2.4 GB total) | ✅ HTTP 201 (idempotent HEAD-skip on re-runs) |
| Deploy + bind both notebooks | ✅ |
| Run `UnzipMedicareFiles` (Spark) | ✅ Completed |
| Run `LoadMedicarePartDfiles` (Spark → Delta) | ✅ Completed |
| Verify Delta table | ✅ `mcpd.medicarepartd` exists |

**Resource IDs from the run:**
| Resource | Name | ID |
|---|---|---|
| Workspace | `ZavaPharma` | `4b284d1a-ac92-442a-b575-fa9a025d59a0` |
| Lakehouse | `MedicarePartD` | `8c8053be-3f52-4630-8081-b1149354e01e` |
| Unzip notebook | `UnzipMedicareFiles` | `f6cfbeac-3726-4abc-a32e-cc07412d6799` |
| Load notebook | `LoadMedicarePartDfiles` | `6e4eb9fd-a473-4221-9a4c-c9b3c6c7c105` |

**Verify query (Fabric SQL):**
```sql
SELECT [year], count(*) AS numberofrows
FROM [MedicarePartD].[mcpd].[medicarepartd]
GROUP BY [year]
```

---

## 6. How to run it (Windows-native)

```powershell
# 1. Log into the correct tenant/account
az login            # or: az account set --subscription "ME-MngEnvMCAP763632-darsch-1"

# 2. Existing workspace (values already in config/variables.md)
pwsh ./deploy-medicare-to-workspace.ps1

# --- or, to build brand-new infrastructure ---
pwsh ./deploy-medicare-e2e.ps1
```

Bash equivalents remain available for macOS/Linux/WSL/Git Bash:
```bash
./deploy-medicare-to-workspace.sh      # existing workspace
./deploy-medicare-e2e.sh               # full deployment
```

> **Idempotency:** re-running skips already-uploaded zips (HEAD 200) and updates existing
> notebooks in place. ⚠️ The load notebook **appends** — re-running the load step duplicates
> rows unless the table is cleared or the notebook uses overwrite mode.

---

## 7. Prerequisites (PowerShell path)

- **PowerShell 7+** (`pwsh`) — validated on 7.6.4
- **Azure CLI** (`az`) — validated on 2.87.0 — logged in via `az login`
- **`curl.exe`** — bundled with Windows 10+ / PowerShell 7+
- **No Python required** for the PowerShell scripts (the bash scripts still use Python 3)
- **Microsoft Fabric** workspace on an **F4+** capacity (F2 lacks Spark resources)

---

## 8. Validation performed (before the live run)

- All three `.ps1` files parse cleanly (`[System.Management.Automation.Language.Parser]::ParseFile`).
- `variables.md` parser reads the real config (WS_ID, LAKEHOUSE_NAME, etc.).
- Year detection returns `2022, 2023, 2024`.
- Notebook prep produces **valid ipynb JSON** with correct `zip_files` / `full_files` lists and
  the lakehouse binding injected.
- `Invoke-Az` wrapper works against the real `az` CLI.
- Both `.sh` scripts still pass `bash -n` when checked out with LF endings.

---

## 9. Original plan (all 8 todos completed)

1. **fix-bash-notebook-path** — repo-relative `notebooks/` in `deploy-medicare-to-workspace.sh`. ✅
2. **ps-existing-workspace** — `deploy-medicare-to-workspace.ps1`. ✅
3. **ps-e2e** — `deploy-medicare-e2e.ps1`. ✅
4. **ps-shared-helpers** — `medicare-common.ps1`. ✅
5. **update-variables-md** — reflect 2022/2023/2024, OS-neutral guidance. ✅
6. **update-loadmedicaredata-md** — remove macOS paths, cross-platform notes. ✅
7. **update-readme** — remove "not supported", add PowerShell Quick Start + prereqs. ✅
8. **validate** — static parse + parser dry-run (then live-verified by the demo run). ✅

---

## 10. Repository conventions established (for future work)

- Deploy scripts exist in **both** bash (`.sh`) and PowerShell (`.ps1`); keep them in parity.
- `config/variables.md` is the single source of truth read by both families.
- `.sh` files must stay **LF-only** (enforced via `.gitattributes`) so they run under
  WSL/Git Bash on Windows clones (`core.autocrlf=true`).
- On Windows, `az rest` bodies must be passed via `--body @file` (never inline) — see the
  `Invoke-AzRestBody` helper.
