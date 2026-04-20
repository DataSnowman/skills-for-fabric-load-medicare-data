# E2E Deployment Timing Reference

Timing data from `deploy-medicare-e2e.sh` runs across different environments.

---

## Run Log

### Run 1 — macOS (MacBook Neo), 2026-04-20

| Field | Value |
|---|---|
| **Machine** | MacBook Neo (macOS) |
| **Total Time** | **34 min 16 sec** |
| **Zip Files** | 11 (2013–2023, ~8.1 GB total) |
| **Capacity** | `westus3f4skillsfghcpcliautopilot` (F4, westus3) |
| **Workspace** | `MedicareSkillsF4ghcpcliautopilot` |
| **Lakehouse** | `MedicareSkillsF4TerminalLHghcpcliautopilot` |
| **Run By** | GitHub Copilot CLI (autopilot) |

**Step Breakdown:**

| Step | Description | Duration (approx) | Notes |
|---|---|---|---|
| 0 | Preflight checks | < 10s | 11 zips detected, both notebooks found |
| 1 | Create Resource Group | < 5s | Already existed, skipped |
| 2 | Create Fabric Capacity | ~10s | Already existed (Active), got capacity ID |
| 3 | Create Workspace | ~15s | Created + capacity assignment verified |
| 4 | Create Lakehouse | ~5s | Created with schemas enabled |
| 5 | Upload 11 zips (~8.1 GB) | **~20 min** | All returned HTTP 201, no skips |
| 6 | Prepare & deploy notebooks | ~1 min | Both deployed + lakehouse bound |
| 7 | Run UnzipMedicareFiles notebook | **~6 min** | 12 poll cycles × 30s |
| 8 | Run LoadMedicarePartDfiles notebook | **~7 min** | 14 poll cycles × 30s |
| 9 | Verify Delta table | < 5s | `mcpd.medicarepartd` confirmed |

**Bottlenecks:**
- Upload (Step 5) is the dominant cost at ~20 min for ~8.1 GB
- On re-runs, uploads are skipped via HEAD check (idempotent), dropping total to ~14 min

---

### Run 2 — WSL (32GB Lenovo, Windows 11), 2026-04-17

| Field | Value |
|---|---|
| **Machine** | 32 GB Lenovo (Windows 11 + WSL Ubuntu-22.04) |
| **Total Time** | Not precisely timed (estimated ~40–50 min) |
| **Zip Files** | 3 initially (2013, 2014, 2015), later expanded |
| **Capacity** | `westus3f4skillsfghcpclilenovo` (F4, westus3) |
| **Notes** | WSL adds overhead: `az` wrapper strips `\r`, `--body @file` workaround needed, TMPDIR on Windows filesystem |

> WSL runs are slower due to Windows `az` binary overhead and cross-filesystem I/O.
> macOS native `az` is significantly faster for the upload step.

---

## Key Observations

1. **Upload is the bottleneck** — ~60% of total time is uploading ~8.1 GB to OneLake
2. **Idempotent re-runs are fast** — HEAD check skips existing blobs, dropping to ~14 min
3. **F4 capacity** is sufficient for unzip + load of 11 files (no Spark resource issues)
4. **macOS outperforms WSL** — native `az` CLI and direct filesystem access are faster
5. **Notebook execution** (unzip + load) takes ~13 min combined on F4

## Estimated Time by Scenario

| Scenario | Estimated Time |
|---|---|
| Fresh deploy, 11 zips, macOS | ~34 min |
| Re-run (blobs exist), macOS | ~14 min |
| Fresh deploy, 11 zips, WSL | ~40–50 min |
| Fresh deploy, 3 zips only | ~15 min |
