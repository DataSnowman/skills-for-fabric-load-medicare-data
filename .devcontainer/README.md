# Dev Container Quick Start

Welcome! This chapter gets you from **zero to a fully provisioned Linux
development environment** for loading Medicare Part D data into Microsoft Fabric —
without installing anything on your local machine. The dev container ships with the
Azure CLI, PowerShell 7, the GitHub CLI, GitHub Copilot CLI, Claude Code, and Python/Jupyter
all pre-installed, so you can run the deployment scripts the moment the container
finishes building.

> 💡 This is the **GitHub Codespaces** path. If you'd rather run the scripts on your
> own machine, see the [main README](../README.md) for local (Windows / macOS / Linux)
> instructions.

## 🎯 Learning Objectives

By the end of this chapter, you'll have:

- Opened this repo in a GitHub Codespace (or a local VS Code Dev Container)
- Verified that Azure CLI, PowerShell, Copilot CLI, Claude Code, and Python/Jupyter are ready
- Signed in to Azure
- Kicked off the end-to-end Medicare deployment

> ⏱️ **Estimated Time:** ~10 minutes (5 min for the container to build + 5 min hands-on)

## ✅ Prerequisites

- **GitHub account** with Codespaces access. [See plans](https://github.com/features/codespaces).
  Every personal account gets a free monthly quota of Codespaces core-hours.
- **An Azure subscription** with permission to create Resource Groups and
  [Fabric capacities](https://learn.microsoft.com/fabric/enterprise/licenses)
  (F4 or higher), **or** Contributor access to an existing Fabric workspace on an F4+ capacity.
- **Medicare Part D zip file(s)** to load — [Download data](https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug/data).
  See [Getting the Medicare Data into the Codespace](#-getting-the-medicare-data-into-the-codespace) below — the zips are **not** in the repo (they're gitignored), so you fetch them into the container.

## 📥 Getting the Medicare Data into the Codespace

The zip files are large and **not committed to the repo** (`*.zip` is gitignored), so they won't
be present in a fresh Codespace. Get them into `data/DemoZippedFiles/` using whichever path fits.

> **⚠️ Names matter.** The deploy scripts auto-detect years from the zip name
> **`Medicare_Part_D_Prescribers_by_Provider_and_Drug_YYYY.zip`**, and the load notebook reads the
> zip's **inner** file expecting a single CSV named
> **`Medicare_Part_D_Prescribers_by_Provider_and_Drug_YYYY.csv`**. Both must match exactly.

### Option A — Download straight into the Codespace (recommended)

Keeps the transfer cloud-to-cloud (CMS → Codespace), so it doesn't go through your home connection.

> **⚠️ You can't `curl` the CMS "Download" button** — that page runs JavaScript, so `curl` only
> saves the ~3 KB HTML web page (if a "zip" is ~3 KB, that's what happened). Instead download the
> underlying **CSV** (a static, curl-able file) and zip it with the correct inner name.

1. In the Codespace terminal, download each year's CSV and repackage it. The CSV URLs below are
   verified working (each is 3.7–4.1 GB):

   ```bash
   cd data/DemoZippedFiles

   declare -A CSV=(
     [2024]="https://data.cms.gov/sites/default/files/2026-05/0ae165f4-eb44-495d-8cac-67f4571b6b83/MUP_DPR_RY26_P04_V10_DY24_NPIBN.csv"
     [2023]="https://data.cms.gov/sites/default/files/2025-04/0d5915ce-002c-4d87-bde8-24ffb08bb6cc/MUP_DPR_RY25_P04_V10_DY23_NPIBN.csv"
     [2022]="https://data.cms.gov/sites/default/files/2024-05/18f82097-61a6-4889-9941-9a0b6ad7523c/MUP_DPR_RY24_P04_V10_DY22_NPIBN.csv"
   )

   for Y in 2024 2023 2022; do
     BASE="Medicare_Part_D_Prescribers_by_Provider_and_Drug_${Y}"
     curl -L -o "${BASE}.csv" "${CSV[$Y]}"
     python3 -c "import zipfile,sys; z=zipfile.ZipFile(sys.argv[2],'w',zipfile.ZIP_DEFLATED); z.write(sys.argv[1], arcname=sys.argv[1]); z.close()" "${BASE}.csv" "${BASE}.zip"
     rm "${BASE}.csv"    # delete CSV to save disk; keep only the zip
   done
   cd -
   ```

   Zipping a ~4 GB CSV in Python takes a few minutes each; deleting each CSV after zipping keeps
   peak disk under ~5 GB (fits a default 32 GB Codespace). For a quick demo, keep just one or two
   years in the loop.

   > **If a CSV URL 404s** (CMS republishes periodically), open the
   > [CMS data page](https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug),
   > open **DevTools → Network**, click that year's **Download**, and copy the request URL ending in
   > `.csv` (a `data.cms.gov/sites/default/files/…` path). Substitute it into the map above.

2. Verify (each should be hundreds of MB, not 3 KB):

   ```bash
   ls -lh data/DemoZippedFiles/*.zip
   ```

> 💡 **Or let an AI agent do it:** paste the copied CMS URL to `copilot` or `claude` and ask it to
> download the file into `data/DemoZippedFiles/` with the correct `..._YYYY.zip` name.

### Option B — Upload zips you already downloaded

If you grabbed the zips on your laptop, get them into the Codespace one of these ways:

- **Drag and drop** the files onto the `data/DemoZippedFiles/` folder in the VS Code file explorer
  (simplest for 1–3 files; a full ~8 GB set is slow over browser upload).
- **GitHub CLI** from your *local* terminal (no browser upload):

  ```bash
  gh codespace cp -e "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2023.zip" \
    "remote:/workspaces/skills-for-fabric-load-medicare-data/data/DemoZippedFiles/"
  ```

> **Tip:** Only have time for a quick demo? Just fetch one or two years (e.g. 2023 and 2022). The
> scripts auto-detect whatever is present.

## 📦 What's Inside the Dev Container

The environment is defined by [`devcontainer.json`](devcontainer.json). Here's what each part does:

| Setting | Value | Why it's here |
|---|---|---|
| `image` | `mcr.microsoft.com/devcontainers/python:1-3.11-bullseye` | Debian-based image with Python 3.11 — the runtime the bash deploy scripts need. |
| `features` → `azure-cli` | latest | Provides `az` for creating the Resource Group, Fabric capacity, and calling Fabric REST APIs. |
| `features` → `powershell` | latest | Installs PowerShell 7 so the `*.ps1` deployment scripts also run inside the container. |
| `features` → `github-cli` | latest | Provides `gh` for GitHub auth and repo operations. |
| `features` → `node` | latest | Node.js + npm — required to install the GitHub Copilot CLI and Claude Code. |
| `customizations.vscode.extensions` | Python, Jupyter, Azure CLI Tools, PowerShell | Auto-installs the VS Code extensions used to edit notebooks and scripts. |
| `onCreateCommand` | `pip install jupyter pandas` + `npm install -g @github/copilot @anthropic-ai/claude-code` | Runs **once** when the container is created — sets up Jupyter, pandas, the Copilot CLI, and Claude Code. |
| `postStartCommand` | echo banner | Runs on **every start** — prints the next-steps reminder in the terminal. |

## 🚀 Quick Start (GitHub Codespaces — Zero Setup)

1. **Fork or open** this repository on GitHub.
2. Select **Code → Codespaces → Create codespace on main**.
3. Wait a few minutes for the container to build. You'll see the tools install in the
   creation log.
4. When the terminal shows the ready banner, you're set:

   ```text
   ✅ Azure CLI, PowerShell, GitHub CLI, Copilot CLI, Claude Code, and Python/Jupyter are ready.
   Run: az login --use-device-code, then ./deploy-medicare-e2e.sh
   ```

### Local Dev Container (alternative)

Prefer to run the container on your own machine? Install
[Docker Desktop](https://www.docker.com/products/docker-desktop/) and the
[Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
extension for VS Code, then:

1. Clone the repo and open the folder in VS Code.
2. When prompted, choose **Reopen in Container** (or run **Dev Containers: Reopen in
   Container** from the Command Palette).
3. Wait for the build to finish — the same tools are installed as in Codespaces.

## 🔐 Authentication

### Step 1 — Sign in to Azure

The deploy scripts call the Azure CLI, so authenticate first. In a Codespace there's no
local browser, so use the device-code flow:

```bash
az login --use-device-code
```

Follow the prompt: open the URL shown, enter the one-time code, and sign in. Then set
your subscription if you have more than one:

```bash
az account set --subscription "<your-subscription-id>"
```

### Step 2 — (Optional) Sign in to an AI coding agent

Only needed if you want an AI agent to drive the deployment interactively.

**GitHub Copilot CLI:**
```bash
copilot
```

On first launch, run `/login` and complete the device-authorization flow in your browser.

**Claude Code:**
```bash
claude
```

On first launch, follow the prompt to authenticate with your Anthropic account.

## ✅ Verify It Works

Run these quick checks in the Codespace terminal to confirm every tool is ready:

```bash
az --version            # Azure CLI
pwsh --version          # PowerShell 7
gh --version            # GitHub CLI
copilot --version       # GitHub Copilot CLI
claude --version        # Claude Code
python3 --version       # Python 3.11
jupyter --version       # Jupyter
```

Each command should print a version number. If they all do, your environment is ready.

## 🏁 You're Ready — Run the Deployment

With Azure sign-in complete and your zip files in `data/DemoZippedFiles/`, create your config
from the template (`cp config/variables.env.example config/variables.env`) and edit
`config/variables.env` with your names/IDs, then launch a
deployment path:

**Full deployment (new infrastructure):**
```bash
./deploy-medicare-e2e.sh
```

**Existing workspace (Contributor access, set `WS_ID` in variables.env):**
```bash
./deploy-medicare-to-workspace.sh
```

**Or let an AI agent drive it:**
```bash
copilot   # or: claude
# then: Read config/variables.env and follow context/buildfabricworkspace.md and context/LoadMedicareData.md
```

> The PowerShell equivalents (`pwsh ./deploy-medicare-e2e.ps1`) also work inside the
> container — pick whichever you prefer.

> ⚠️ **Cost Warning:** The full deployment creates a **billable** Fabric capacity.
> Pause or delete the capacity when you're done.

## 🛠️ Troubleshooting

### `copilot: command not found`

The npm install may not have completed. Re-run it:

```bash
npm install -g @github/copilot @anthropic-ai/claude-code
```

### `az login` doesn't open a browser

Codespaces has no local browser — always use the device-code flow:

```bash
az login --use-device-code
```

### Changed `devcontainer.json` and the change isn't applied

Rebuild the container: **Command Palette → Dev Containers: Rebuild Container** (or, in
Codespaces, **Codespaces: Rebuild Container**). `onCreateCommand` only runs on a fresh build.

### Script permission denied

Make the script executable:

```bash
chmod +x deploy-medicare-e2e.sh
```

## 🔑 Key Takeaways

1. **Zero local setup** — Codespaces builds a Linux container with every tool the repo needs.
2. **Two script families** — Both bash (`*.sh`) and PowerShell (`*.ps1`) run inside the container.
3. **Device-code auth** — Use `az login --use-device-code` because there's no local browser.
4. **One-time provisioning** — `onCreateCommand` sets up Jupyter, pandas, Copilot CLI, and Claude Code on build; rebuild if you change it.

> 📚 More on dev containers: [Introduction to Dev Containers](https://docs.github.com/codespaces/setting-up-your-project-for-codespaces/adding-a-dev-container-configuration/introduction-to-dev-containers)
