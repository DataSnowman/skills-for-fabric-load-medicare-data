#!/usr/bin/env bash
# =============================================================================
# azurelinux-wsl-setup.sh
#
# One-time setup for running this repo inside an Azure Linux 4.0 (beta) WSL
# distro — the "WSL container" alternative to Docker/dev containers.
#
# Azure Linux is RPM-based and uses the `tdnf` package manager (not apt).
#
# Usage (inside the Azure Linux WSL distro, from the repo root):
#   chmod +x .devcontainer/azurelinux-wsl-setup.sh
#   ./.devcontainer/azurelinux-wsl-setup.sh
# =============================================================================
set -euo pipefail

echo "=== Updating package metadata ==="
sudo tdnf -y update

echo "=== Installing base tooling + Python + Azure CLI + PowerShell + Node ==="
sudo tdnf -y install \
    ca-certificates \
    curl \
    tar \
    gzip \
    which \
    git \
    sudo \
    bash \
    python3 \
    python3-pip \
    azure-cli \
    powershell \
    nodejs \
    npm

echo "=== Installing Python packages (jupyter, pandas) ==="
python3 -m pip install --upgrade pip
python3 -m pip install jupyter pandas

echo "=== Installing GitHub Copilot CLI + Claude Code (npm globals) ==="
sudo npm install -g @github/copilot @anthropic-ai/claude-code

echo ""
echo "✅ Azure Linux 4.0 environment ready (Azure CLI, PowerShell, Copilot CLI, Claude Code, Python/Jupyter, Node)."
cp -n config/variables.env.example config/variables.env 2>/dev/null && \
    echo "→ Created config/variables.env from the template."
echo "BEFORE deploying:"
echo "  1) az login --use-device-code"
echo "  2) fill in values in config/variables.env (copied from config/variables.env.example)"
echo "  3) add Medicare Part D zip files to data/DemoZippedFiles/ (see the README there)"
echo "THEN run ./deploy-medicare-e2e.sh"
