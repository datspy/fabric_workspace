# Fabric Data Engineering — VS Code Dev Container

A ready-to-use dev container for developing Microsoft Fabric notebooks and Spark job definitions locally in VS Code, with execution on Fabric remote Spark compute.

## What's included

| Layer | Details |
|---|---|
| **Base image** | `mcr.microsoft.com/msfabric/synapsevscode/fabric-synapse-vscode:latest` — JDK, Conda, Jupyter, and the Fabric runtime pre-installed |
| **VS Code extensions** | Fabric Data Engineering, Python, Jupyter, GitLens, GitHub Pull Requests, Git Graph, Black formatter, Prettier, YAML |
| **Ports** | 4040 / 4041 forwarded for the Spark UI |

## Prerequisites

1. **Docker Desktop** — running and healthy.  
   On Windows + WSL2, allocate at least **8 GB RAM** via WSL Settings (Spark is memory-hungry).
2. **VS Code** with the **Dev Containers** extension (`ms-vscode-remote.remote-containers`).
3. **A Microsoft Fabric workspace** you have access to.
4. **SSH key loaded** in your local SSH agent for GitHub:
   ```bash
   ssh-add -l          # should show your key
   ssh-add ~/.ssh/id_ed25519   # if not loaded
   ```

## Quick start

```bash
# 1. Clone your Fabric-connected GitHub repo (or any new repo)
git clone git@github.com:<your-org>/<your-repo>.git
cd <your-repo>

# 2. Copy the .devcontainer folder into the repo root
#    (if you haven't committed it yet)
cp -r /path/to/.devcontainer .

# 3. Open in VS Code
code .
```

VS Code will detect the dev container config and show a prompt:  
**"Reopen in Container"** → click it. First build takes a few minutes.

## After the container starts

1. **Sign in to Fabric**  
   `Ctrl+Shift+P` → `Fabric Data Engineering: Sign In`

2. **Select your workspace**  
   `Ctrl+Shift+P` → `Fabric Data Engineering: Select Workspace`

3. **Set a local work folder**  
   `Ctrl+Shift+P` → `Fabric Data Engineering: Set Local Work Folder`  
   Point this at a subfolder inside `/code` (e.g. `/code/notebooks`).

4. **Develop & run**  
   - Create or open notebooks — IntelliSense works locally.
   - Run cells on **Fabric remote Spark compute** via the extension.
   - Browse lakehouse tables and files from the sidebar.

## Git workflow

Since the Fabric workspace is already connected to this GitHub repo:

```
 Edit locally in dev container
        │
        ▼
  git add / commit / push   ← standard Git inside the container
        │
        ▼
  GitHub repo (source of truth)
        │
        ▼
  Fabric portal → Source Control → "Update all"
        │
        ▼
  Fabric workspace synced
```

> **Tip:** Avoid editing the same notebook in both the Fabric portal and
> locally at the same time — this causes merge conflicts in the item
> definition JSON files.

## Customisation

- **Add Python packages:** Edit `postCreateCommand` in `devcontainer.json`, e.g.  
  `"postCreateCommand": "conda install -y pandas delta-spark && git config ..."`
- **Add VS Code extensions:** Append extension IDs to the `extensions` array.
- **Adjust Docker resources:** Edit `docker-compose.yaml` to set memory/CPU limits, or adjust via Docker Desktop / WSL settings.

## Stopping the container

Click the green **><** icon in the bottom-left of VS Code → **Reopen Locally**.  
This stops the container and returns you to your local machine.

## References

- [Fabric Data Engineering VS Code extension — Docker support](https://learn.microsoft.com/en-us/fabric/data-engineering/set-up-vs-code-extension-with-docker-image)
- [Official sample repo (microsoft/SynapseVSCode)](https://github.com/microsoft/SynapseVSCode/tree/main/samples/.devcontainer)
- [VS Code Dev Containers documentation](https://code.visualstudio.com/docs/devcontainers/containers)
