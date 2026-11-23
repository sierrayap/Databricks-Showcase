# Databricks Showcase

Lab notebooks for the Databricks showcase. Download this whole repo as a ZIP and import it into your Databricks workspace.

## Labs

| Folder | Lab |
|---|---|
| `mapi-config-copilot/` | MAPI Config Copilot (Agent Bricks + Apps + Unity AI Gateway) |
| `mapi-config-copilot/lakebase/` | MAPI part 2: move the app to Lakebase, branching, synced tables (run right after part 1) |
| `greenstone-nba-genie-code/` | Genie Code for ML and AI (Next Best Action) |

## How to get the labs into your workspace

1. On GitHub: **Code → Download ZIP**.
2. Unzip on your laptop.
3. In your Databricks workspace: **Workspace → Import** the unzipped folder (or each lab subfolder) into `/Users/<your-email>/`.
4. Open the lab you are running and start with `00_Lab_Guide`, then `01_...`.

Keep each lab folder intact (`app/`, `agent/`, `genie/`, etc. must stay with the notebooks). Path names do not matter — the notebooks discover their own folder at runtime.

## Notes

- Each participant creates their own schemas and apps under the shared showcase catalog (configured in each lab's `_lab_config` notebook).
- Do not share a single cloned folder among multiple people — each person should import their own copy.
