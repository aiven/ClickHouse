---
description: 'How to handle Aiven patches that re-point a contrib submodule to an Aiven fork'
sidebarTitle: 'Submodule forks'
slug: '/aiven/runbooks/submodule-forks'
title: 'Runbook — submodule forks'
doc_type: 'guide'
---

# Runbook — submodule forks {#runbook-submodule-forks}

Some patches re-point `contrib/<x>` to an Aiven fork (SDK pin + Aiven commits).
The fork must exist **before** the port that bumps the gitlink can build.

## Agent role {#agent-role}

Read-only discovery in this checkout. Print commands for the human. **Never**
clone/push fork repos.

1. Find the upstream submodule SHA required by the new LTS peel.
2. List Aiven commits that must sit on top (from prior line / dossier).
3. Emit: create branch on fork → cherry-pick Aiven commits → give human the
   resulting SHA for `.gitmodules` / gitlink.
4. Escalate `external_dependency` until that SHA exists.

## Registry (fill per uplift) {#registry-fill-per-uplift}

| Submodule | Upstream base (this LTS) | Aiven fork | Required tip | Status |
|---|---|---|---|---|
| `contrib/…` | | `aiven/…` | | pending / ready |
