# Runbook — submodule fork preparation

Some Aiven patches re-point a vendored submodule (`contrib/<x>`) from the
upstream repo to an **Aiven fork** that carries an upstream SDK pin plus one or
more Aiven commits on top (e.g. an IPv6 host fix and/or a signature-delegation
change). The fork branch must exist in the required state **before** the patch
that pins it can build, because the C++ in that patch depends on the fork's SDK
change (a virtual method, an exposed member, ...). A worker that naively applies
the `.gitmodules` hunk would pin a ref that is missing or stale.

This runbook is the durable, version-independent recipe for finding and
preparing those forks. The discovery query and prep recipe are reusable every
uplift; the registry table at the bottom is the per-uplift state.

## Hard invariant — the agent never touches the fork repos

The agent's role is **read-only discovery inside the ClickHouse checkout**, plus
**emitting commands for the human to run**. The agent MUST NOT clone, fetch,
branch, cherry-pick, or push in any Aiven fork repository (`aiven/aws-sdk-cpp`,
`aiven/azure-sdk-for-cpp`, `aiven/mariadb-connector-c`, ...). Fork preparation
happens outside this checkout, by the human. The agent:

1. finds the **upstream base hash for the new LTS version** (read-only),
2. identifies the **Aiven commits** that must be cherry-picked on top,
3. **prints the commands** the human runs in the fork clone,
4. waits for the human to report back the **resulting fork-branch HEAD SHA**,
   which then becomes the `contrib/<x>` gitlink target.

## Step 1 — Discover the full submodule-coupled patch set (read-only)

Run once per uplift, against the source Aiven branch, to enumerate every patch
that touches `.gitmodules` or a `contrib/` submodule pointer:

```bash
for sha in $(git rev-list <upstream-tag>..<aiven-branch>); do
  files=$(git show --name-only --format= "$sha")
  if echo "$files" | rg -q '^\.gitmodules$|^contrib/'; then
    echo "--- $(git show --no-patch --format='%h %s' "$sha")"
    echo "$files" | rg '^\.gitmodules$|^contrib/' | sed 's/^/      /'
  fi
done
```

Example (25.8 → 26.3): `git rev-list v25.8.18.1-lts..origin/v25.8.18.1-lts-aiven`.

Distinguish two categories in the output:

- **Fork redirects** — a `.gitmodules` `url =` change to the Aiven org and/or a
  `contrib/<x>` gitlink bump. These need fork preparation (this runbook).
- **In-repo `contrib/*-cmake` edits** — e.g. `contrib/curl-cmake/CMakeLists.txt`
  ("Enable curl ipv6"). These edit the in-repo CMake wrapper, not a submodule
  fork; they are a never-touch-path edit, handled separately, NOT here.

Note that a single fork often accumulates **two stacked Aiven commits** (e.g. an
IPv6 host fix and a delegation change), surfacing as two inventory rows that
touch the same `contrib/<x>`.

## Step 2 — Find the upstream base hash for the new version (read-only)

For each fork submodule, the base the fork branch is built from is the commit
the **new** LTS upstream tag pins:

```bash
git ls-tree <new-lts-tag> contrib/<submodule-path>
# e.g. git ls-tree v26.3.10.62-lts contrib/mariadb-connector-c
#      -> 160000 commit d0a788c5b9fcaca2368d9233770d3ca91ea79f88  contrib/mariadb-connector-c
```

The 40-char commit is the **upstream base hash**. Compare it to the base the
source-branch patch was built against (the `-Subproject commit ...` line in the
patch's `contrib/<x>` hunk):

- **Bases match** → the human can cherry-pick the Aiven commits straight onto
  the same base (no rebase). (26.3 case: `contrib/azure`, `contrib/mariadb-connector-c`.)
- **Bases differ** → the Aiven commits must be re-based onto the new base in the
  fork. (26.3 case: `contrib/aws`.)

## Step 3 — Identify the Aiven commits to cherry-pick (read-only)

```bash
git log --oneline <upstream-tag>..<aiven-branch> -- contrib/<submodule-path>
```

Each commit listed is an Aiven change to that submodule pin; the human
cherry-picks the corresponding fork-side commits (in order) onto the new base.

## Step 4 — Emit the human prep commands (agent prints; human runs)

The agent prints, and the human runs **in the Aiven fork clone** (never the
ClickHouse checkout):

```bash
# in the Aiven fork repo (e.g. ~/projects/<fork>)
git fetch <official-upstream-remote>                       # to have the base commit
git checkout -b aiven/clickhouse-<new-version> <upstream-base-hash>
git cherry-pick <aiven-fork-commit-1> [<aiven-fork-commit-2> ...]
git push origin aiven/clickhouse-<new-version>
git rev-parse HEAD     # report this SHA back -> contrib/<x> gitlink target
```

The human reports back the resulting **HEAD SHA**. Only then can the patch be
dispatched with the gitlink + `.gitmodules` `branch =` baked in (see clause (vi)
in `docs/aiven/skills/dispatch-prompt-template.md`; the prepared fork overrides
the default `external_dependency` escalation).

## Best practice — batch the fork prep before the first dispatch

Run Steps 1–3 at uplift bootstrap and prepare **all** fork branches up front, so
no submodule-coupled patch escalates `external_dependency` mid-dispatch. Record
the results in the per-uplift registry below.

## Registry — 26.3 uplift

| submodule | Aiven fork | stacked Aiven commits (inventory NNN) | upstream base (26.3 pin) | fork branch (`aiven/clickhouse-v26.3.10.62`) HEAD | status |
|---|---|---|---|---|---|
| `contrib/aws` | `aiven/aws-sdk-cpp` | IPv6 S3 host fix + S3 signature delegation (015) | `22f694afbdc7e9766894998c3745e23f004f8b86` | `c930cb8e8c51d4010dca68e01edf73ae1bb15af0` | done |
| `contrib/azure` | `aiven/azure-sdk-for-cpp` | IPv6 Azure host fix + Azure signature delegation (016) | `0f7a2013f7d79058047fc4bd35e94d20578c0d2b` | `98519bd324221c1b2e3c7576317a6a09fa1825ea` | done |
| `contrib/mariadb-connector-c` | `aiven/mariadb-connector-c` | `X509_check_host` hostname-length SSL/TLS fix (021) | `d0a788c5b9fcaca2368d9233770d3ca91ea79f88` | `2914d3fbce4f82b0f0d66034eb7afd1dd3dc5c70` | done |

Not a fork redirect (no prep): `contrib/curl-cmake/CMakeLists.txt` ("Enable curl
ipv6") is an in-repo CMake-wrapper edit, handled as a never-touch-path patch.

## Pointers

- `docs/aiven/skills/dispatch-prompt-template.md` — clause (vi) (submodule
  fork-redirect preflight + `external_dependency` escalation).
- `docs/aiven/AGENTS.md` §3 — the `.gitmodules` editability invariant.
- `docs/aiven/uplifts/26.3/inventory.md` — patch NNN ↔ source SHA mapping.
