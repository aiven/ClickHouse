# Runbook — safety rules for hook + git operations

**Scope.** Procedural rules that prevent the agent from breaking the repository, the remote, or any production endpoint while exercising or verifying safety mechanisms. Read once; obey always. Referenced from `docs/aiven/AGENTS.md` via the runbooks navigation entry.

**Origin.** Established 2026-05-21 after an incident in which the agent ran `git push --force origin v26.3.10.62-lts-aiven-dev` from the parent shell as a "demo" of the G2 deny hook. The push succeeded (the hook had a regex bug separate from the question of whether using a real destructive command as a test stimulus is ever acceptable — it is not). Full incident: `tmp/bootstrap/hooks-incident.md`.

## Rule 1 — Never use real destructive commands as test stimuli

**The rule.**
The agent must not invoke any real `git push`, `git push --force`, `git rebase`, `git reset --hard`, `git clean -f`, `git commit`, `git cherry-pick` (without `--no-commit`), or `rm -rf` of a path under the repository, **for the purpose of testing whether a safety mechanism (hook, policy, CI gate) would block it**.

**Why.**
Using a destructive command as a test stimulus implicitly bets that the safety mechanism works. That is the same bet you are trying to verify. If the bet loses, the cost is the destruction the mechanism was supposed to prevent. The verification has the same blast radius as the failure mode. This is the airbag-by-crash anti-pattern: "the airbag works if you survive."

The correct verification has **smaller** blast radius than the failure mode, not equal.

**The two sanctioned verification patterns.**

1. **Static (preferred).** Feed synthetic JSON to the hook script and assert its output.

   ```bash
   echo '{"command":"git push fake-remote-DO-NOT-USE fake-branch"}' \
     | .cursor/hooks/deny-irreversible-git.sh
   # → {"permission":"deny", ...}
   ```

   The hook is a pure function of its input. No git invocation. Zero blast radius. This is exactly what `tmp/bootstrap/smoke-test.sh` does.

2. **Runtime probe (only when static is insufficient — e.g., asking "is Cursor actually invoking the hook?").** Use only `-h` (help) variants of denied subcommands.

   - `git rebase -h` — prints rebase help and exits. Matches G2 deny regex.
   - `git push -h` — prints push help and exits. Matches G1 deny regex.
   - `git commit -h` — prints commit help and exits. Matches G7 deny regex.
   - `git cherry-pick -h` — prints cherry-pick help and exits. Matches G7 deny regex.

   The regex match fires the deny if the hook is active. If the hook is silent, the worst case is git prints help text. **Zero side effects in either branch of the outcome.**

**Forbidden alternatives (named so you don't accidentally try them).**

- ❌ `git push --dry-run --force ...` — `--dry-run` reduces side effect but the command still consults the remote and exists in shell history as a real `push`. Use `git push -h` instead.
- ❌ "Pushing to a test branch" — any push that doesn't exist as a literal `-h` invocation is forbidden. Branches you "made up" can still exist; remotes you "made up" can still have a configured URL; mistakes here are unrecoverable from the agent side.
- ❌ "I'll be careful" — the rule does not have an exception for situations where the agent has high confidence. The whole point is to make the safe path mechanically cheap so confidence isn't on the critical path.

## Rule 2 — Sentinel inputs for synthetic tests

When the test text would otherwise contain a real-looking remote/branch/SHA/path, replace with a sentinel:

| Real-looking | Sentinel | Notes |
|---|---|---|
| `origin` | `fake-remote-DO-NOT-USE` | Not configured in `git remote`; any accidental invocation fails fast. |
| `main`, `master`, etc. | `fake-branch-DO-NOT-USE` | Not in `git branch`; ditto. |
| Any specific tag (e.g., `v26.3.10.62-lts`) | `fake-ref-DO-NOT-USE` | Not in `git tag`; ditto. |
| Real-looking SHA (e.g., `abc1234`) | `0000000000000000000000000000000000000000` | All-zeros SHA is never valid. |
| `/repo`, `/some/path` | `/nonexistent-fake-path-DO-NOT-USE` | Cannot exist by name; `cd` would fail. |

**Why this is belt-and-suspenders.**
Synthetic tests do not execute the strings they contain. Sentinel naming is the second line of defense: in the event of a future refactor that accidentally invokes the test text as a shell command (e.g., a `bash -c "$got"` instead of the current safe pipe), the sentinel guarantees git fails on resolution rather than acting on a real endpoint.

## Rule 3 — Hook installation order

**The rule.**
When installing or modifying Cursor hooks, the order is:

1. Write all hook scripts to `.cursor/hooks/<name>.sh`.
2. `chmod +x` the scripts.
3. Run the static smoke test (Rule 1, pattern 1) and verify all assertions pass.
4. Write or modify `.cursor/hooks.json` last.
5. (If runtime probe needed) trigger a `-h` variant of a denied subcommand and check Cursor's Hooks Settings → Execution Log.

**Never** delete-and-recreate `.cursor/hooks.json` mid-session. If you need to temporarily disable hooks (e.g., to escape a chicken-and-egg state), rename instead:

```bash
mv .cursor/hooks.json .cursor/hooks.json.disabled
# ... do the operation ...
mv .cursor/hooks.json.disabled .cursor/hooks.json
```

The rename preserves the inode and the Cursor file watcher.

**Why.**
Cursor's file watcher on `hooks.json` is most likely inode-bound (typical inotify pattern). Delete-then-recreate orphans the watch on the destroyed inode; the new inode is not picked up, leaving the hook system half-loaded (Settings shows hooks but runtime doesn't invoke them, or invokes a stale snapshot). Was observed first-hand on 2026-05-21; remediation per Cursor docs is a window reload, which is expensive.

## Rule 4 — Smoke test must precede any runtime probe

**The rule.**
Before triggering a runtime probe (Rule 1, pattern 2), the static smoke test (Rule 1, pattern 1) MUST report all assertions passing. If any assertion fails, fix the hook regex first; do not attempt a runtime probe against a known-broken script.

**Why.**
A failing static test means the script has wrong logic for some input. A runtime probe with a broken script has unpredictable verdict, and "Cursor invoked the hook and the script returned allow when it should have denied" is indistinguishable from "Cursor didn't invoke the hook at all". The static test eliminates the script-logic axis before runtime is involved.

## Rule 5 — Regex authoring for shell-command hooks

**The rule.**
When writing a `beforeShellExecution` hook regex that targets a specific subcommand (e.g., `git push`), the regex MUST match the subcommand at the start of any sub-shell, not just at the start of the full command string. Use the chain-aware prefix:

```bash
GIT_AT_START='(^[[:space:]]*|[&;|][[:space:]]*)git[[:space:]]+'
if [[ "$command" =~ ${GIT_AT_START}push ]]; then ...
```

**Why.**
The Shell tool passes the **entire chained command** (e.g., `cd /repo && git push ...`) as a single command string. A regex anchored to `^` only matches commands where `git` is the very first token, missing the common pattern of a directory change followed by the git operation. Was observed first-hand on 2026-05-21; the smoke test must include at least one chained case per denied subcommand to catch this category of regression.

## Quick reference

| Situation | What to do |
|---|---|
| Verify a hook's deny logic | Pipe synthetic JSON to the script (Rule 1, pattern 1) |
| Verify Cursor invokes the hook | `git <subcmd> -h` (Rule 1, pattern 2) |
| Install new hooks | Scripts → chmod → static smoke → `hooks.json` (Rule 3) |
| Disable hooks temporarily | `mv hooks.json hooks.json.disabled` (Rule 3) |
| Author a new shell-command regex | Use `GIT_AT_START` chain-aware prefix (Rule 5) |
| Hook script regex changes | Run static smoke before any runtime probe (Rule 4) |
