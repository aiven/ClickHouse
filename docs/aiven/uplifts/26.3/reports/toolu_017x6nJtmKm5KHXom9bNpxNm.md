<!-- subagentStop archive | source=transcript | status=completed | type=general-purpose | ts=2026-06-12T09:14:41Z -->

Step 8 — Halt-and-escalate report.

---
```
outcome: success
patch_slug: register-ytsaurus-directives
source_sha: 1b350e6b6522ba1f92ed938b0339ce1d5071994c
proposed_commit:
  staged_files:
    - src/Common/config.h.in
    - src/Dictionaries/registerDictionaries.cpp
    - src/Storages/registerStorages.cpp
    - src/TableFunctions/registerTableFunctions.cpp
    - src/configure_config.cmake
    - docs/aiven/patches/071-register-ytsaurus-directives.md
  commit_message: |
    patch-port(071): Add REGISTER_YTSAURUS directives

    Original author: Aliaksei Khatskevich <alex.khatskevich@aiven.io>, 2026-03-18.

    (cherry picked from commit 1b350e6b6522ba1f92ed938b0339ce1d5071994c)

    26.3 adaptation: added the matching option(REGISTER_YTSAURUS_* ... ON) lines to
    src/configure_config.cmake (absent from the 25.8 source). The dictionary-source
    flag REGISTER_DICTIONARY_SOURCE_YTSAURUS and its option already shipped in
    patch-port(045), so the duplicate config.h.in line is dropped and the
    registerDictionaries.cpp guard is resolved to
    USE_YTSAURUS && REGISTER_DICTIONARY_SOURCE_YTSAURUS. All flags default ON,
    mirroring patch-port(045)/(051)/(052) — no default-behavior change.
    byte_equivalent: false.
  byte_equivalent: false
tests:
  added: no_justified
  kind: upstream-existing
  paths: []
  upstream_reference:
    - tests/queries/0_stateless/01645_system_table_engines.sql
  pre_patch_fail_verified: false
  post_patch_pass_verified: true
  justification: |
    Build-system-only: two #cmakedefine01 flags, two option() lines, three
    preprocessor-guard upgrades. USE_YTSAURUS defaults ON and both
    REGISTER_YTSAURUS_* options default ON, so each two-level guard
    USE_YTSAURUS && REGISTER_YTSAURUS_* is true on a default build → zero
    default-behavior change. The introduced behavior (eliding a surface) is a
    compile-time property gated by CMake flags, not a runtime setting, so a
    stateless test cannot observe it on the default build and a worktree-flip
    yields pass-pass (no differential). Mirrors committed 045/051/052/070.
    Verified post-patch against the staged binary: system.table_engines smoke
    = 1 and the registration catalog (01645_system_table_engines) is intact.
escalation_reason: none
```

## Tier results

- Tier 1 (textual cherry-pick): **pass** — config.h.in auto-merged cleanly (Divergence A, no dup); one expected conflict in `registerDictionaries.cpp` (Divergence B) hand-resolved; the two storage/function hunks applied clean.
- Tier 2 (semantic patch-id): **pass** — patch-ids differ (`byte_equivalent: false`), but decomposition shows the divergence is ONLY the three expected adaptations; no other semantic C++ divergence.
- Tier 3 (build + verify): **pass** — reconfigure + 3281-step rebuild, ninja exit 0; generated `config.h` 3×1; smoke 1/1.

## Evidence

**Cherry-pick (`cherrypick.log`)** — only the expected file conflicted:
```
Auto-merging src/Common/config.h.in
Auto-merging src/Dictionaries/registerDictionaries.cpp
CONFLICT (content): Merge conflict in src/Dictionaries/registerDictionaries.cpp
Auto-merging src/Storages/registerStorages.cpp
Auto-merging src/TableFunctions/registerTableFunctions.cpp
```

**Divergence A — `config.h.in` deduped (no duplicate), two new flags added.** `grep "YTSAURUS"` on the resolved file plus the staged diff:
```
105:#cmakedefine01 REGISTER_DICTIONARY_SOURCE_YTSAURUS   <- single copy (045)
109:#cmakedefine01 REGISTER_YTSAURUS_TABLE_ENGINE         <- new (after AZURE_BLOB_TABLE_ENGINE)
129:#cmakedefine01 REGISTER_YTSAURUS_FUNCTION             <- new (after AZURE_BLOB_FUNCTION)
```
Git's 3-way merge recognized the 045 dict-source line as common context, so `config.h.in` merged without a textual conflict — only `registerDictionaries.cpp` needed hand-resolution.

**Divergence B — resolved dict guard** (`registerDictionaries.cpp`), `REGISTER_DICTIONARY_SOURCE_YTSAURUS` term preserved:
```
89-#if USE_YTSAURUS && REGISTER_DICTIONARY_SOURCE_YTSAURUS
90:        registerDictionarySourceYTsaurus(source_factory);
91-#endif
```
The two clean hunks:
```
-#if USE_YTSAURUS
+#if USE_YTSAURUS && REGISTER_YTSAURUS_TABLE_ENGINE      (registerStorages.cpp)
-#if USE_YTSAURUS
+#if USE_YTSAURUS && REGISTER_YTSAURUS_FUNCTION          (registerTableFunctions.cpp)
```

**Flag↔option coverage diff (`flag-option-coverage.log`)** — EMPTY; both sides equal exactly the YTsaurus pair:
```
(diff output empty; exit 0)
config.h.in side : REGISTER_YTSAURUS_FUNCTION / REGISTER_YTSAURUS_TABLE_ENGINE
cmake side       : REGISTER_YTSAURUS_FUNCTION / REGISTER_YTSAURUS_TABLE_ENGINE
```
`REGISTER_DICTIONARY_SOURCE_YTSAURUS` correctly appears on neither side (not re-added).

**Decomposition (`decomposition-proper.log`)** — divergence is only the expected three things, nothing else:
```
< +#cmakedefine01 REGISTER_DICTIONARY_SOURCE_YTSAURUS    (b) dropped dup dict-source flag
< -#if USE_YTSAURUS                                       (c) source removed pre-image
> -#if REGISTER_DICTIONARY_SOURCE_YTSAURUS                (c) ours removed HEAD's 045 pre-image (post-image identical)
> +option(REGISTER_YTSAURUS_TABLE_ENGINE "..." ON)        (a) added option
> +option(REGISTER_YTSAURUS_FUNCTION "..." ON)            (a) added option
```
(The dispatch's literal Step 3 command greps after the outer `diff`, which prefixes lines with `<`/`>` and so captures nothing; I additionally ran the schema runbook's correct grep-then-diff form above to genuinely verify the divergence.)

**Build (`build-postpatch.log`)** — reconfigure ran, broad rebuild green:
```
[0/2] Re-checking globbed directories...
[1/2] Re-running CMake...
-- Configuring done (7.7s)
...
[3278/3281] Linking CXX executable programs/clickhouse
ninja exit: 0
```

**Generated config.h (`configh-generated.log`)** — all three flags = 1:
```
REGISTER_YTSAURUS_TABLE_ENGINE = 1
REGISTER_YTSAURUS_FUNCTION = 1
REGISTER_DICTIONARY_SOURCE_YTSAURUS = 1
USE_YTSAURUS = 1
```

**Smoke (`smoke-engine.log` / `smoke-tf.log`)** — surfaces resolve on the default build:
```
SELECT count() FROM system.table_engines   WHERE name='YTsaurus'  -> 1
SELECT count() FROM system.table_functions WHERE name='ytsaurus'  -> 1
(registered names confirmed: engine 'YTsaurus', function 'ytsaurus')
```

**Final staged state (`final-status.log`)** — exactly 6 files, no `tests/queries/`:
```
new file:   docs/aiven/patches/071-register-ytsaurus-directives.md
modified:   src/Common/config.h.in
modified:   src/Dictionaries/registerDictionaries.cpp
modified:   src/Storages/registerStorages.cpp
modified:   src/TableFunctions/registerTableFunctions.cpp
modified:   src/configure_config.cmake
6 files changed, 277 insertions(+), 3 deletions(-)
```

## What I did

- **Step 0–1:** Confirmed HEAD `v26.3.10.62-lts-aiven-dev`, tip `patch-port(070)`, only known carry-overs uncommitted, source SHA is a commit by Aliaksei Khatskevich (2026-03-18). Drift analysis confirmed every expectation (dict-source flag at line 105, dict guard in 045 form, clean hunks `#if USE_YTSAURUS`, dict-source option at line 255). Conclusion: `still-needed-but-rewrite`.
- **Step 2:** `git cherry-pick --no-commit -x 1b350e6b65`. Resolved Divergence B (kept the incoming `#if USE_YTSAURUS && REGISTER_DICTIONARY_SOURCE_YTSAURUS`); Divergence A merged cleanly with a single dict-source line and the two new flags. Staged the four source files.
- **Step 2.5:** Added the two `option(REGISTER_YTSAURUS_* … ON)` lines after the existing dict-source option (PC-1); coverage diff EMPTY.
- **Step 3–5:** Decomposition shows only the three expected divergences. Reconfigure + broad rebuild green (ninja exit 0). Generated `config.h` 3×1; YTsaurus engine/function smoke 1/1; server started and stopped cleanly.
- **Step 6–7:** Wrote and staged the dossier `docs/aiven/patches/071-register-ytsaurus-directives.md`. Final staged set = exactly 6 files, no `tests/queries/`.
- **Did NOT** commit, branch, or touch any never-touch path. Hit the transient `deny-agent-commits.sh` fail-closed hook during server shutdown; retried per the env note and it cleared.

## Proposed next step

Ready for human commit. Suggested:
```
git commit -F tmp/patch-071/commit-message.txt
```
(write the `proposed_commit.commit_message` above to that file first; the body already carries the `Original author:` line and the `(cherry picked from commit …)` trailer, so do NOT use `git commit --author=` or `-c CHERRY_PICK_HEAD`). This is the fourth `REGISTER_*` family link (045→051→052→070→**071**→075); 075 chains onto it next.

---

**Teaching note (per mentor rule):**

- **Today you learned:** A git 3-way cherry-pick won't flag a "both-added" line as a conflict when it can align it as *common context* — Divergence A merged silently, so the duplication guard had to be an independent post-merge check (the coverage diff + the generated `config.h` 3×1), not reliance on a conflict marker.
- **Rule of thumb:** Every new `#cmakedefine01 REGISTER_<X>` needs a matching `option(REGISTER_<X> … ON)`; an undefined `#cmakedefine01` silently resolves to `0` and disables the feature — verify with an *empty* flag↔option coverage diff and the generated `config.h` reading `1`.
- **Next rabbit hole:** patch 075 (the next family link) — what does it add on top of the `USE_YTSAURUS && REGISTER_YTSAURUS_*` two-level guards now in place?
