# Patch 070 — disable-ytsaurus-engine

## 0. Lineage

| LTS uplift | First-carry handle on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `d297c19ff8331ecd7ae3e75be2cdc39d3c010796` | Aliaksei Khatskevich (author) / Joe Lynch (committer), 2026-03-16 | original carry (the version we port FROM) |
| 26.3-aiven | `patch-port(070)` (staged) | T3 patch worker (T3.24), 2026-06-12 | conflict-resolved (hunk-1 dropped, already satisfied by 045) — see §2 and §6 |

The current uplift's row uses the stable handle `patch-port(070)` (NOT a SHA);
it stays "(staged)" until the human commits. Find it later with
`git log --grep '^patch-port(070)'`.

## 1. Purpose

Make the YTsaurus **table engine** registration compile-conditional on the
existing `USE_YTSAURUS` build flag, so a build configured with
`-DUSE_YTSAURUS=0` links cleanly. On 26.3 HEAD the forward-declaration of
`registerStorageYTsaurus` is already wrapped in `#if USE_YTSAURUS` (lines
~74-76 of `src/Storages/registerStorages.cpp`), but the **call** at line ~197
was UNGUARDED — so a `USE_YTSAURUS=0` build references an undeclared symbol and
fails to compile. This patch wraps the call in `#if USE_YTSAURUS … #endif`,
closing that dangling reference. The motivation is durable: Aiven wants to be
able to disable the YTsaurus engine "before we are completely sure it is safe
and fully integrated" (source commit body).

This is the **third link of the `REGISTER_*` engine/function family**
(045 → 051 → 052 → **070** → 071 → 075). 070 is the YTsaurus build-correctness
step; the next patch (071) layers the `REGISTER_YTSAURUS_*` flags on top and
adds the `USE_YTSAURUS && ` prefixes.

Source SHA on `v25.8.18.1-lts-aiven`: `d297c19ff8331ecd7ae3e75be2cdc39d3c010796`
(from `docs/aiven/uplifts/26.3/inventory.md` row 070).
Original author: `alex.khatskevich@aiven.io` (committer `joelynch112@gmail.com`).
Original purpose (verbatim from the source commit body):

> Disable YTsaurus engine
>
> Disable it before we are completely sure it is safe and fully
> integrated.

## 2. Upstream-drift findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`, not just textually applicable.

### Commands run

```bash
# Hunk-1 target: is the dictionary source already REGISTER-gated by 045?
grep -nB1 -A1 "registerDictionarySourceYTsaurus(source_factory)" \
  src/Dictionaries/registerDictionaries.cpp   # -> #if REGISTER_DICTIONARY_SOURCE_YTSAURUS (045)

# Hunk-2 target: is the storage call still unguarded on HEAD?
sed -n '193,199p' src/Storages/registerStorages.cpp   # -> call present with NO #if directly above

# Forward-declaration already guarded (proves the build-correctness rationale):
grep -nB1 -A1 "void registerStorageYTsaurus" \
  src/Storages/registerStorages.cpp   # -> #if USE_YTSAURUS / decl / #endif
```

### Findings

- **Source Hunk 1 — `src/Dictionaries/registerDictionaries.cpp`** (the source
  wraps `registerDictionarySourceYTsaurus` in `#if USE_YTSAURUS`): on 26.3 HEAD
  this call is ALREADY guarded `#if REGISTER_DICTIONARY_SOURCE_YTSAURUS`
  (lines 89-91, `tmp/patch-070/head-dict-guard.log`), added by committed
  `patch-port(045)`. The hunk is `already-applied` on the REGISTER dimension; the
  `USE_YTSAURUS && ` prefix the source wants is added by the NEXT patch (071),
  not here. **Hunk 1 is DROPPED** — `registerDictionaries.cpp` is left exactly as
  HEAD has it (045's gate must not regress).
- **Source Hunk 2 — `src/Storages/registerStorages.cpp`**: on HEAD the
  `registerStorageYTsaurus(factory)` call is UNGUARDED (line ~197,
  `tmp/patch-070/head-storage-callsite.log`) while its forward-declaration
  already carries `#if USE_YTSAURUS` (lines 74-76,
  `tmp/patch-070/head-storage-fwddecl.log`). This is a real, load-bearing fix:
  without the guard a `-DUSE_YTSAURUS=0` build references an undeclared function.
  **Hunk 2 is APPLIED** — it applied cleanly during the cherry-pick.
- Conclusion: **`still-needed-but-rewrite`** (partial port). Hunk 1
  `already-applied` (045 REGISTER-gate); Hunk 2 `still-needed` (build-correctness
  for `USE_YTSAURUS=0`). Net effect on HEAD: a single `#if USE_YTSAURUS` wrap
  around `registerStorageYTsaurus(factory)`. Exactly one source file changes;
  `registerDictionaries.cpp` is untouched.

### Required deviation from the literal 25.8 patch (intentional)

1. **Drop the `registerDictionaries.cpp` hunk.** The source's first hunk is
   satisfied differently on our lineage: the dictionary source is already gated
   by `REGISTER_DICTIONARY_SOURCE_YTSAURUS` via committed `patch-port(045)`.
   Re-applying the source's `#if USE_YTSAURUS` here would either conflict with or
   regress 045's REGISTER-gate; the `USE_YTSAURUS && REGISTER_DICTIONARY_SOURCE_YTSAURUS`
   two-level check is the job of `patch-port(071)`. This is the reason
   `byte_equivalent: false`.

## 3. C++ review

Apply `docs/aiven/skills/cpp-review-checklist.md`. The change is a
preprocessor-guard only; no runtime data path is altered when `USE_YTSAURUS=1`
(the default).

- 1 Lifetime + ownership: `n/a` — no object lifetime touched; the guard only
  elides the `registerStorageYTsaurus(factory)` call when `USE_YTSAURUS=0`.
- 2 Exception safety: `n/a` — no new throwing code; eliding a `registerX` call
  cannot break exception safety of the surrounding scope.
- 3 Thread-safety + concurrency: `n/a` — registration runs once at startup; no
  concurrency semantics changed.
- 4 Performance + memory: ✓ — strictly reductive at `USE_YTSAURUS=0` (engine not
  registered); default build (`USE_YTSAURUS=1`) is behavior-identical to upstream.
- 5 Settings as public API: ✓ — no server *setting* added; `USE_YTSAURUS` is a
  pre-existing compile-time CMake flag (defaults ON). Default build ⇒ no
  public-API surface change.
- 6 Error handling: ✓ — when the engine is compiled out, using it fails via the
  existing upstream "unknown table engine" path; no new error code needed.
- 7 Upstream / vendored code: ✓ — the edited file is ClickHouse-owned
  (`src/Storages/registerStorages.cpp`); none under `contrib/**`. The guard
  matches the pre-existing `#if USE_YTSAURUS` forward-declaration guard, closing
  the dangling-reference build break for `USE_YTSAURUS=0`.
- 8 Behavior under settings: ✓ — with `USE_YTSAURUS=1` (default) the engine is
  still registered (verified: `system.table_engines` smoke = 1); with
  `USE_YTSAURUS=0` the engine is absent and the build links (mechanically certain
  — the forward-decl is already guarded; this dispatch does not separately build
  that variant).

## 4. Test design

(b) **Documented justification — no new test, with reference to an existing
upstream test (`tests.added: no_justified`), mirroring committed siblings
051/052/045.**

This patch is **build-system-only** (a single `#if USE_YTSAURUS` wrap) and adds
**no runtime behavior on a default build**: `USE_YTSAURUS` defaults **ON** (proof:
the call currently compiles, so the symbol is available ⇒ `USE_YTSAURUS=1`), so
the new guard is **true on a default build → zero default-behavior change**. The
behavior the patch *introduces* — eliding the YTsaurus engine registration — is a
**compile-time** property of the produced binary, gated by a CMake flag, not a
runtime setting. A stateless `.sql`/`.sh` test cannot observe it on the default
build (the engine is present), and producing a "disabled" binary requires a
second CMake configure (`-DUSE_YTSAURUS=0`) + rebuild that the stateless runner
cannot express. A worktree-flip evidence pair is also unreachable: pre- and
post-patch both register YTsaurus on a default build (pass-pass), so there is no
differential observable. Per AGENTS §7(b) the correct posture is `no_justified`
with build-system verification + an existing upstream test that exercises the
gated surface.

The `USE_YTSAURUS=0`-compiles claim is documented and mechanically certain (the
forward-declaration is already guarded by `#if USE_YTSAURUS`, so guarding the
call removes the only remaining dangling reference); it is recorded here rather
than separately built (same evidentiary style as committed 039/041).

### Build-system verification (the correctness evidence)

- **Guard present and well-formed** (`tmp/patch-070/guard-present.log`): the
  post-patch source has `#if USE_YTSAURUS` / `registerStorageYTsaurus(factory);`
  / `#endif` at lines 197-199.
- **Default-build smoke = 1** (`tmp/patch-070/smoke.log`): on the post-patch
  server, `SELECT count() FROM system.table_engines WHERE name='YTsaurus'` = **1**
  — the engine is still registered on the default `USE_YTSAURUS=1` build,
  confirming zero default-behavior change.
- **Incremental build green** (`tmp/patch-070/build-postpatch.log`): touching
  only `registerStorages.cpp` triggered a 20-step incremental rebuild (recompiled
  `registerStorages.cpp.o` + relink), ninja exit 0, no CMake reconfigure.

### Existing upstream test exercising the gated surface (run post-patch, PASS)

Run against the staged/post-patch binary (`tmp/patch-070/upstream-test.log`):

- `tests/queries/0_stateless/01645_system_table_engines.sql` — selects from
  `system.table_engines`, the exact registration catalog that
  `registerStorageYTsaurus(factory)` (and every guarded engine) feeds. Result:
  `01645_system_table_engines: [ OK ] 0.08 sec`. There is no in-tree YTsaurus
  functional test (the engine requires an external YTsaurus cluster, so no
  default-runnable test can exercise it); this test verifies the registration
  catalog is intact post-patch, and the §4 smoke (`count()=1` for `YTsaurus`)
  confirms YTsaurus specifically still registers on the default `USE_YTSAURUS=1`
  build.
- Why this distinguishes the contract: with `USE_YTSAURUS=1` (default) the engine
  is still registered and the catalog query behaves exactly as upstream, which is
  the default-build invariant this patch must preserve.

## 5. Rollback considerations

- Revert safety: ✓ — pure compile-time guard. Reverting removes the `#if`/`#endif`
  around the call; no schema migration, no on-disk format change, no ZK state.
- State surviving restart: none.
- Disabling the new behavior without rebuilding: `n/a` — the toggle is the
  compile-time `USE_YTSAURUS`. To re-enable the engine that was compiled out,
  rebuild with `-DUSE_YTSAURUS=1` (the default).

## 6. Per-uplift notes

### 25.8-aiven (historical)

Original carry. Author: Aliaksei Khatskevich; committer: Joe Lynch
(`d297c19ff8`). The 25.8 commit had two hunks: `registerDictionaries.cpp`
(wrapping `registerDictionarySourceYTsaurus` in `#if USE_YTSAURUS`) and
`registerStorages.cpp` (wrapping `registerStorageYTsaurus(factory)` in
`#if USE_YTSAURUS`). No 25.8 stateless test (build-correctness change).

### 26.3-aiven (this uplift)

- Cherry-pick was: **conflict-resolved (partial port)**.
  - `src/Storages/registerStorages.cpp` (Hunk 2) applied cleanly — the single
    `#if USE_YTSAURUS … #endif` wrap around `registerStorageYTsaurus(factory)`.
  - `src/Dictionaries/registerDictionaries.cpp` (Hunk 1) CONFLICTED: HEAD has
    `#if REGISTER_DICTIONARY_SOURCE_YTSAURUS` (from committed `patch-port(045)`),
    the source expected the call unguarded and wanted `#if USE_YTSAURUS`.
    Resolved by **keeping HEAD verbatim** (`git checkout HEAD --
    src/Dictionaries/registerDictionaries.cpp`), so the staged dict diff is
    EMPTY and 045's REGISTER-gate is NOT regressed. The `USE_YTSAURUS && ` prefix
    that the source wants on this call is the job of the next patch, 071.
- 26.3 adaptation: the source's `registerDictionaries.cpp` hunk is intentionally
  dropped (already satisfied by 045). `byte_equivalent: false`. Only the
  `registerStorages.cpp` guard is applied. This patch adds **no** `REGISTER_*`
  flag, no `config.h.in` change, and no `configure_config.cmake`/`option()` line
  (070 introduces no new flag — 071 will add the `REGISTER_YTSAURUS_*` flags and
  the `USE_YTSAURUS && ` prefixes).
- Upstream-drift conclusion: `still-needed-but-rewrite` (§2) — partial port.
- Test: no new stateless test — `no_justified`, build-correctness-only,
  default `USE_YTSAURUS=1` ⇒ no default-behavior change. Build-system
  verification: guard present (`tmp/patch-070/guard-present.log`), incremental
  build green (`tmp/patch-070/build-postpatch.log`, ninja exit 0), default-build
  smoke = 1 (`tmp/patch-070/smoke.log`).
- Time-to-port: build was **warm-cache** incremental — touching only
  `registerStorages.cpp`, no CMake reconfigure (no `config.h.in`/cmake edit), a
  20-step rebuild. Wall: ~48s (`tmp/patch-070/build-start.txt` 08:21:28Z →
  `tmp/patch-070/build-end.txt` 08:22:16Z, ninja exit 0).
- Anything surprising: the highest-risk step was conflict resolution on
  `registerDictionaries.cpp` — accidentally rewriting it would regress 045. The
  empty staged-dict diff check (`tmp/patch-070/staged-dict.diff`) makes that
  impossible to miss.
