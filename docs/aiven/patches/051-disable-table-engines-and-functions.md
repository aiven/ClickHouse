# Patch 051 — disable-table-engines-and-functions

## 0. Lineage

| LTS uplift | First-carry handle on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `4641e51fdc021ba901a46c65f67fff8515db0c00` | Tilman Moeller (author) / Joe Lynch (committer), 2026-01-08 | original carry (the version we port FROM) |
| 26.3-aiven | `patch-port(051)` (staged) | T3 patch worker (T3.22), 2026-06-11 | conflict-resolved + hardened-rewrite — see §2 and §6 |

The current uplift's row uses the stable handle `patch-port(051)` (NOT a SHA);
it stays "(staged)" until the human commits. Find it later with
`git log --grep '^patch-port(051)'`.

## 1. Purpose

Add compile-time toggles (`#cmakedefine01 REGISTER_<NAME>_TABLE_ENGINE` /
`#cmakedefine01 REGISTER_<NAME>_FUNCTION`) plus `#if` guards around each gated
table-engine and table-function registration, so a build can exclude specific
engines/functions at compile time via `-DREGISTER_<NAME>=0`. The motivation is
durable: Aiven's managed fleet wants to **disable risky engines/functions**
(e.g. `Executable`, `URL`, `File`, `remote`) for security/compliance, and to
produce smaller binaries with a reduced dependency/attack surface.

The implementation is a **two-level check**: an engine/function is registered
only if `[USE_<LIB> &&] REGISTER_<NAME>` — the existing dependency guard (e.g.
`USE_MONGODB`) AND the new registration flag. Defaults are **enabled**, so a
build that passes no flags behaves exactly as upstream (fully backward
compatible); the toggles are opt-*out*.

This is the **foundation of the `REGISTER_*` engine/function family**: sibling
of patch 045 (dictionary sources, already committed as `0d6ef43e590`);
precursor of 052 (TimeSeries/ObjectStorage/DataLake), 070→071 (YTsaurus), and
075 (ArrowFlight).

Source SHA on `v25.8.18.1-lts-aiven`: `4641e51fdc021ba901a46c65f67fff8515db0c00`
(from `docs/aiven/uplifts/26.3/inventory.md` row 051).
Original author: `tilman.moeller@aiven.io` (committer `joelynch112@gmail.com`).
Original purpose (verbatim from the source commit body):

> Added support for disabling various table engines and table functions
>
> This commit adds CMake configuration flags to disable specific table engines
> and table functions at compile time, allowing builds with a reduced feature
> set. This enables:
>
> 1. Security: Disable risky engines/functions (e.g., Executable, URL, File, Remote)
> 2. Compliance: Remove features that don't meet regulatory requirements
> 3. Minimal Builds: Create smaller binaries by excluding unused features
> 4. Cloud Deployments: Optimize for cloud environments
>
> The implementation adds two-level checks:
> - Dependency check: Is the library available? (e.g., USE_MONGODB)
> - Registration flag: Should the engine/function be registered? (e.g., REGISTER_MONGODB_TABLE_ENGINE)
>
> Both conditions must be true for an engine/function to be registered. If the
> registration flag is not set in CMake, it defaults to enabled (backward
> compatible).
>
> Co-authored-by: Kevin Michel <kevin.michel@aiven.io>
> Co-authored-by: Joe Lynch <joe.lynch@aiven.io>
> Co-authored-by: Aliaksei Khatskevich <alex.khatskevich@aiven.io>

### Ground truth — where the defaults live (RPM build harness)

As with the dictionary-source sibling (045), the production default selection is
**not** in the ClickHouse repo. Aiven's RPM build harness passes an explicit
`-DREGISTER_<NAME>=0/1` for each flag. The in-tree `option(... ON)` default
(added here, §2/§6) only governs builds (dev/CI) that pass no flags; it is the
safety net that keeps a no-flag build identical to upstream.

## 2. Upstream-drift findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`, not just textually applicable.

### Commands run

```bash
# The whole REGISTER_* table-engine/function mechanism is Aiven's; it must NOT
# exist in the base tag.
git show v26.3.10.62-lts:src/Common/config.h.in \
  | grep -cE "REGISTER_(S3|URL|FILE|LOG)_TABLE_ENGINE"      # -> 0

# Are all wrapped registration entry points still present on HEAD?
for id in registerStorageLog registerStorageFile registerStorageURL \
          registerStorageExecutable registerStorageRedis registerStorageKeeperMap \
          registerStorageMySQL registerStorageMongoDB registerStorageFileLog \
          registerStorageNATS registerStorageIceberg registerStorageS3Queue \
          registerStorageODBC registerStorageJDBC registerStorageAzureQueue \
          registerTableFunctionExecutable registerTableFunctionFile \
          registerTableFunctionURL registerTableFunctionURLCluster \
          registerTableFunctionMongoDB registerTableFunctionRedis \
          registerTableFunctionHive registerTableFunctionODBC \
          registerTableFunctionJDBC registerTableFunctionRemote \
          registerStorageAzure; do
  printf '%s: ' "$id"; git grep -c "$id" -- src/ | awk -F: '{s+=$NF} END{print s+0}'
done    # -> every id >= 3
```

### Findings

- 26.3 HEAD has **no** per-engine/function disable mechanism. The base-flag
  check returns `0` (`tmp/patch-051/base-has-flags.log`). The `REGISTER_*`
  mechanism is **Aiven's own**, introduced by the committed sibling 045
  (`0d6ef43e590`); it is absent from `v26.3.10.62-lts`. The patch is **still
  needed**; it is NOT obsoleted by upstream.
- All wrapped registration entry points still exist on HEAD
  (`tmp/patch-051/identifier-grep.log`: each id ≥ 3). No upstream rename/removal
  of the gated call-sites.
- `src/Common/config.h.in` drifted: upstream 26.3 **removed** the trailing
  `.incbin` comment + `#cmakedefine SOURCE_DIR "@SOURCE_DIR@"` block that the
  25.8 diff carried as its insertion-point context (HEAD file is 106 lines,
  ending at `REGISTER_DICTIONARY_SOURCE_YTSAURUS` + a blank line). Patch 045
  also added a `REGISTER_DICTIONARY_SOURCE_YTSAURUS` line at the end of the
  dictionary block. The cherry-pick therefore conflicted; resolved by hand
  (see §6) — the new 29-flag block lands after the dictionary block, and the
  `SOURCE_DIR` context is **not** reintroduced (it no longer exists on HEAD).
- `src/TableFunctions/TableFunctionRemote.cpp` drifted: upstream reshaped the
  `factory.registerFunction(...)` signatures (`{lambda, {}}`,
  `{.documentation = {}, .allow_readonly = true}`). The patch's only semantic
  contribution — wrapping the `remote` registration in
  `#if REGISTER_REMOTE_FUNCTION` — was re-applied onto HEAD's signatures, so the
  added `#if`/`#endif` lines are byte-identical to the source (see §6).
- Conclusion: **`still-needed-but-rewrite`** — semantics intact, but the port
  intentionally diverges from the source by ADDING the matching
  `option(REGISTER_<NAME> "..." ON)` block to `src/configure_config.cmake`
  (the source omitted it), and the two conflicts above were hand-resolved.

### Required deviations from the literal 25.8 patch (intentional hardening)

1. **`option(REGISTER_<NAME> "..." ON)` defaults in `src/configure_config.cmake`.**
   The 25.8 source commit added 29 `#cmakedefine01 REGISTER_<NAME>` lines and
   the `#if` guards but **did not** add matching `option()` lines — on 25.8 the
   private RPM build always passed explicit `-D` flags. On 26.3, the maintainer
   adopted the superior 045 pattern: `#cmakedefine01` of an **undefined** CMake
   symbol resolves to `#define VAR 0`, so without an `option(... ON)` line every
   guarded engine/function would be **silently disabled** in any no-flag
   (dev/CI/OSS) build — a release-breaking regression. We add the full 29-line
   `option(... ON)` block. `option()` (not `set(... 1)`) is parity-safe: a
   `-DREGISTER_<NAME>=0/1` already in the CMake cache **wins** over the option
   default, so the RPM harness's explicit `-D` flags are unaffected, while a
   no-flag build keeps everything ON (= upstream behavior). This is the
   load-bearing divergence and the reason `byte_equivalent: false`.

## 3. C++ review

Apply `docs/aiven/skills/cpp-review-checklist.md`. The change is build-system +
preprocessor-guard only; no runtime data path is altered when defaults are ON.

- 1 Lifetime + ownership: `n/a` — no object lifetime touched; guards only elide
  `registerStorageX` / `registerTableFunctionX` calls at compile time.
- 2 Exception safety: `n/a` — no new throwing code; eliding a `registerX` call
  cannot break exception safety of the surrounding scope.
- 3 Thread-safety + concurrency: `n/a` — registration runs once at startup; no
  concurrency semantics changed.
- 4 Performance + memory: ✓ — strictly reductive (a disabled engine/function is
  not registered → smaller catalog/binary). Default build is behavior-identical
  to upstream.
- 5 Settings as public API: ✓ — no server *setting* added; these are
  compile-time CMake options, not user-visible runtime settings. Default-ON ⇒
  no public-API surface change (de-escalated clause (v): not a default-behavior
  change when ported with `option(... ON)`).
- 6 Error handling: ✓ — when an engine/function is compiled out, using it fails
  via the existing upstream "unknown table engine" / "unknown function" path;
  no new error code needed.
- 7 Upstream / vendored code: ✓ — all edited files are ClickHouse-owned
  (`src/Common/config.h.in`, `src/configure_config.cmake`, three `src/Storages`
  /`src/TableFunctions` `.cpp` files); none under `contrib/**`. The `#cmakedefine01`
  → 0 trap is closed by the `option(... ON)` block (§2 deviation 1).
- 8 Behavior under settings: ✓ — with all toggles default-ON the registration
  set is unchanged (verified: generated `config.h` all = 1; smoke counts 6/6);
  with a toggle `=0` the corresponding engine/function is absent (exercised only
  by the Aiven managed build, out of repo scope). No new wrap beyond the source.

## 4. Test design

(b) **Documented justification — no new test, with reference to existing
upstream tests (`tests.added: no_justified`), mirroring committed sibling 045.**

This patch is **build-system-only** and adds **no runtime behavior on a default
build**: all flags default ON via `option(... ON)`, so the registration set is
identical to upstream. The behavior the patch *introduces* — eliding an
engine/function's registration — is a **compile-time** property of the produced
binary, gated by a CMake option, not a runtime setting. A stateless `.sql`/`.sh`
test cannot observe it on the default build (everything is present), and
producing a "disabled" binary requires a second CMake configure + rebuild that
the stateless runner cannot express. A worktree-flip evidence pair is also
unreachable: pre- and post-patch both register everything on a default build
(pass-pass), so there is no differential observable. Per AGENTS §7(b) the
correct posture is `no_justified` with build-system verification + existing
upstream tests that exercise the gated surfaces.

### Build-system verification (the correctness evidence)

- **Generated `config.h` all = 1** (`tmp/patch-051/configh-generated.log`):
  the post-patch `build/includes/configs/config.h` defines **all 29** new flags
  to `1` on a no-flag build — proving the `option(... ON)` block works and no
  engine/function is silently disabled (the failure mode this dispatch guards
  against). Checked the full set; zero came back `0`/missing.
- **Runtime smoke 6/6** (`tmp/patch-051/smoke-functions.log`,
  `tmp/patch-051/smoke-engines.log`): on the post-patch server,
  `SELECT count() FROM system.table_functions WHERE name IN
  ('s3','url','file','remote','executable','redis')` = **6**, and
  `SELECT count() FROM system.table_engines WHERE name IN
  ('Log','StripeLog','S3','URL','File','KeeperMap')` = **6** — every gated
  surface still resolves.

### Existing upstream tests exercising the gated surfaces (run post-patch, PASS)

Run against the staged/post-patch binary (`tmp/patch-051/upstream-tests.log`):

- `tests/queries/0_stateless/02211_jsonl_format_extension.sql` — uses the
  `file()` **table function** (gated by `REGISTER_FILE_FUNCTION`). Result:
  `[ OK ]`.
- `tests/queries/0_stateless/00423_storage_log_single_thread.sql` — uses the
  `Log` **table engine** (gated by `REGISTER_LOG_TABLE_ENGINE`). Result:
  `[ OK ]`.
- `tests/queries/0_stateless/00288_empty_stripelog.sql` — uses the `StripeLog`
  **table engine** (also `REGISTER_LOG_TABLE_ENGINE`). Result: `[ OK ]`.

- Why this distinguishes the Aiven change from upstream: upstream 26.3 has **no**
  `REGISTER_*_TABLE_ENGINE` / `REGISTER_*_FUNCTION` macro at all (§2), so the
  `#if REGISTER_<NAME>` guards and their observable effect (registration present
  at `=1`) exist *only* because of this patch. With defaults ON the existing
  suite's behavior is unchanged, which is exactly the contract this patch must
  preserve.

## 5. Rollback considerations

- Revert safety: ✓ — pure compile-time change. Reverting removes the toggles,
  guards, and the `option()` block; no schema migration, no on-disk format
  change, no ZK state.
- State surviving restart: none.
- Disabling the new behavior without rebuilding: `n/a` — the toggles are
  compile-time. To re-enable an engine/function that was compiled out, rebuild
  with the toggle `=1` (or no flag, since the in-tree default is ON).

## 6. Per-uplift notes

### 25.8-aiven (historical)

Original carry. Author: Tilman Moeller; committer: Joe Lynch
(`4641e51fdc`). No 25.8 stateless test (build-system change; production
selection driven entirely by the RPM harness `-D` flags). The 25.8 commit did
**not** add `option(... ON)` defaults to `configure_config.cmake` — it relied on
the private build always passing explicit `-D` flags. That omission is corrected
in the 26.3 carry (§2 deviation 1).

### 26.3-aiven (this uplift)

- Cherry-pick was: **conflict-resolved + hardened-rewrite**.
  - `src/Storages/registerStorages.cpp`,
    `src/Storages/ObjectStorage/registerStorageObjectStorage.cpp`,
    `src/TableFunctions/registerTableFunctions.cpp` applied cleanly.
  - `src/Common/config.h.in`: hand-resolved. Upstream removed the
    `.incbin`/`SOURCE_DIR` trailing block (the 25.8 insertion-point context) and
    045 added `REGISTER_DICTIONARY_SOURCE_YTSAURUS`; the new 29-flag block was
    placed after the dictionary block, and `SOURCE_DIR` was **not** reintroduced.
  - `src/TableFunctions/TableFunctionRemote.cpp`: hand-resolved. Upstream
    reshaped the `registerFunction` signatures; the patch's `#if
    REGISTER_REMOTE_FUNCTION`/`#endif` was wrapped around HEAD's `remote` line,
    so the added lines are byte-identical to the source.
- 26.3 adaptation (§2 deviation 1): added the matching 29-line
  `option(REGISTER_<NAME> "..." ON)` block to `src/configure_config.cmake`
  (absent from the 25.8 source). `byte_equivalent: false`. Tier-2 patch-id
  decomposition (`tmp/patch-051/decomposition.log`) shows the **only**
  divergences are (a) that `configure_config.cmake` block and (b) one dropped
  blank line in `config.h.in` from upstream's `SOURCE_DIR` removal — no semantic
  C++ difference. Flag↔option coverage diff
  (`tmp/patch-051/flag-option-coverage.log`) is symmetric: 29 added flags == 29
  added options. Cross-reference the dictionary-source sibling 045.
- Upstream-drift conclusion: `still-needed-but-rewrite` (§2).
- Test: no new stateless test — `no_justified`, build-system-only. Build-system
  verification (generated `config.h` all = 1; smoke 6/6) + three existing
  upstream tests run post-patch and PASS (§4):
  `02211_jsonl_format_extension`, `00423_storage_log_single_thread`,
  `00288_empty_stripelog`.
- Time-to-port: build was effectively **cold/near-full** — editing
  `configure_config.cmake` forced a CMake reconfigure, and the build graph had
  to rebuild ~15.5k steps (sccache-accelerated). Wall: ~40 min
  (`tmp/patch-051/build-start.txt` 14:21:22Z → `build-end.txt` 15:01:29Z,
  ninja exit 0). Incrementals after this are restored.
- Anything surprising: the source commit's omission of the `option()` block
  is the single highest-risk trap — without it, `#cmakedefine01` of the
  undefined symbols would silently disable ~17 engines + ~12 functions
  (including `remote`) in any default build. Step 2.5's coverage diff and Step
  5a's `config.h` all-= 1 check make that impossible to miss.

### Consumer contract (for the `REGISTER_*` family)

The Aiven RPM spec's `%build` passes `-DREGISTER_<NAME>=0/1` for these flags.
The `option(... ON)` form (not `set(... 1)`) is load-bearing: a `-D` cache entry
is set before `configure_config.cmake` runs, and `option()` never overwrites an
existing cache entry, so the spec's value wins. **Family caveat:** a
`-DREGISTER_<NAME>=…` whose flag is not yet defined (because 052/070/071/075 are
not yet ported) is silently ignored by CMake (only a `Manually-specified
variables were not used by the project` warning) — so the 26.3 spec can only
faithfully reproduce 25.8's engine selection once the whole family lands.
Recommendation: the spec/CI should assert that no `REGISTER_*` name appears in
that CMake warning.
