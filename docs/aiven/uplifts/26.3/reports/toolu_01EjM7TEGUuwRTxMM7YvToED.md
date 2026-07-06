<!-- subagentStop archive | source=transcript | status=completed | type=general-purpose | ts=2026-06-12T11:50:32Z -->

 I'm going to examine the commits to understand what patches are involved here.

Now let me run the apply checks against HEAD and verify the symbols at HEAD.

 I'm tracing through a code change that introduces an `internal` flag check in the `doCreateTable` function, which gates certain operations based on whether they're internal. The change also propagates this flag through `InterpreterRenameQuery`, `DatabaseReplicated`, and `StorageMaterializedView`, but I'm uncertain whether `InterpreterCreateQuery` already has this member or if it needs to be added.

Looking at the subsequent patches, I see that patch 063 changes temporary table naming to use `.tmp_` prefix, and then patch 064 calls `setInternal(true)` before creating the temporary replacement table. This creates a real dependency chain: patch 064 needs both the `setInternal` mechanism from 062 and the `.tmp_` naming from 063, because without the internal flag check from 062, the temporary table creation would fail when it tries to create a table starting with `.tmp_`. Now I'm verifying the textual dependency by checking that 064's parent commit matches 062's result, which confirms 064 was built directly on top of 062's version of InterpreterCreateQuery.cpp. I should also run the apply checks and confirm that `internal` and `setInternal` already exist in the current codebase.

Let me examine the HEAD state of the touched symbols to characterize drift.

 Looking at the patch application results across these commits — 062 has a conflict in the include block when running with --check, while 063 and 064 both apply cleanly in isolation, though 064's success seems tied to the parent commit's state matching the expected doCreateOrReplaceTable line.

The key issue is understanding the dependencies: 062 + 063 without 064 breaks CREATE OR REPLACE functionality, so I need to trace through the actual HEAD state of InterpreterCreateQuery to see if the internal member and setInternal method exist, and verify the context around the doCreateTable and doCreateOrReplaceTable lines to understand why 062 fails while the others succeed.

 I'm seeing that InterpreterCreateQuery already has the `internal` member and setter at HEAD, so both the check and the setInternal call will compile without needing the header changes from patch 062. The key difference is that 062 only adds setInternal to InterpreterRenameQuery, which doesn't have it currently.

The patch failures stem from include block changes in InterpreterRenameQuery.cpp where HEAD has a new NamedCollectionsFactory include, causing the hunk at line 9 to fail, though the BAD_ARGUMENTS ErrorCode addition should still apply. For doCreateTable, the checkTableNameLength call has shifted to line 1953 due to code changes, but a three-way merge should handle it. TemporaryReplaceTableName.cpp remains unchanged, so patch 063 applies cleanly. Patch 064 also applies cleanly since the target line for doCreateOrReplaceTable still exists at the expected location.

The critical insight is that patches 062, 063, and 064 must be applied in strict order for correctness, not just for clean application. Patch 062 establishes the prohibition against certain table names and the internal-bypass mechanism. Patch 063 changes the naming convention to `.tmp

 I'm checking the current state of the codebase to understand which changes are already present. InterpreterCreateQuery already has the `internal` member and `setInternal` method at HEAD, so the call in 064 should work standalone, but 062's check for `!internal` in `doCreateTable` relies on that existing. InterpreterRenameQuery.h doesn't have `setInternal` or `internal` yet—062 adds those, and without it, 064 won't compile properly.

For the StorageMaterializedView change, 062 calls `interpreter.setInternal(true)` on InterpreterRenameQuery, which requires the header changes from 062. The InterpreterRenameQuery.cpp patch is failing because the include order has shifted at HEAD—there's now a NamedCollectionsFactory.h include between AccessRightsElement.h and typeid_cast.h, so the context lines in the patch no longer match exactly where StringUtils.h needs to be inserted.

The ErrorCodes hunk for BAD_ARGUMENTS should match the patch context at HEAD since only NOT_IMPLEMENTED and LOGICAL_ERROR are present, but the whole file fails because git apply treats it as all-or-nothing when the first hunk fails. TemporaryReplaceTableName.cpp applies cleanly since it still has the `_tmp_replace_` code. For doCreateOrReplaceTable, the 064 patch applies clean, but I need to verify that 062's checkTableNameLength hunk context around line 1953 is still intact—specifically checking if `data_path = database->getTableDataPath(create);` still follows the checkTableNameLength call.

 I'm checking the patch application points across the files. The InterpreterCreateQuery.cpp hunk looks good—the surrounding context matches what's at HEAD, so that section should apply cleanly. Now I'm verifying the DatabaseReplicated.cpp changes.

For StorageMaterializedView.cpp, I notice the create_interpreter calls are already there from earlier patches, but the rename interpreter at line 630 still needs the internal flag set. The patch context lines up with what's currently in the file, so that should apply. However, I'm hitting a snag with InterpreterRenameQuery.cpp—there's a conflicting include that was inserted in between, which is blocking the patch from applying there.

The core issue is that 062 establishes a prohibition against creating tables with `.tmp` prefix unless marked internal, then 063 renames temporary tables to use that `.tmp_replace_` pattern, and 064 ensures those temp-table-creating interpreters get marked internal before execution. This creates a dependency chain where each patch builds on the previous one's mechanism.

 I'm noticing some interesting patterns in the StorageMaterializedView code—there are already `setInternal(true)` calls at lines 316 and 580 from the pre-existing Aiven ports on this branch, and the 062 patch modifies the `exchangeTargetTable` method around lines 612-630, which still relies on the bare `InterpreterRenameQuery` constructor with the refresh context.

Looking at the broader dependency chain: 062 introduces the `.tmp` naming prohibition in both `InterpreterCreateQuery::doCreateTable` and `InterpreterRenameQuery::executeToTables`, leveraging the pre-existing `internal` member in InterpreterCreateQuery while adding new `setInternal`/`internal` symbols to InterpreterRenameQuery. Then 063 changes the temp table naming scheme from `_tmp_replace_` to `.tmp_replace_`, which triggers the prohibition that 062 just added. Finally, 064 adds `setInternal(true)` to InterpreterCreateQuery itself to handle these cases.

The textual dependency is clear: 064's parent blob for InterpreterCreateQuery.cpp matches exactly what 062 produced, so 064 was authored on top of 062. But semantically, 064 depends on 063 — without the naming change to `.tmp_replace_`, the prohibition wouldn't apply and `setInternal(true)` would be unnecessary. More critically, 063 without 064 (on top of 062) would break CREATE OR REPLACE entirely, since the temp table would fail the prohibition check. This is the strongest signal that the strict ordering matters and that 063 and 064 need to land together.

Now I'm checking whether `doCreateOrReplaceTable` calls `doCreateTable` on the same InterpreterCreateQuery instance or creates a fresh one — the diff shows it's instantiating a new InterpreterCreateQuery with the query pointer and context. So 063 without 062 and 064 is just a harmless rename with no enforcement, and 064 alone is a no-op without 063's temp table changes. The strict ordering is genuinely required for correctness across intermediate states. Now looking at the drift against 26.3: patch 062 fails to apply cleanly because the include block in InterpreterRenameQuery.cpp has shifted — HEAD inserted a new include between existing ones, breaking the context for the StringUtils.h addition.  The core logic anchors are intact though, so it's a trivial manual fix. Patches 063 and 064 both apply cleanly with no drift. The risk profile is medium for 062 due to the include conflict and moderate regression surface across replicated DDL and materialized view paths, but low for 063 since it's a single literal change, and low-to-medium for 064 since it depends entirely on the prior two patches being present.

Now I'm writing up the final report, keeping it structured per the mentor guidelines but concise since this is research output. I'll organize it by patch with a summary cluster and keep the learning-log integration light.

# Porting Preflight: `.tmp` table chain (062 → 063 → 064)

## Goal restatement
You want to confirm the strict-order dependency among three unported Aiven patches and characterize their drift against 26.3 HEAD (`5f45a89693f`, the `v26.3.10.62-lts-aiven-dev` branch). The mechanism under test: a prohibition on user-created `.tmp*` tables, plus the two internal "fake temporary table" code paths that must be exempted from it.

---

## 062 — `50a42b769e` "Prohibit .tmp table creation"

**1. WHAT.** Installs a guard that rejects any non-internal `CREATE`/`RENAME` whose target table name starts with `.tmp`. In `InterpreterCreateQuery::doCreateTable` it adds `if (!internal && startsWith(create.getTable(), ".tmp")) throw BAD_ARGUMENTS`, reusing the *pre-existing* `internal` member of `InterpreterCreateQuery`. It adds the same guard to `InterpreterRenameQuery::executeToTables` (keyed on `getContext()->isInternalQuery()`), and—because `InterpreterRenameQuery` had no internal concept—adds a brand-new `setInternal`/`internal{false}` member to `InterpreterRenameQuery.h`. Finally it flips the two legitimate internal callers to internal mode: `DatabaseReplicated::recoverLostReplica` (the replica-recovery `CREATE`) and `StorageMaterializedView::exchangeTargetTable` (the RMV rename).

**2. FILES (~19 loc).** `DatabaseReplicated.cpp` (+3), `InterpreterCreateQuery.cpp` (+4), `InterpreterRenameQuery.cpp` (+9), `InterpreterRenameQuery.h` (+3), `StorageMaterializedView.cpp` (+3).

**3. CHAIN DEPENDENCY.** This is the *base* of the chain — it defines the prohibition that 063/064 must dance around. It introduces the symbol the chain hinges on: the `.tmp` check in `doCreateTable` and `setInternal` on `InterpreterRenameQuery`. Nothing here depends on 063/064.

**4. DRIFT vs 26.3.** All five files exist. `git apply --check` **FAILS** (and `--3way --check` reports all five "does not match index", expected given 25.8→26.3 blob drift). The single real conflict hotspot is the include block of `InterpreterRenameQuery.cpp`: the patch adds `+#include <Common/StringUtils.h>` using context `AccessRightsElement.h → typeid_cast.h`, but HEAD inserted `#include <Common/NamedCollections/NamedCollectionsFactory.h>` between those two lines, breaking the context. The substantive anchors are all intact at HEAD:
- `doCreateTable`: the `checkTableNameLength(create.getTable());` → `data_path = database->getTableDataPath(create);` window is unchanged.
- `DatabaseReplicated.cpp:1822` still has the bare `InterpreterCreateQuery(query_ast, create_query_context).execute();` followed by `if (allow_concurrent_table_creation)`.
- `StorageMaterializedView.cpp:630` (`exchangeTargetTable`) still has the bare `InterpreterRenameQuery(rename_query, refresh_context).execute();`.
- `InterpreterCreateQuery.h` already carries `setInternal`/`internal` (lines 49, 138) — so the create-side hunk needs no header change.

So the failure is a one-line include-ordering shift, not logic drift. Re-anchor the include hunk and the rest lands.

**5. RISK: MED.** Only patch with a real apply conflict, and it touches replicated-DDL, rename, and RMV-exchange paths — broadest blast radius — but the conflict is trivial and all logic anchors survive.

---

## 063 — `cc718390aa` "Use .tmp for all fake temporal tables"

**1. WHAT.** Changes the `CREATE OR REPLACE` temporary table naming scheme so it shares the `.tmp` namespace with the RMV temp tables. In `TemporaryReplaceTableName`, `toString` now emits `.tmp_replace_{hash}_{suffix}` (was `_tmp_replace_…`) and `fromString`'s regex becomes `^\.tmp_replace_(\w+)_(\w+)$`. This makes the CREATE-OR-REPLACE temp table *fall under* 062's `.tmp` prohibition.

**2. FILES (~4 loc).** `TemporaryReplaceTableName.cpp` only (2 changed lines: literal + regex).

**3. CHAIN DEPENDENCY.** No *textual* overlap with 062 (different file). But it is **semantically downstream of 062 and requires 064 to follow**: renaming the temp table into the `.tmp` namespace is only meaningful because 062 reserved that namespace, and it actively breaks the create path until 064 exempts it. Out-of-order it is either pointless (before 062) or regression-inducing (after 062, before 064).

**4. DRIFT vs 26.3.** `TemporaryReplaceTableName.cpp` exists and is **byte-identical** in the touched region at HEAD (`_tmp_replace_` literal + regex unchanged). `git apply --check` is **clean** (`--3way` only flags index mismatch). No logic drift.

**5. RISK: LOW.** One-line literal+regex swap, clean apply, zero drift — *provided* it is sequenced with 064.

---

## 064 — `c95ef0cc6b` "asd .tmp create or replace" (garbled subject)

**1. WHAT (real change).** Despite the placeholder subject, the diff is the missing exemption for the `CREATE OR REPLACE` path. In `InterpreterCreateQuery::doCreateOrReplaceTable`, the inner create previously was `InterpreterCreateQuery(query_ptr, create_context).doCreateTable(...)` — a *fresh* interpreter with `internal == false`. 064 hoists it into a named `interpreter`, calls `interpreter.setInternal(true)`, then `doCreateTable`. Effect: the temporary `.tmp_replace_…` table (renamed by 063) is created as an internal query and therefore *bypasses* 062's `.tmp` prohibition. Without this, `CREATE OR REPLACE` would throw `BAD_ARGUMENTS`.

**2. FILES (~3 loc).** `InterpreterCreateQuery.cpp` only.

**3. CHAIN DEPENDENCY — confirmed, dual.**
- **Textual/provenance link to 062:** 064's pre-image blob for `InterpreterCreateQuery.cpp` is `cb8076223ed`, which is *exactly the post-image* of 062's edit to that same file — i.e., 064 was authored directly on top of 062. (The two hunks themselves are in different functions, `doCreateTable` ~1863 vs `doCreateOrReplaceTable` ~2147, so they don't physically collide.)
- **Semantic link to 063:** `setInternal(true)` here only *matters* once the temp table name starts with `.tmp` (063) and the prohibition exists (062). Without 063 the temp name is `_tmp_replace_`, never trips the guard, and 064 is a no-op.

**4. DRIFT vs 26.3.** File exists; `git apply --check` is **clean** (`--3way` only flags index mismatch). The `[[maybe_unused]] bool done = InterpreterCreateQuery(query_ptr, create_context).doCreateTable(create, properties, ddl_guard, mode);` line and surrounding context are intact at HEAD, and `setInternal` already exists on `InterpreterCreateQuery`. No drift.

**Caveat you asked me to test — apply of 063/064 against HEAD WITHOUT 062:** both apply *clean in isolation*, and this is **misleading**:
- 063 alone: applies, but is a pointless rename (no prohibition to satisfy, no exemption needed).
- 064 alone: applies and even compiles (`setInternal` pre-exists), but is a **silent no-op** — it marks a create internal for a guard that isn't there yet.
- The dangerous intermediate is **062 + 063 without 064**: that state compiles but breaks `CREATE OR REPLACE` at runtime (the `.tmp_replace_…` create hits the new guard with `internal == false` → `BAD_ARGUMENTS`). Apply-clean tells you nothing about this; correctness requires the full ordered triple.

**5. RISK: LOW–MED.** Trivial clean apply, but its correctness is *entirely* contingent on 062+063 already being present; landed out of order it silently does nothing or leaves CREATE OR REPLACE broken.

---

## Cluster summary
1. **Strict order is genuine, not conventional:** 062 reserves the `.tmp` namespace, 063 moves CREATE-OR-REPLACE temp tables *into* that namespace (tripping the guard), and 064 exempts that create path — landing 062+063 without 064 ships a runtime-broken `CREATE OR REPLACE` (`BAD_ARGUMENTS`), and 064 is a no-op without 063. Port them as a contiguous, ordered block.
2. **Apply-clean is deceptive:** 063 and 064 each apply cleanly in isolation against HEAD, but only 062 carries a real conflict (a one-line include-ordering shift in `InterpreterRenameQuery.cpp` from an inserted `NamedCollectionsFactory.h`); all logic anchors for 062 survive at HEAD.
3. **Riskiest item: 062** (MED — sole apply conflict, broadest blast radius across DDL/rename/RMV paths); the riskiest *sequencing hazard* is allowing 063 to merge ahead of 064.

---

**Today you learned:** `git apply --check` validates text adjacency, not semantic ordering — a clean apply can still hide a logically broken intermediate state. Patch pre-image blob indices (`index <old>..<new>`) are a reliable fingerprint for proving one commit was authored on top of another.
**Rule of thumb:** For a dependency chain, reason about each *intermediate* tree's runtime correctness, not just whether the final patch applies.
**Next rabbit hole:** Confirm whether any existing stateless/integration test exercises `CREATE OR REPLACE` on a `Replicated` database, since that is exactly the path 064 protects and the place a botched ordering would surface.
