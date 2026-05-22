# Aiven LTS patch — C++ / database review checklist

> A focused checklist for the T3+ patch worker. Apply every item explicitly: write the answer in the dossier's `## 3. C++ review` section, even when the answer is "n/a — patch is config-only" or "n/a — no allocation involved". Silence is not evidence.
>
> This list is **not** exhaustive of all C++ pitfalls; it codifies the small set that matters for the ClickHouse binary that goes to Aiven production.

## 1. Lifetime + ownership

- [ ] Every raw pointer in the patch has a documented owner (or is plainly transient).
- [ ] `shared_ptr` vs `unique_ptr` choices match ClickHouse conventions in the surrounding code (`DB::ContextPtr`, `DB::StoragePtr`, etc.).
- [ ] No new dependency from a long-lived object to a short-lived one (e.g. capturing `Context &` in a thread that outlives the query).

## 2. Exception safety

- [ ] If the patch introduces a new path that can throw, the caller's catch-block (or `SCOPE_EXIT`) still leaves invariants intact.
- [ ] Partial mutations are rolled back on throw — or the partial state is documented as acceptable.
- [ ] `noexcept` is added only when the body is truly noexcept (no allocation, no called function that throws).

## 3. Thread-safety + concurrency

- [ ] Shared state is either immutable (`const`), protected by a mutex named in the patch, or via `Context`'s already-thread-safe accessors.
- [ ] No new lock taken while holding a `Context` mutex (deadlock risk against the global `Context::shared` mutex chain).
- [ ] If the patch introduces a thread pool job, the captured state outlives the job (see §1).
- [ ] **No sleep-based race-condition fixes.** This is a hard ban per repo policy.

## 4. Performance + memory

- [ ] If the patch is in a per-row hot path, it preserves batch semantics (`Chunk` / `IColumn` / `MutableColumns`) — no per-row virtual dispatch or per-row allocation introduced.
- [ ] No new string concatenation, `std::string` copy, or `std::format` call in a hot loop. Prefer `StringRef`, `WriteBufferFromString`, or arena allocation when the surrounding code does.
- [ ] If the patch is in a settings / parse / control path, performance is irrelevant — say so explicitly.

## 5. Settings as public API

- [ ] If the patch reads or writes a setting, the setting name appears in `src/Core/Settings.cpp` or `src/Core/SettingsChangesHistory.cpp` (or is added by the patch).
- [ ] Default value preserves backward compat for existing users (a new bool default `true` is suspicious; `false` is usually safer for a new gate).
- [ ] Setting type matches what the patch passes to `getSetting<T>` or `setSetting`.

## 6. Error handling

- [ ] Error codes used are real (`src/Common/ErrorCodes.cpp`), not invented inline.
- [ ] If the patch GATES a user action with a new error, the error MESSAGE distinguishes it from upstream errors that share the code. (Aiven gates often share `SUPPORT_IS_DISABLED` / `BAD_ARGUMENTS` with upstream — the message must be distinctive enough to test against.)

## 7. Upstream / vendored code

- [ ] The patch does not modify `contrib/**` (vendored upstream).
- [ ] The patch does not modify `.claude/**`, `.github/workflows/**`, or root `AGENTS.md` (hook will deny).
- [ ] If the surrounding ClickHouse code was refactored between prior LTS and this LTS, the patch's assumptions still hold. (This belongs in `## 2. Upstream-drift findings` in the dossier — record evidence here too if it changed the C++ shape.)

## 8. Behavior under settings

- [ ] If the patch's effect is gated by a setting, it must do nothing observable when the setting is off (no log spam, no extra allocations, no extra ZK calls).
- [ ] If the patch enables a setting unconditionally in some code path (like patch 007 does for `enable_deflate_qpl_codec` in `recoverLostReplica`), justify why — and why the user cannot turn it off without breaking recovery.

## Output format

For each checklist item, the dossier records one of:

- `✓ — <one-sentence evidence>` (file + line, or "covered by the test at <path>").
- `n/a — <one-sentence reason>` (e.g. "config-only patch, no C++ semantics changed").
- `🚨 — <one-sentence finding>` (the item failed; escalate via `halt-and-escalate` with `escalation_reason: cpp_concern` and a concrete proposed fix).

Five lines per checklist section is the target. If you can't say something in one sentence per item, that's evidence the patch is more complex than it looked — surface that in the retrospective.
