# Patch 068 — systemd-logging

## 0. Lineage

| LTS uplift | First-carry SHA on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `fc8f70cca6d6076df16ee618e910d79217182752` | Joe Lynch <joelynch112@gmail.com> | (original carry on `v25.8.18.1-lts-aiven`) |
| 26.3-aiven | (staged — `test_design_blocked`, awaiting human commit) | T3.21 worker | clean cherry-pick, byte-equivalent |

The current uplift's row stays "(staged)" until the human commits.

## 1. Purpose

When ClickHouse runs as a systemd service, plain `Poco::ConsoleChannel` output (stdout/stderr) reaches the journal only as opaque, line-split text: multiline payloads such as stack traces are shredded into one journal entry per physical line, and there is no structured priority or ClickHouse-specific metadata. This patch adds a `SystemdJournalChannel` that speaks the systemd journal *native* protocol over `/run/systemd/journal/socket`, so each log message becomes a single structured journal entry with a real syslog `PRIORITY=`, multiline-safe `MESSAGE`, and ClickHouse fields (`CLICKHOUSE_TIMESTAMP`, `CLICKHOUSE_SOURCE`, `CLICKHOUSE_QUERY_ID`, `TID`, `CODE_FILE`, `CODE_LINE`).

The "why" is durable: Aiven runs ClickHouse under systemd in production, and operators want `journalctl -p`, query-id correlation, and intact stack traces without scraping line-split console output. The behavior is opt-in by environment: it activates only when the `JOURNAL_STREAM` env var is set (which systemd sets for its managed units) and the journal socket connects; otherwise it falls back to the original `Poco::ConsoleChannel`, so interactive/non-systemd runs are byte-for-byte unchanged.

Source SHA on `v25.8.18.1-lts-aiven`: `fc8f70cca6d6076df16ee618e910d79217182752` (inventory row 068).
Original author: `Joe Lynch <joelynch112@gmail.com>` (AuthorDate `Tue Jan 27 15:40:59 2026 +0100`).
Original purpose (quoted):

```
Systemd logging

* set priority and other fields correctly
* log mutiline properly
```

## 2. Upstream-drift findings

### Commands run

```bash
# Net-new identifiers — MUST be ABSENT on HEAD (collision check):
for id in SystemdJournalChannel isRunningUnderSystemd JOURNAL_STREAM; do git grep -c "$id" -- src/; done
# Depended-on API — MUST be PRESENT on HEAD:
for id in ExtendedLogMessage getFrom OwnFormattingChannel getFormatForChannel OwnSplitChannel; do git grep -c "$id" -- src/; done
git log --oneline v25.8.18.1-lts-aiven..HEAD -- src/Loggers/Loggers.cpp
git log --oneline -S 'SystemdJournalChannel' -- src/Loggers/
git log --oneline -S 'journal' -i -- src/Loggers/Loggers.cpp
grep -nE 'OwnFormattingChannel.*new Poco::ConsoleChannel|should_log_to_console|getFormatForChannel\(config, "console"' src/Loggers/Loggers.cpp
```

### Findings

- Net-new identifiers (collision check): `SystemdJournalChannel` = 0, `isRunningUnderSystemd` = 0, `JOURNAL_STREAM` = 0 on HEAD → no collision; the two new files and two new symbols are net-new.
- Depended-on API (must exist): `ExtendedLogMessage` = 28, `getFrom` = 64, `OwnFormattingChannel` = 24, `getFormatForChannel` = 7, `OwnSplitChannel` = 22 → all present.
- `ExtendedLogMessage.h` still provides every member the new TU uses: `static ExtendedLogMessage getFrom(const Poco::Message &)`, `const Poco::Message * base`, `uint64_t time_in_microseconds`, `uint64_t thread_id`, `std::string query_id`.
- Upstream changes to touched files between prior and current LTS:
  - `src/Loggers/Loggers.cpp`: 4 unrelated commits (`code review`, `Add OwnFileChannel to support composite rotation strategy`, `Async log: Flush earlier and increase default queue size`, `Add ability to enable JSON logging only for specific channel`). None touched the first console-channel construction line.
- Upstream changes that touched the patch's behavior (symbols, error codes, callers):
  - The replacement anchor `auto log = std::make_shared<DB::OwnFormattingChannel>(pf, new Poco::ConsoleChannel);` is present verbatim at line 248, with context `should_log_to_console` (235), `getFormatForChannel(config, "console", ...)` (247) matching the source's pre-patch shape.
  - Upstream-equivalent search (`-S 'SystemdJournalChannel'` and `-S 'journal' -i`) returned empty → upstream did NOT add systemd-journal logging; the patch is still needed.
- Conclusion: **`still-needed-and-applies`** (recorded as `still-needed-applies-cleanly`). Clean cherry-pick, no collision, no upstream-equivalent, API premise verified.

## 3. C++ review

Applying `docs/aiven/skills/cpp-review-checklist.md`:

- **1 Lifetime + ownership:** ✓ — Channels are managed by `Poco::AutoPtr` (intrusive ref-count, the Poco convention used throughout `Loggers.cpp`); `new DB::SystemdJournalChannel()` / `new Poco::ConsoleChannel()` are immediately captured into `Poco::AutoPtr<Poco::Channel> channel` and handed to `OwnFormattingChannel`, so no raw pointer outlives its owner. The single `journal_fd` is a process-global `static int` (no per-instance ownership).
- **2 Exception safety:** ✓ — The logging path cannot throw out: `sendToJournal` ignores `send` errors (`(void)sent`) and uses `MSG_NOSIGNAL`; `open()` swallows socket/connect failures and leaves `journal_fd = -1`. Buffer building uses `std::string` (may throw `bad_alloc` only under OOM, same risk as any Poco channel). Known limitation: errors are silently dropped by design ("logging should not throw").
- **3 Thread-safety + concurrency:** ✓ with a noted limitation — the socket is opened exactly once via `std::call_once(init_once_flag, ...)`, so concurrent loggers race-free on init. `send` on a connected `SOCK_DGRAM` unix socket is atomic per datagram and MT-safe at the syscall level. Limitation (documented, not a defect): `journal_fd` is a one-shot process-global; if the journal socket drops it never reconnects (it would just silently stop sending). No new lock; no `Context` interaction.
- **4 Performance + memory:** ✓ — This is the logging path, not a per-row hot path. Each message builds one `std::string buffer` with `buffer.reserve(text.size() + 512)` (one pre-sized allocation, amortizing the field appends) and issues a single `send()`. The cost is one allocation + one syscall per log line — acceptable for a logging channel and no worse than the formatting Poco already does. No `Chunk`/`IColumn` semantics involved.
- **5 Settings as public API:** n/a — no new setting; the gate is the `JOURNAL_STREAM` environment variable (set by systemd), not a ClickHouse setting. No `Settings.cpp` / `SettingsChangesHistory.cpp` change.
- **6 Error handling:** n/a — no `ErrorCodes` use; the patch throws nothing and gates on no error code. Failures degrade to console fallback (`isConnected()` false → `Poco::ConsoleChannel`) or silent no-op (`send` failure).
- **7 Upstream / vendored code:** ✓ — touches only `src/Loggers/` (two new files + one gate in `Loggers.cpp`). No `contrib/**`, no `.claude/**`, no `.github/workflows/**`, no root `AGENTS.md`. The surrounding `Loggers.cpp` was refactored 4× upstream but none of those changes invalidate the patch's assumptions (anchor verbatim, `ExtendedLogMessage` API intact).
- **8 Behavior under settings:** ✓ — gated on `under_systemd = isRunningUnderSystemd()` (`JOURNAL_STREAM` present). When unset (interactive / non-systemd), the code takes the unchanged `new Poco::ConsoleChannel()` branch: no journal socket opened, no extra allocations, nothing observable changes. The effect is a strict opt-in.

## 4. Test design

**Outcome: `test_design_blocked` — pre-approved by the human; this patch ships WITHOUT a new test, with the justification recorded below.**

### Why no test is feasible (the justification)

Patch 068 adds zero SQL/HTTP-visible surface: no system table, no setting, no async metric, no query-behavior change. Its only observable effect is bytes sent to a unix datagram socket (`/run/systemd/journal/socket`), and only when the `JOURNAL_STREAM` env var is set **and** that socket exists and connects (otherwise it silently falls back to `Poco::ConsoleChannel`). A stateless `.sql`/`.sh` test is therefore impossible — there is nothing to `SELECT`/observe from a client.

The two paths that *could* test it were both rejected by the human:

- An **integration test with a fake journal socket** is feasible but was judged too much plumbing / flakiness for this patch's value right now.
- A **gtest unit test** of the framing/priority logic would require a bounded source divergence (the framing builder `sendToJournal` and `getSyslogPriority` are `private static`), which adds permanent re-merge cost to every future uplift — explicitly rejected.

Precedent in this uplift: patch 037 (environment-dependent) and patch 040 (default-config-only) shipped with documented coverage gaps.

### Strongest test that WOULD work if revisited later (actionable gap)

An integration test (under `tests/integration/`) that:

1. Creates a `SOCK_DGRAM` listener bound at `/run/systemd/journal/socket` (a fake journald) inside the test container.
2. Starts the server with `JOURNAL_STREAM` exported and `<logger><console>true</console></logger>`.
3. Drives a log line (any query that logs at the configured level), reads the datagram off the fake socket, and asserts the systemd native-protocol framing: the binary `MESSAGE\n` + little-endian `uint64` length prefix + payload, a numeric `PRIORITY=` matching the Poco→syslog map, and the `CLICKHOUSE_QUERY_ID=` / `TID=` / `CODE_FILE=` / `CODE_LINE=` fields.

This is the path to close the gap if the value justifies the plumbing in a future cycle. The gap is declared, not silent, and is actionable.

## 5. Rollback considerations

- **Is the revert safe?** Yes, trivially. The patch changes only in-memory logging-channel *selection* at startup (`buildLoggers`). There is no schema migration, no on-disk format change, no ZooKeeper state, no setting persisted anywhere.
- **State surviving a restart?** None. The only durable artifact is journal entries already shipped to systemd (external, append-only journald state, identical in nature to ordinary console logs reaching the journal). No ClickHouse-side state is created.
- **Disabling without rebuild?** Unset the `JOURNAL_STREAM` environment variable (or run outside systemd): the channel selection falls back to `Poco::ConsoleChannel` with no rebuild required. There is also the natural runtime fallback — if the journal socket fails to connect, the code already uses `Poco::ConsoleChannel`.

## 6. Per-uplift notes

### 25.8-aiven (historical)

n/a — original carry on `v25.8.18.1-lts-aiven` (`fc8f70cca6…`, Joe Lynch).

### 26.3-aiven (this uplift)

- Cherry-pick was: **clean** (`git cherry-pick --no-commit -x`, exit 0, `Auto-merging src/Loggers/Loggers.cpp`, no conflict markers). Three files staged: `Loggers.cpp` (modified), `SystemdJournalChannel.cpp` (new), `SystemdJournalChannel.h` (new).
- Patch-id (Tier 2): source and staged `git patch-id --stable` both `2ee1ed7c59a1ea1b4c5816c74e20b588053dde8f` → byte-equivalent.
- Second-console-block verification (policy call 3): `Loggers.cpp` has a near-duplicate console block lower in the file (`should_log_to_console` at line 355). The staged diff confirms the `isRunningUnderSystemd` helper and the `under_systemd` gate landed **only** on the first `buildLoggers` console block (hunk `@@ -245,7 +253,28 @@`, around line 248); the second block is untouched. Verified by inspecting `git diff --cached -- src/Loggers/Loggers.cpp`.
- Build (Tier 3a): glob-refresh via plain `cmake -B build` (new `.cpp` enters `build.ninja`), then `ninja -C build clickhouse` — exit 0, `Linking CXX executable programs/clickhouse`. The new TU compiled (fresh `SystemdJournalChannel.cpp.o`, 154 KB) and `libloggers.a` + the full executable linked, confirming the clause-(iii) API premise against HEAD (no renamed symbol).
- Upstream-drift conclusion: `still-needed-and-applies` (no upstream systemd-journal logging exists).
- Test added at: none — see §4 (`test_design_blocked`, human pre-approved; no SQL/HTTP-visible surface to assert on).
- Time-to-port: ~4 min subagent wall-clock; build directory was **warm-cache** (sccache hot; the glob refresh + Loggers relink + new TU compile was a ~50-step incremental, not a `contrib` rebuild).
- Anything surprising: the cherry-pick was textbook-clean despite 4 intervening upstream refactors of `Loggers.cpp`; the only non-obvious step was the CMake glob refresh required for the brand-new translation unit.
