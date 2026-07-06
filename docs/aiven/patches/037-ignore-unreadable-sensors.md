# Patch 037 — ignore-unreadable-sensors

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `7a7058eba8` | Tilman Moeller (author) / Joe Lynch (committer) | (the version we are porting FROM) |
| 26.3-aiven | `patch-port(037)` | parent agent, 2026-05-31 | ported, `test_design_blocked` (untestable by nature; see §4) |

## 1. Purpose

A convenience patch to stop `AsynchronousMetrics` from polluting the server log
with `LOG_WARNING` lines on machines whose hardware advertises thermal/EDAC
sensors that cannot actually be read (the commit body cites "developer laptops
with GPU drivers that advertise non-working thermal sensors").

It removes the `openEDAC()` and `openSensorsChips()` calls from the
`AsynchronousMetrics` constructor, so those sensor sources are never opened.

Source SHA on `v25.8.18.1-lts-aiven`: `7a7058eba8`.
Original author: `tilman.moeller@aiven.io`. Co-author: Kevin Michel.
Original purpose (verbatim from the source commit body):

> Ignore unreadable sensors
>
> This is a convenience patch to avoid polluting logs on developer laptops
> with GPU drivers that advertise non-working thermal sensors.
>
> The patch disables initialization of EDAC (Error Detection And Correction)
> and hardware monitoring chip sensors by removing the calls to openEDAC()
> and openSensorsChips() from the AsynchronousMetrics constructor.

## 2. Upstream-drift / validity findings

### Commands run

```bash
# Constructor still calls the two functions on 26.3 HEAD?
sed -n '193,196p' src/Common/AsynchronousMetrics.cpp
# → openSensors(); openBlockDevices(); openEDAC(); openSensorsChips();
#   The two removed calls are present at :195-196 — patch applies cleanly.

# patch-id equivalence
git show 7a7058eba8 | git patch-id --stable                 # 7c62e541306a...
git diff --cached -- src/Common/AsynchronousMetrics.cpp | git patch-id --stable  # 7c62e541306a... (MATCH)
```

### Findings

- **Still applies & byte-equivalent.** Clean `cherry-pick -x`; stable patch-id
  matches source exactly (`byte_equivalent: true`).
- **Still effective on 26.3 — purpose survives.** The removed constructor calls
  are the only places that *populate* `edac` / `hwmon_devices`. The update loop's
  `openEDAC()` / `openSensorsChips()` calls (`:2335` / `:2286`) live inside
  `catch` blocks — recovery re-opens that fire only when a read on an
  *already-open* sensor throws. With the constructor calls removed, the vectors
  start empty, the loop iterates nothing, no read throws, and the catch-path
  re-open never fires. So the sensors are never opened and the
  `LOG_WARNING` (`openSensorsChips`, ~`:350`) is never emitted. The patch is NOT
  a no-op (contrast 044): on affected hardware it genuinely suppresses the
  warnings.
- **No obsolescence.** Upstream has not added a graceful-skip or a setting that
  would supersede this; the constructor still unconditionally opens the sensors.

## 3. C++ review

- 1 Lifetime + ownership: `n/a — removes two calls; no new objects/ownership.`
- 2 Exception safety: `✓ — strictly fewer operations in the constructor; nothing added that can throw.`
- 3 Thread-safety + concurrency: `✓ — the removed calls were under data_mutex in the constructor (TSA_REQUIRES(data_mutex)); removing them cannot introduce a race. The update loop still holds the same mutex.`
- 4 Performance + memory: `✓ (minor positive) — skips a /sys scan at startup.`
- 5 Settings as public API: `n/a — no setting involved. NOTE: behavior is no longer configurable at runtime; EDAC/hwmon async metrics (EDAC*_Correctable/Uncorrectable, Temperature*) are simply absent. Acceptable for the convenience intent; flagged for awareness.`
- 6 Error handling: `✓ — removes a source of LOG_WARNING noise; introduces no new error path.`
- 7 Upstream / vendored code: `✓ — touches only src/Common/AsynchronousMetrics.cpp; no contrib/**, .claude/**, root AGENTS.md, workflows.`
- 8 Behavior under settings: `n/a.`

## 4. Test design

(c) **No test — `test_design_blocked` (untestable by nature).**

- Why no evidence-of-causation pair is constructible:
  - The observable the patch changes is a `LOG_WARNING` that `openSensorsChips`
    emits **only when a temperature read throws `ErrnoException`** — i.e. only on
    a machine that has a hwmon/EDAC sensor node which *exists* but cannot be
    read. CI runners generally have no such broken sensors, so the warning never
    appears pre- or post-patch → a vacuous fail→fail, not fail→pass.
  - `AsynchronousMetrics` reads **absolute** `/sys/class/hwmon` and
    `/sys/devices/system/edac` paths. A stateless test cannot redirect, sandbox,
    or fault-inject these, so the broken-sensor condition cannot be simulated.
  - The secondary observable (absence of `EDAC*` / `Temperature*` rows in
    `system.asynchronous_metrics`) is equally hardware-dependent: on a runner
    without those sensors the rows are absent regardless of the patch.
- Why it does not fit `§7(b)` `no_justified`: it is a runtime change to metric
  collection, not build-system-only / a config rename, and no existing upstream
  test exercises the warning-suppression behavior.
- Disposition: human-approved `test_design_blocked`. The change is a
  deliberately-carried, trivial, low-risk 2-line removal whose effect is by
  construction only visible on specific broken hardware; shipping it without an
  automated evidence pair is the correct outcome, recorded here per AGENTS §7
  ("silently shipping no test is forbidden" — this is not silent).

## 5. Rollback considerations

- Revert safety: trivial — re-add the two constructor calls. No schema, on-disk,
  or ZK state involved.
- State that survives restart: none.
- Runtime disable: `n/a` — there is no setting; the metrics are simply not
  collected. (If per-server configurability of these sensors is ever wanted,
  that would be a separate feature, not this convenience removal.)

## 6. Per-uplift notes

### 25.8-aiven (historical)

Original carry. Author: Tilman Moeller. Committer: Joe Lynch. Co-author: Kevin
Michel. No 25.X test (untestable convenience change).

### 26.3-aiven (this uplift)

- Cherry-pick: clean (`-x --no-commit`); staged diff patch-id-identical to
  source (`byte_equivalent: true`).
- Build verification: `ninja -C build clickhouse` exit 0 (incremental, 110
  steps: recompiled `Common/AsynchronousMetrics.cpp.o`, relinked
  `programs/clickhouse`; no warnings/errors — see `build/build-037.log`). The
  `build/` dir had lost `CMakeFiles/rules.ninja`, so the `cmake --fresh` recovery
  from `docs/aiven/runbooks/build-and-test.md` §2/§6 was used first (with
  `CC`/`CXX` pointed at `/opt/llvm-21`). The change removes two call sites to
  functions that remain defined and still referenced from the update loop, so no
  symbol/linkage change.
- Test added at: `n/a — test_design_blocked; see §4.`
- Anything surprising: the update-loop's `openEDAC`/`openSensorsChips` calls look
  like they would re-open the sensors and defeat the patch, but they are
  catch-only recovery paths that never fire when the vectors start empty — so the
  2-line constructor removal is sufficient. "Reads cleanly" plus "purpose
  survives" still left the patch genuinely untestable, which is why it is a
  human-approved `test_design_blocked` rather than a normal evidence-pair port.
```

