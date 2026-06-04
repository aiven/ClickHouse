# 26.3 uplift work log

Appended automatically by the subagentStop hook. Human-edit only to add
commentary rows. The Report column links to the verbatim worker summary
archived under `reports/`; rows with `n/a` either had no parseable
summary (read-only subagents like cursorGuide / explore) or predate one
of the two hook fixes that landed in this uplift:

- `8069f64f030` (T3.4 retrospective Finding A): first hook fix; started
  parsing the YAML report body from `.agent_transcript_path` instead of
  the prose-only `.summary`. The T3.4 row was backfilled manually
  (`backfilled=true` in its archived report); rows from `T3.5` onward
  were auto-populated by this fix.
- `<bootstrap-hook-fix-2026-05-26>` (T3.6 retrospective Finding G): second
  hook fix after Cursor silently changed the JSON shape so that
  `.agent_transcript_path` became `null` and `.transcript_path` was
  redirected to the PARENT's transcript. The fix derives the subagent's
  own JSONL via `dirname(.transcript_path)/subagents/<youngest>.jsonl`
  and concatenates text from ALL assistant turns (workers sometimes
  emit a wrap-up turn after the YAML). The T3.5
  (`fix-compatibility-setting-crash-on-removed-setting`) and T3.6
  (`replicated-database-attach-with-shard-macro`) rows show the
  intermediate regression: T3.5 auto-populated correctly under the first
  fix; T3.6 had its YAML fields manually backfilled from parent inspection
  of the on-disk subagent transcripts (Report=`n/a` reflects the original
  auto-population state). Later rows are auto-populated by the second fix.
- **Hook v3 third regression (T3.8-T3.15, investigated 2026-05-27/28):**
  intermittent `unknown / unknown` rows landed during T3.8-T3.15
  despite the v3 hook being healthy under synthetic input. The probe
  block at `.cursor/hooks/log-subagent-completion.sh` (landed in
  `acb4d88fc70`, since removed) captured raw `$input` to
  `tmp/hook-probe/` on six real dispatches. Every captured payload
  showed `message_count: 0`, `tool_call_count: 0`, and no assistant
  content — confirming **Cursor never sends the assistant transcript
  content in the `subagentStop` hook input**. The v3 fallback (read
  the on-disk subagent JSONL directly via
  `dirname(.transcript_path)/subagents/<youngest>.jsonl`) is therefore
  the permanent path, not a workaround. The intermittent misses
  happen when the JSONL file isn't fully flushed by the time the hook
  fires. The T3.13/T3.14/T3.15 rows (the `refreshable-mv-shard-macro-expansion`
  and `zk-node-leak-after-create-delete-table` dispatches) were backfilled
  offline from the on-disk JSONLs using the same jq pipeline the hook uses;
  their report archives carry `backfilled=true` and cite the source
  JSONL UUID in the archive header.

**Authoritative record.** The worker rows for T3.1–T3.3 and T3.8–T3.12 were
never auto-captured (the hook regressions above) and are not reconstructed
here; the `unknown`-slug rows below (the early dispatches, by timestamp +
subagent type) are their residual traces. For *what those dispatches did*,
the per-dispatch retrospectives in this directory are the authoritative
record — every T-number T3.1–T3.15 has one. Any `unknown` row can still be
backfilled from its on-disk subagent transcript
(`agent-transcripts/<parent>/subagents/`) if a precise row is ever needed.

| Timestamp (UTC) | Status | Patch slug | Subagent type | Subagent id | Outcome | Escalation reason | Report |
|---|---|---|---|---|---|---|---|
| 2026-05-21T15:02:50Z | n/a | unknown | cursorGuide | toolu_01Lz4wj9M5SYzVTEgbD41kY6 | unknown | n/a | n/a |
| 2026-05-21T15:31:00Z | n/a | unknown | explore | toolu_011LmVHT3yUxqHRv2kjWwV1b | unknown | n/a | n/a |
| 2026-05-22T12:24:05Z | n/a | unknown | explore | toolu_019bjhJRVovPjShrqdjWCitE | unknown | n/a | n/a |
| 2026-05-22T13:03:52Z | n/a | unknown | explore | toolu_01ApNc7r72hpFYpxdFumFkMD | unknown | n/a | n/a |
| 2026-05-22T13:51:17Z | n/a | unknown | general-purpose | toolu_01HWaaRkwWQsfkDiRAJ9aXC9 | unknown | n/a | n/a |
| 2026-05-23T21:18:03Z | n/a | unknown | general-purpose | toolu_01AW4UYewkfSoqD1soEB7JCk | unknown | n/a | n/a |
| 2026-05-24T19:02:06Z | n/a | unknown | cursorGuide | toolu_01MJH3YEWnmEu7XKTiT9yt8F | unknown | n/a | n/a |
| 2026-05-24T19:54:31Z | completed | unknown | general-purpose | toolu_01VZ3EE86DPCQsaaebE5P7Ye | unknown | n/a | n/a |
| 2026-05-25T11:09:51Z | completed | hide-secrets-system-mutations-command | general-purpose | toolu_01LpNLkS2cPr4HQjgmvDdTKf | success | none | [report](reports/toolu_01LpNLkS2cPr4HQjgmvDdTKf.md) |
| 2026-05-25T13:53:56Z | completed | fix-compatibility-setting-crash-on-removed-setting | general-purpose | toolu_01L8EruASfYWFnjX7keDHkXg | escalate | test_design_blocked | n/a |
| 2026-05-26T12:08:46Z | completed | replicated-database-attach-with-shard-macro | general-purpose | toolu_01Hr5eGvi4VS8h2bRy6ahWu4 | escalate | test_design_blocked | n/a |
| 2026-05-26T15:22:38Z | completed | default-logs-to-keep | general-purpose | toolu_01FgRHtjZGkmh4h6Z7DZaceE | success | none | [report](reports/toolu_01FgRHtjZGkmh4h6Z7DZaceE.md) |
| 2026-05-28T11:45:49Z | completed | refreshable-mv-shard-macro-expansion | general-purpose | toolu_017G6Sxog7w15j7wi61nPbj6 | escalate | test_design_blocked | [report](reports/toolu_017G6Sxog7w15j7wi61nPbj6.md) |
| 2026-05-28T12:36:32Z | completed | refreshable-mv-shard-macro-expansion | general-purpose | toolu_01Ngku5yMGP4YVcVxAQ3ND3G | success | none | [report](reports/toolu_01Ngku5yMGP4YVcVxAQ3ND3G.md) |
| 2026-05-28T13:17:30Z | completed | zk-node-leak-after-create-delete-table | general-purpose | toolu_01FigZuPjPKDHr5pMUFgpoQ6 | success | none | [report](reports/toolu_01FigZuPjPKDHr5pMUFgpoQ6.md) |
| 2026-05-29T12:05:39Z | completed | unknown | explore | toolu_01D63axzozcvEmqxwJyYYfpE | unknown | n/a | [report](reports/toolu_01D63axzozcvEmqxwJyYYfpE.md) |
| 2026-05-31T09:39:29Z | completed | unknown | general-purpose | toolu_01PL7kpuZYVhVPrR7s7DeSyg | unknown | n/a | n/a |
| 2026-05-31T10:33:55Z | completed | unknown | general-purpose | toolu_01SmV6ER5SPtqizNtYNKaxHw | unknown | n/a | n/a |
| 2026-05-31T11:03:10Z | completed | unknown | general-purpose | toolu_01LaFLhPq7itDzkEZhcvMLFd | unknown | n/a | n/a |
| 2026-06-01T14:53:17Z | completed | unknown | general-purpose | toolu_01LeJHyiLuTEzbTMgqKQ4rcE | unknown | n/a | n/a |
| 2026-06-02T09:01:26Z | completed | azure-signature-delegation | general-purpose | toolu_01CUwgrfbb7eMnpxkv743qE6 | escalate | policy_call | [report](reports/toolu_01CUwgrfbb7eMnpxkv743qE6.md) |
| 2026-06-02T09:23:11Z | completed | azure-signature-delegation | general-purpose | toolu_01VAiwsBbJWpYGJXvrxXLsuX | escalate | policy_call | [report](reports/toolu_01VAiwsBbJWpYGJXvrxXLsuX.md) |
| 2026-06-02T11:34:23Z | completed | unknown | general-purpose | toolu_01JR51dqrXB6kUuN5MiZ2iwA | unknown | n/a | n/a |
| 2026-06-02T12:27:59Z | completed | unknown | explore | toolu_01FTv3qN3GQCBZWT7rfocJrC | unknown | n/a | [report](reports/toolu_01FTv3qN3GQCBZWT7rfocJrC.md) |
| 2026-06-02T13:39:44Z | completed | unknown | general-purpose | toolu_0135XHqfYq7U66mmBzRkpsU2 | unknown | n/a | n/a |
| 2026-06-02T14:52:06Z | completed | unknown | shell | toolu_01Mts54ayp5Ww4uQRRVkWUKc | unknown | n/a | n/a |
| 2026-06-02T15:04:08Z | completed | unknown | shell | toolu_011q2ceBLW45XT38Zk4jNDtc | unknown | n/a | n/a |
| 2026-06-03T07:22:27Z | completed | unknown | shell | toolu_01W5xygCmCtYqwuqzmh5hpAQ | unknown | n/a | n/a |
| 2026-06-03T08:18:14Z | completed | unknown | shell | toolu_01YErECqAnf1GRkm12MtYN62 | unknown | n/a | n/a |
| 2026-06-03T08:33:36Z | completed | unknown | explore | toolu_01U8wvwvaakNf6Tf64d6sv97 | unknown | n/a | n/a |
| 2026-06-03T08:48:01Z | completed | unknown | general-purpose | toolu_01XDtm6reTiEATBwrzuMVF1y | unknown | n/a | n/a |
| 2026-06-03T09:00:29Z | completed | unknown | general-purpose | toolu_019NuNh4BnLUaqqr3C5Ywvhx | unknown | n/a | [report](reports/toolu_019NuNh4BnLUaqqr3C5Ywvhx.md) |
| 2026-06-03T09:15:47Z | completed | unknown | general-purpose | toolu_01PyX3WTDVjL6RuFqNd5rbQ7 | unknown | n/a | n/a |
| 2026-06-03T09:45:07Z | completed | unknown | general-purpose | toolu_01WspJUxLLV9oK4nkRHdb1au | unknown | n/a | n/a |
| 2026-06-03T12:56:44Z | completed | unknown | shell | toolu_01CJ54TRQ6Z6chRyCWq7mEpE | unknown | n/a | [report](reports/toolu_01CJ54TRQ6Z6chRyCWq7mEpE.md) |
| 2026-06-03T13:09:21Z | completed | unknown | shell | toolu_012oLDYNLqRPJeLaGnXUUvZ3 | unknown | n/a | [report](reports/toolu_012oLDYNLqRPJeLaGnXUUvZ3.md) |
| 2026-06-03T13:27:41Z | completed | unknown | shell | toolu_01PuhZ5YFno3FHTvpUkqXaHz | unknown | n/a | [report](reports/toolu_01PuhZ5YFno3FHTvpUkqXaHz.md) |
| 2026-06-03T13:37:53Z | completed | unknown | shell | toolu_01KwsbJS3doCFwFs3SxCnnEE | unknown | n/a | [report](reports/toolu_01KwsbJS3doCFwFs3SxCnnEE.md) |
| 2026-06-03T13:42:05Z | completed | unknown | shell | toolu_01PDrSejsLmU9Yeo6w8kJB7Z | unknown | n/a | [report](reports/toolu_01PDrSejsLmU9Yeo6w8kJB7Z.md) |
| 2026-06-03T14:49:46Z | completed | unknown | general-purpose | toolu_01EdDaHc2AgUnRt1ZkcmTj7E | unknown | n/a | n/a |
| 2026-06-04T10:00:13Z | completed | unknown | general-purpose | toolu_018neBM9hQdiayvvPWTwprzG | unknown | n/a | n/a |
| 2026-06-04T10:44:30Z | completed | unknown | explore | toolu_01AoSy21m2GU24KnhmKpDk3A | unknown | n/a | n/a |
| 2026-06-04T10:53:10Z | completed | unknown | general-purpose | toolu_01Kh6GRGnQTYexxvrEXAvbM7 | unknown | n/a | n/a |
| 2026-06-04T11:15:15Z | completed | unknown | general-purpose | toolu_01VDy5oX83dCkGxWsUjgmjSm | unknown | n/a | n/a |
