<!-- subagentStop archive | source=transcript | status=completed | type=cursorGuide | ts=2026-06-12T10:22:53Z -->

 I need to fetch the Cursor llms.txt file and check the local hooks.json to get a better sense of what I'm working with.

 I need to look up the hooks documentation.



I've read the full Cursor hooks documentation. Here are precise, doc-cited answers. The key headline up front: **the docs say hook input is delivered over stdin only — argv/env delivery is not documented anywhere**, so the `E2BIG` you're seeing is an undocumented implementation behavior, not a documented contract.

All citations below are from the official hooks reference at `https://cursor.com/docs/hooks.md` (section names given).

---

## 1. Input delivery mechanism

**Documented answer: stdin (JSON over stdio). Nothing in the docs says argv or env.**

- Top of page (intro): *"Hooks are spawned processes that **communicate over stdio using JSON in both directions**."*
- Section "Hook Types → Command-Based Hooks": *"Command hooks execute shell scripts that **receive JSON input via stdin** and return JSON output via stdout."*
- Every example script in the docs reads stdin (`json_input=$(cat)`, `json.load(sys.stdin)`, `await stdin.text()`).

There is **no documented mechanism** that passes the payload via command-line arguments or environment variables for `preToolUse` (or any command hook). The only env vars Cursor is documented to set are the fixed metadata variables in the "Environment Variables" section (`CURSOR_PROJECT_DIR`, `CURSOR_VERSION`, `CURSOR_USER_EMAIL`, `CURSOR_TRANSCRIPT_PATH`, `CURSOR_CODE_REMOTE`, `CLAUDE_PROJECT_DIR`, plus session-scoped vars from `sessionStart`) — none of which scale with file size.

So: per the docs, a large file's contents should never reach argv/env. An `E2BIG` at spawn time indicates the running build is doing something the documentation does not describe. This is worth a bug report to Cursor with your repro.

## 2. Payload contents for `preToolUse` (Write/Edit)

**The documented `preToolUse` schema is generic and the only worked example is for `Shell`. There is no documented Write/Edit-specific `tool_input` schema.**

From section "Hook events → preToolUse", the documented input is:

```json
{
  "tool_name": "Shell",
  "tool_input": { "command": "npm install", "working_directory": "/project" },
  "tool_use_id": "abc123",
  "cwd": "/project",
  "model": "claude-sonnet-4-20250514",
  "agent_message": "Installing dependencies..."
}
```

Plus the common base fields from "Common schema → Input (all hooks)": `conversation_id`, `generation_id`, `model`, `hook_event_name`, `cursor_version`, `workspace_roots`, `user_email`, `transcript_path`.

Key precise points:

- `tool_input` is documented as an **opaque, tool-specific object**. The docs only show its shape for `Shell` (`command`, `working_directory`). **The contents of `tool_input` for `Write` or `Edit` are not documented at all.**
- Therefore the docs **do not confirm or deny** whether `preToolUse` for a Write carries full file contents, `new_string`/`old_string`, or new file content. Based on the analogous (but *different*) hooks, those fields scale with size: `afterFileEdit` carries `edits: [{ old_string, new_string }]`, and `beforeReadFile` explicitly carries `content: "<file contents>"`. But those are separate hooks; you cannot rely on them as the `preToolUse` schema.
- Your empirical evidence (the payload only blows up for files >128 KB) is the real signal here: it means the `preToolUse` `tool_input` for Write/Edit *does* contain a size-scaling field (full content or new_string), even though the docs don't spell it out.

Two documentation caveats relevant to your config:

- The matcher value **`Edit` is not in the documented list**. Section "Matcher Configuration → Available matchers by hook" lists for `preToolUse`: *"Values include `Shell`, `Read`, `Write`, `Grep`, `Delete`, `Task`, and MCP tools using the `MCP: ` format."* `Edit` is not listed (`afterFileEdit` uses `Write`/`TabWrite`). So your `"Write|Edit"` matcher's `Edit` alternative may not match anything documented — worth verifying it does what you expect.
- The per-script table lists `matcher` with type `object`, but every example (and your config) uses a regex **string**. The docs are internally inconsistent here; the string form is what's shown working.

## 3. Is delivery configurable?

**No. There is no documented option to change how input is delivered, force stdin-only, or exclude/limit large fields.**

The complete documented surface is in "Configuration → Per-Script Configuration Options":

| Option | Purpose |
| --- | --- |
| `command` | script path/command |
| `type` | `"command"` or `"prompt"` |
| `timeout` | seconds |
| `loop_limit` | for stop/subagentStop |
| `failClosed` | block on failure |
| `matcher` | filter which calls trigger the hook |

There is **no** field to truncate payloads, drop file contents, switch transport, or cap input size. The only documented lever that reduces *how often* the hook fires (and thus avoids the big-payload calls) is `matcher` — but it can only filter by tool type, not by file size. So there is no documented configuration workaround for the `E2BIG`.

## 4. `failClosed` semantics for `preToolUse`

From "Per-Script Configuration Options": 

> `failClosed` (default `false`): *"When `true`, hook failures (**crash, timeout, invalid JSON**) **block the action** instead of allowing it through."*

And from "Hook Types → Command-Based Hooks → Exit code behavior": *"Other exit codes - Hook failed, action proceeds (**fail-open by default**)."*

Applied to your case:

- **`failClosed: true`** (your current setting): a hook failure blocks the tool call. A `spawn E2BIG` is a hook failure (the process never runs / never returns valid JSON), so per the documented semantics the Write/Edit **is blocked**. That matches what you'd observe: large-file writes get denied.
- **`failClosed: false`**: the failure is fail-open, so the Write/Edit would **proceed** despite the spawn error.

Important precision: the docs enumerate failure causes as *"crash, timeout, invalid JSON."* A **spawn-time failure (`E2BIG`) is not explicitly listed.** It's reasonable to treat it as a "crash"/failure, and your observed blocking behavior is consistent with that — but the doc does not *explicitly* state that spawn errors are covered by `failClosed`. So: behavior is consistent with docs, not literally spelled out.

## 5. Documented size limits / large-file guidance

**None.** There is no documented size limit for hook input payloads, no mention of file-size thresholds, and no guidance about large files anywhere in the hooks documentation. The "Troubleshooting" section covers only path/reload issues and exit-code 2 blocking. So there is nothing authoritative to cite on limits.

---

## Bottom line for your bug

- Per docs, input **should** arrive on stdin only (Q1), so a size-scaling field landing in argv/env is **undocumented behavior** — strong candidate for a Cursor bug report with your repro.
- The docs **don't define** the Write/Edit `tool_input` shape (Q2), but your >128 KB threshold proves it carries content/`new_string`.
- There is **no documented knob** to change transport or trim the payload (Q3), and **no documented size limit** (Q5).
- Your `failClosed: true` blocking large writes is **consistent with** the documented fail-closed semantics, though spawn errors aren't explicitly named (Q4). If you want large writes to go through while you wait for a fix, the only documented change is flipping that hook to `failClosed: false` — at the cost of losing enforcement on exactly the calls that fail to spawn.

A practical interim note (not from docs, clearly flagged as my reasoning): since you can't control transport, the only doc-supported mitigations are narrowing the `matcher` or accepting fail-open; neither truly fixes the root cause, which lives in Cursor's spawn path.

 I've got the complete hooks documentation now, so I can address the user's questions directly. I notice there's a discrepancy in the docs—the matcher field is described as "object" type, but the examples show it being used as a string. Let me work through each question carefully, starting with how input is delivered to hooks.

The documentation explicitly states that hooks are spawned processes communicating over stdio using JSON in both directions, and command hooks receive JSON input via stdin and return JSON output via stdout. There's no documentation for argv or environment variable delivery—that would diverge from the documented behavior.

For the payload contents in preToolUse, the documented input schema includes tool_name, tool_input, tool_use_id, cwd, model, agent_message, plus common fields like conversation_id and generation_id. However, the tool_input for Write operations isn't explicitly documented—only a Shell example is shown. I also notice that "Edit" isn't listed in the matcher values (which are Shell, Read, Write, Grep, Delete, Task, MCP), so that's worth flagging. The afterFileEdit hook does document edits with old_string and new_string fields, but preToolUse Write's tool_input structure isn't specified, and there's no documentation for file-size-scaling fields.

Regarding configurability, there's no documented option to control the delivery mechanism or exclude large fields. The only per-script options are command, type, timeout, loop_limit, failClosed, and matcher.

For failClosed semantics, the docs state it defaults to false, and when true, hook failures like crashes, timeouts, or invalid JSON block the action instead of allowing it through.

Regarding the E2BIG spawn error—the docs list "crash, timeout, invalid JSON" as failures, but don't explicitly mention spawn errors. A spawn failure could arguably be considered a crash, so with failClosed:true it would likely block the action, though this isn't explicitly documented. The key point is that argv/env delivery itself contradicts the documented stdin-only behavior, suggesting this is an undocumented implementation detail or bug.

I should also flag the matcher field inconsistency: the docs say it's type "object" but the examples and config use a string regex like "Write|Edit". Additionally, "Edit" isn't in the documented matcher values for preToolUse (Shell, Read, Write, Grep, Delete, Task, MCP), which is worth noting.

I've completed the research. The full answer is above, with all findings cited to the official hooks reference (`https://cursor.com/docs/hooks.md`). The most important takeaway for your bug: **the documentation only ever describes stdin delivery**, so the `E2BIG` from large file contents reaching argv/env is undocumented behavior and a strong candidate for a Cursor bug report.
