# Patch 027 — fix-uncaught-exception-s3-storage

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `2f052691db` | Tilman Moeller (author), co-authored by Kevin Michel, 2025-12-18 | (the version we are porting FROM) |
| 26.3-aiven | — (no commit) | parent agent, 2026-06-03 | **`obsoleted-by-upstream`** — dropped; see §2 |

## 1. Purpose (of the original 25.8 patch)

In `IDisk::copyFile`, if an exception was thrown while `copyData` ran, the explicit
`out->finalize()` was skipped. `finalize` then happened in the `out` destructor, and
a throwing `finalize` inside a destructor terminated the whole server. The 25.8 fix
wrapped `copyData` in a `try/catch` that logged, called `out->finalize()`, and
rethrew:

```cpp
try
{
    copyData(*in, *out, cancellation_hook);
}
catch (...)
{
    tryLogCurrentException(__PRETTY_FUNCTION__);
    out->finalize();
    throw;
}
out->finalize();
```

## 2. Upstream-drift / validity findings — `obsoleted-by-upstream`

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

26.3 redesigned the `WriteBuffer` lifecycle into an explicit finalize-or-cancel
contract with unwinding-tolerant destructors. The crash the patch fixed cannot
occur, and the patch's mechanism is now actively wrong.

1. **The base `~WriteBuffer()` no longer finalizes in the destructor.** It only
   `chassert`s when destructed un-finalized/un-canceled **and** not during stack
   unwinding (`src/IO/WriteBuffer.cpp:21`):

   ```cpp
   WriteBuffer::~WriteBuffer()
   {
       if (!finalized && !canceled && !isStackUnwinding())
       {
           LOG_ERROR(...);
           chassert(false && "WriteBuffer is neither finalized nor canceled in destructor.");
       }
   }
   ```

   `isStackUnwinding()` is `exception_level < std::uncaught_exceptions()` where
   `exception_level` is captured at construction (`src/IO/WriteBuffer.h:148-153`).
   When `copyData` throws, `out` is destructed while the exception is in flight, so
   `isStackUnwinding()` is true and the assert is skipped — no throw, no crash, in
   both release and debug.

2. **The derived `~WriteBufferFromS3()` aborts the partial upload itself**, noexcept
   (`src/IO/WriteBufferFromS3.cpp:314-318`):

   ```cpp
   if (!canceled && !multipart_upload_id.empty() && !multipart_upload_finished)
   {
       LOG_WARNING(log, "WriteBufferFromS3 was neither finished nor aborted, try to abort upload in destructor. {}.", ...);
       tryToAbortMultipartUpload();
   }
   ```

   So a failed S3 copy on 26.3 already (a) does not crash and (b) cleanly aborts the
   dangling multipart upload.

3. **A faithful port would be a regression.** The patch's `catch` calls
   `out->finalize()`. On 26.3, `finalize` on a `WriteBufferFromS3` *completes* the
   multipart upload with whatever bytes were written — committing a truncated /
   corrupt object on the error path. The 26.3 error-path primitive is `cancel()`
   (noexcept, aborts), not `finalize()`. Carrying 027 verbatim would persist partial
   data.

The current 26.3 `IDisk::copyFile` (`src/Disks/IDisk.cpp:61-77`) is byte-for-byte the
pre-patch version and is already safe given the lifecycle above.

## 3. C++ / security review

No code carried. The decision rests on the upstream lifecycle redesign: explicit
finalize/cancel, destructor logging instead of finalize, `isStackUnwinding()`
tolerance, and `tryToAbortMultipartUpload` on the S3 buffer's error path. These
collectively subsume the Aiven fix and handle the abort more correctly than the
original (which committed partial data).

## 4. Test design

None. There is no code change. The obsolescence is established by code analysis of
the `WriteBuffer` / `WriteBufferFromS3` destructors and `isStackUnwinding`. A
regression test was considered (proving an S3 copy failure does not crash on 26.3)
but deferred: deterministically forcing a mid-`copyData` failure on the disk copy
path requires non-trivial fault injection, and the no-crash guarantee is already
enforced structurally by the destructor contract (a debug `chassert` would fire
loudly if the contract regressed).

## 5. Rollback considerations

N/A — nothing applied.

## 6. Per-uplift notes

### 25.8-aiven (historical)

Source `2f052691db` (author Tilman Moeller, co-authored by Kevin Michel,
2025-12-18): the `try/catch` + `finalize` + rethrow in `IDisk::copyFile`, fixing a
server-terminating throwing-finalize-in-destructor on the (then-current) lifecycle.

### 26.3-aiven (this uplift)

- `obsoleted-by-upstream`: dropped, no commit. The 26.3 `WriteBuffer` lifecycle
  prevents the crash and aborts partial uploads; a verbatim port would commit partial
  data.
- Decision ratified by the maintainer (drop the code, document only).
