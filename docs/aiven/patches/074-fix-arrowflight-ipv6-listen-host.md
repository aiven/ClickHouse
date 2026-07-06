# Patch 074 — fix-arrowflight-ipv6-listen-host

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.8-aiven | `ab1fb1df41` (author Vitaly Baranov `vitlibar@clickhouse.com`; committed by Joe Lynch, 2025-09-04) | cherry-pick of an upstream commit | (the version we are porting FROM) |
| 26.3-aiven | — (no commit) | parent agent, 2026-06-11 | **`obsoleted-by-upstream`** — dropped; see §2 |

## 1. Purpose (of the original 25.8 patch)

`ab1fb1df41` is **not an Aiven-authored change**. It is the upstream ClickHouse
commit *"Fix ArrowFlight support for IPv6 in `listen_host`"* by Vitaly Baranov,
which Aiven cherry-picked onto the 25.8-aiven branch (committer Joe Lynch) because
the 25.8 base predated it.

The change reworks `src/Server/ArrowFlightHandler.cpp` (and drops one declaration
from `ArrowFlightHandler.h`) so that the listening address is converted to an
`arrow::flight::Location` correctly for IPv6: `arrow::flight::Location::ForGrpc*()`
builds a URL, which requires an IPv6 literal to be wrapped in brackets. The new
`addressToArrowLocation` helper produces the bracketed `host_component`:

```cpp
String host_component =
    (ip_to_listen.family() == Poco::Net::AddressFamily::IPv6)
        ? ("[" + ip_to_listen.toString() + "]")
        : ip_to_listen.toString();
```

Without it, an ArrowFlight server bound to an IPv6 `listen_host` builds a malformed
gRPC location.

## 2. Upstream-drift / validity findings — `obsoleted-by-upstream`

> Mandatory section. Verify the patch is still SEMANTICALLY needed against
> `v26.3.10.62-lts`.

The patch is the **same upstream commit** that already lives in the 26.3 base:

- Base `v26.3.10.62-lts` contains commit `12b0084e3e8` — *"Fix ArrowFlight support
  for IPv6 in `listen_host`."*, **identical author** (Vitaly Baranov
  `vitlibar@clickhouse.com`), **identical timestamp** (`Thu Sep 4 09:41:23 2025
  +0200`), **identical diffstat** (`ArrowFlightHandler.cpp` +30/-20,
  `ArrowFlightHandler.h` -1). `git merge-base --is-ancestor 12b0084e3e8
  v26.3.10.62-lts` confirms it is reachable from the base tag.
- The base `ArrowFlightHandler.cpp` already carries the post-image: the
  `addressToArrowLocation(const Poco::Net::SocketAddress &, bool use_tls)` helper and
  the bracketed-IPv6 `host_component` line are present verbatim.

`ab1fb1df41` (the Aiven cherry-pick) and `12b0084e3e8` (the native base copy) are the
same change. The `git apply` of the 25.8 cherry no longer applies (forward or
reverse) only because of unrelated downstream drift in the same file (e.g.
`1eb9be266aa` *"Mute `clang-analyzer-deadcode.DeadStores`"* and comment cleanups) —
not because the fix is missing. The IPv6 behavior is fully present.

### Conclusion

Re-carrying 074 onto 26.3 would re-apply a commit the base already contains:
a no-op at best, a conflict-generating duplicate at worst. Disposition:
**`obsoleted-by-upstream`**, dropped (no code, no commit). Mirrors the 027/028/017
pattern — an upstream-authored fix Aiven only carried because the older base lacked
it.

## 3. C++ / security review

No code carried. No security implication: the bracketed-IPv6 URL construction in
`addressToArrowLocation` is already the base behavior, so the IPv6 `listen_host`
correctness this patch provided is preserved by the base itself.

## 4. Test design

None — there is no code change. Obsolescence is established by commit identity
(same author/timestamp/diffstat) plus `git merge-base --is-ancestor 12b0084e3e8
v26.3.10.62-lts`, and by the post-image (`addressToArrowLocation` + bracketed
`host_component`) being present in the base `ArrowFlightHandler.cpp`.

## 5. Rollback considerations

N/A — nothing applied.

## 6. Per-uplift notes

### 25.8-aiven (historical)

Source `ab1fb1df41`: a cherry-pick of upstream `12b0084e3e8` (author Vitaly Baranov),
committed by Joe Lynch on 2025-09-04 because the 25.8 base predated the upstream fix.

### 26.3-aiven (this uplift)

- `obsoleted-by-upstream`: dropped, no commit. The identical upstream commit
  `12b0084e3e8` is already an ancestor of base `v26.3.10.62-lts`; the IPv6
  `listen_host` fix is present in the base `ArrowFlightHandler.cpp`.
- This patch is unrelated to the Aiven `REGISTER_*` engine/flag family
  (045/051/052/070/071/075); it was grouped with 075 only by the shared word
  "ArrowFlight".
