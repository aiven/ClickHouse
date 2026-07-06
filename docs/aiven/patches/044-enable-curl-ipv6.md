# Patch 044 — enable-curl-ipv6

## 0. Lineage

| LTS uplift | First-carry commit on aiven branch | Ported by | Outcome |
|---|---|---|---|
| 25.3-aiven | `630fd5bb0c` | Aliaksei Khatskevich, 2025-10-21 | already a no-op — `curl_config.h` at its parent already `#define ENABLE_IPV6` (see §2) |
| 25.8-aiven | `b8e3cdd8ea` | Tilman Moeller (author) / Joe Lynch (committer) | (the version we are porting FROM) — also a no-op |
| 26.3-aiven | `patch-drop(044)` | parent agent, 2026-05-31 | ineffective-no-op — drop; see §2 |

The drop was committed as `patch-drop(044)` (no code carried; reason in §2).
Find it with `git log --grep '^patch-drop(044)'`.

## 1. Purpose

Per the source commit body, the intent was to make curl's IPv6 support a
build-time toggle:

> Enable curl ipv6
>
> Make IPv6 support in curl configurable via CMake option, enabled by default.
> Previously, IPv6 support was hardcoded in curl_config.h, making it impossible
> to disable IPv6 at build time. Add a CMake option `ENABLE_IPV6` that controls
> IPv6 support in curl, with a default value of 1 (enabled). This maintains the
> current behavior while providing the flexibility to disable IPv6 ...

The change (`b8e3cdd8ea`, 1 file) adds to `contrib/curl-cmake/CMakeLists.txt`:

```cmake
option (ENABLE_IPV6 "Enable IPv6" 1)         # after the ENABLE_CURL option
...
target_compile_definitions (_curl PRIVATE
    ...
    ENABLE_IPV6                              # added, UNCONDITIONALLY
    ...)
```

Source SHA on `v25.8.18.1-lts-aiven`: `b8e3cdd8ea`.
Original author: `tilman.moeller@aiven.io`.

## 2. Upstream-drift / validity findings

> Mandatory section. Verify the patch is still SEMANTICALLY needed AND that it
> actually achieves its stated purpose against `v26.3.10.62-lts`.

### Commands run

```bash
# How does curl gate IPv6 internally?
sed -n '127,131p' contrib/curl/lib/curl_setup.h
# → /* Compatibility */
#   #ifdef ENABLE_IPV6
#   #define USE_IPV6 1
#   #endif
#   So ENABLE_IPV6 is the gate; defining it turns USE_IPV6 on.

# Is ENABLE_IPV6 already defined in the hand-written curl_config.h on each base?
git show v25.8.18.1-lts:contrib/curl-cmake/curl_config.h | grep -n IPV6
# → 56:#define ENABLE_IPV6
git show v26.3.10.62-lts:contrib/curl-cmake/curl_config.h | grep -n IPV6
# → 58:#define ENABLE_IPV6

# Who added that define, and is it upstream / already in 26.3?
git log -1 -S '#define ENABLE_IPV6' -- contrib/curl-cmake/curl_config.h --format='%H %ad %an  %s'
# → 9251fb803a  Mon Feb 10 2020  Ivan  Enable OpenSSL support in Curl (#9039)
git merge-base --is-ancestor 9251fb803a v26.3.10.62-lts && echo IN_26.3
# → IN_26.3

# Cross-LTS: was the same patch already a no-op on the PREVIOUS LTS (25.3-aiven)?
git show 630fd5bb0c^:contrib/curl-cmake/curl_config.h | grep -n IPV6
# → 57:#define ENABLE_IPV6   (already defined at the patch's PARENT on 25.3-aiven)
git merge-base --is-ancestor 9251fb803a v25.3.14.14-lts && echo IN_25.3_base
# → IN_25.3_base
```

Full logs under `tmp/patch-044/`.

### Findings

- **IPv6 is already enabled on the base — and has been since 2020.** Both LTS
  bases ship `#define ENABLE_IPV6` in the hand-written
  `contrib/curl-cmake/curl_config.h` (which is consumed because the build sets
  `HAVE_CONFIG_H`). `contrib/curl/lib/curl_setup.h` maps
  `#ifdef ENABLE_IPV6 → #define USE_IPV6 1`. The define was added by upstream
  commit `9251fb803a` (2020), an ancestor of both `v25.8.18.1-lts` and
  `v26.3.10.62-lts`.
- **The patch does not achieve its stated purpose.** Its goal is to make IPv6
  *disableable*. But:
  1. `option (ENABLE_IPV6 "Enable IPv6" 1)` declares a cache variable that
     **nothing in the build ever reads** — there is no `if (ENABLE_IPV6)` and no
     generator expression keyed on `${ENABLE_IPV6}`. The option is dead.
  2. `ENABLE_IPV6` is added to `target_compile_definitions` **unconditionally**
     (not gated on the option), so `-DENABLE_IPV6=OFF` on the CMake line would
     not remove it.
  3. The hardcoded `#define ENABLE_IPV6` in `curl_config.h` is **not removed**,
     so even a gated compile-define could not turn IPv6 off.
- **The added compile-define is redundant** with `curl_config.h:58`. Worse, the
  compiler-level `-DENABLE_IPV6` expands to `#define ENABLE_IPV6 1` while
  `curl_config.h` does a bare `#define ENABLE_IPV6` (empty), so the two differ in
  replacement text and can trigger `-Wmacro-redefined`.
- **Net effect on 26.3 (and 25.8): a no-op.** IPv6 was enabled and
  un-disableable before the patch and remains so after it. The patch carries
  dead config plus a redefinition-warning risk for zero behavioral benefit.
- **Cross-LTS confirmation — it never worked, on any line.** The same change
  was first carried on `25.3-aiven` as `630fd5bb0c` ("Enable curl ipv6",
  Aliaksei Khatskevich, 2025-10-21). Inspecting that commit's PARENT shows
  `curl_config.h:57` already had `#define ENABLE_IPV6`, so the 25.3 carry was a
  no-op too. The upstream define `9251fb803a` (2020) is an ancestor of the 25.3,
  25.8 and 26.3 bases alike. So the patch has been an ineffective no-op across
  all three LTS lines, re-carried by two different authors without anyone
  noticing — strong evidence the drop loses nothing.
- **Control-group proof.** An earlier 25.3 release, `v25.3.6.56-lts-aiven`
  (tip `34737bcefe8`), does NOT carry the patch at all — its
  `contrib/curl-cmake/CMakeLists.txt` has no `ENABLE_IPV6` option — yet its
  `curl_config.h:53` still has `#define ENABLE_IPV6`. A build that never had the
  patch therefore has IPv6 enabled exactly the same way. The patch's
  presence/absence is behaviorally indistinguishable: the definition of a no-op.
- Conclusion: **`ineffective-no-op`** — drop. This differs from 007
  (`irrelevant-by-removal`: feature deleted upstream) and 043
  (`superseded-by-upstream-equivalent`: upstream landed the same *effective*
  change). Here the patch never delivered its intended effect on either LTS; the
  base already provides "IPv6 on" via a long-standing upstream define.

### If the real requirement is to *disable* IPv6 at build time

Out of scope for a faithful port, and no evidence Aiven needs it (default is
ON). The correct change would be a **two-file `contrib/curl-cmake/` rewrite**:
make `curl_config.h`'s `#define ENABLE_IPV6` conditional (e.g. `#cmakedefine`
driven by the option) **and** gate the compile-define on `if (ENABLE_IPV6)`.
That should be raised as a product decision, not carried as this no-op patch.

## 3. C++ review

`n/a — cmake/build-system change, no C++.` The only relevant dimension is §7
(upstream/vendored ownership): the file is `contrib/curl-cmake/CMakeLists.txt`,
an upstream-owned path the `deny-upstream-file-writes.sh` hook denies — moot
here, since no change is carried.

## 4. Test design

(c) **No new test — patch is being proposed for DROP, not port.**

- Existing test path: `n/a`. The change is a build-time macro; no runtime test
  exercises IPv6-vs-not, and there is no behavioral delta to test (IPv6 is
  enabled on both sides).
- Why no test is warranted: there is no observable difference between "with" and
  "without" the patch — the base already defines `ENABLE_IPV6`. A
  fails-before/passes-after pair is impossible.

## 5. Rollback considerations

- Revert safety: `n/a` — no code carried by the drop.
- If a future curl bump ever *removes* `#define ENABLE_IPV6` from
  `curl_config.h`, IPv6 would silently turn off and this analysis must be
  revisited (the next uplift's drift check — the `curl_config.h` grep in §2 —
  will surface that).

## 6. Per-uplift notes

### 25.3-aiven (historical)

First carry: `630fd5bb0c` ("Enable curl ipv6", Aliaksei Khatskevich,
2025-10-21). Already a no-op when authored — `curl_config.h` at its parent
already defined `ENABLE_IPV6` (line 57). Same dead-option + redundant-define
shape as the 25.8 carry.

### 25.8-aiven (historical)

Original carry. Author: Tilman Moeller. Committer: Joe Lynch. The patch was
already a no-op when authored: `curl_config.h` defined `ENABLE_IPV6` (since the
2020 upstream commit), so the added option/define changed nothing.

### 26.3-aiven (this uplift)

- Cherry-pick was: NOT performed. Parent stopped at Step 1 (drift/validity
  analysis) with conclusion `ineffective-no-op`.
- Test added at: `n/a — no source change in this dispatch; see §4.`
- Anything surprising: first observed `ineffective-no-op` drop — a patch that
  applies cleanly and looks like a tidy build toggle but does not achieve its
  stated purpose because the mechanism it adds (a dead CMake option + a
  redundant, ungated compile-define) cannot override the long-standing upstream
  hardcode it was meant to make configurable. The tell was tracing the macro:
  `option` → (unused) and `curl_setup.h`'s `ENABLE_IPV6 → USE_IPV6` mapping
  against the already-present `curl_config.h` define. "Applies cleanly" said
  nothing about "works".
```

