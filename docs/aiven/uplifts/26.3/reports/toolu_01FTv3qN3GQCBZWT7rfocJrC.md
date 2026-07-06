<!-- subagentStop archive | source=transcript | status=completed | type=explore | ts=2026-06-02T12:27:59Z -->

I'll read the patch diff first, then compare each listed file on HEAD for anchor drift and the `setGlobalContext()` call path.

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

[REDACTED]

# Drift analysis: patch 05d8148a57 (`Allow avnadmin creating database using sql`)

Branch: `v26.3.10.62-lts-aiven-dev` at `/home/tilman.moeller/projects/ClickHouse`. None of the patch symbols exist on HEAD (`setGlobalContext`, `createReplicatedDatabaseByClient`, `DEFAULT_REPLICATED_DATABASE_PRIVILEGES`, the three server settings). This is a fresh port, not a re-apply.

---

## Security-critical call path: who calls `setGlobalContext()`?

**Only caller (in the patch):** `InterpreterCreateQuery::createReplicatedDatabaseByClient()`.

From `tmp/patch-019/source.diff`:

```cpp
auto new_context = Context::createCopy(context);
new_context->setGlobalContext();
new_context->setSetting("allow_distributed_ddl", 1);
executeQuery(create_db_query, new_context, QueryFlags{ .internal = true });
// ...
executeQuery(grant_query, new_context, QueryFlags{ .internal = true });
```

**Elevation target:** a **cloned, throwaway context**, not the session context.

- `Context::createCopy` (HEAD `src/Interpreters/Context.cpp:1298–1302`) allocates a new `Context` via copy ctor, copying `user_id`, cached `access`, settings, roles, etc.
- `setGlobalContext()` clears `user_id` and sets `need_recalculate_access = true` **only on that copy**.
- The original session `context` from `getContext()` is untouched; `createReplicatedDatabaseByClient()` reads `context->getUserName()` from the session for the GRANT target.

**Verdict:** elevation is scoped to ephemeral internal query execution, which matches the intended design.

---

## File-by-file anchor analysis

### 1. `src/Core/ServerSettings.cpp`

| Patch anchor | HEAD status |
|---|---|
| Insert 3 `DECLARE(String, …)` after `disable_insertion_and_mutation`, before `parts_kill_delay_period` | **Anchor exists; likely clean insert** |

**HEAD neighbor (lines 1137–1142):**

```1137:1142:src/Core/ServerSettings.cpp
    DECLARE(Bool, disable_insertion_and_mutation, false, R"(
    Disable insert/alter/delete queries. This setting will be enabled if someone needs read-only nodes to prevent insertion and mutation affect reading performance. Inserts into external engines (S3, DataLake, MySQL, PostrgeSQL, Kafka, etc) are allowed despite this setting.
    )", 0) \
    DECLARE(UInt64, parts_kill_delay_period, 30, R"(
    Period to completely remove parts for SharedMergeTree. Only available in ClickHouse Cloud
    )", 0) \
```

**LIST macro structure:** settings live in `LIST_OF_SERVER_SETTINGS_WITHOUT_PATH` (starts `src/Core/ServerSettings.cpp:59`), combined at `:1609–1611`. New settings belong in the WITHOUT_PATH list (same as patch).

**Drift:** description of `disable_insertion_and_mutation` expanded on HEAD (~97 lines later than patch base). Insert point unchanged.

---

### 2. `src/Databases/DatabaseReplicated.cpp` + `.h`

| Patch change | HEAD status |
|---|---|
| Add `shard_macros` ctor param + member + `getShardMacros()` | **Major signature drift** |
| Store macro string before expansion in `registerDatabaseReplicated` | **Logic anchor exists but ctor call changed** |

**HEAD constructor (lines 195–209)** — patch base had no `zookeeper_name_`, no `shard_macros_`:

```195:209:src/Databases/DatabaseReplicated.cpp
DatabaseReplicated::DatabaseReplicated(
    const String & name_,
    const String & metadata_path_,
    UUID uuid,
    const String & zookeeper_name_,
    const String & zookeeper_path_,
    const String & shard_name_,
    const String & replica_name_,
    DatabaseReplicatedSettings db_settings_,
    ContextPtr context_)
    : DatabaseAtomic(name_, metadata_path_, uuid, "DatabaseReplicated (" + name_ + ")", context_)
    , zookeeper_name(zookeeper_name_)
    , zookeeper_path(normalizeZooKeeperPath(zookeeper_path_))
    , shard_name(shard_name_)
    , replica_name(replica_name_)
```

**HEAD members (lines 245–248 in `.h`):** `zookeeper_name`, `zookeeper_path`, `shard_name`, `replica_name` — no `shard_macros`.

**HEAD `registerDatabaseReplicated` shard handling (lines 2628–2667):**

```2628:2667:src/Databases/DatabaseReplicated.cpp
        String zookeeper_path = safeGetLiteralValue<String>(arguments[0], "Replicated");
        String shard_name = safeGetLiteralValue<String>(arguments[1], "Replicated");
        String replica_name  = safeGetLiteralValue<String>(arguments[2], "Replicated");
        // ...
        auto zookeeper_name = zkutil::extractZooKeeperName(zookeeper_path);
        zookeeper_path = zkutil::extractZooKeeperPath(zookeeper_path, /*check_starts_with_slash*/false);
        // ...
        shard_name = args.context->getMacros()->expand(shard_name, info);
        // ...
        return std::make_shared<DatabaseReplicated>(
            args.database_name,
            args.metadata_path,
            args.uuid,
            zookeeper_name,
            zookeeper_path,
            shard_name,
            replica_name,
            std::move(database_replicated_settings), args.context);
```

**Drift:** must add `shard_macros` **alongside** `zookeeper_name` (store pre-expansion arg[1], expand into `shard_name`). Patch hunks will not apply cleanly.

---

### 3. `src/Interpreters/Access/InterpreterGrantQuery.cpp`

| Patch anchor | HEAD status |
|---|---|
| ~60-line block in `execute()` after grantee checks, before `AccessRights new_rights` | **Insert point exists; ordering hazard on HEAD** |
| Similar GRANT shortcut | **Nothing similar exists** |

**HEAD `execute()` tail (lines 473–484):**

```473:484:src/Interpreters/Access/InterpreterGrantQuery.cpp
    /// Check if the current user has corresponding access rights granted with grant option.
    bool need_check_grantees_are_allowed = true;
    if (!query.current_grants)
        checkGrantOption(access_control, *current_user_access, grantees, need_check_grantees_are_allowed, elements_to_grant, elements_to_revoke);

    /// Check if the current user has corresponding roles granted with admin option.
    checkAdminOption(access_control, *current_user_access, grantees, need_check_grantees_are_allowed, roles_to_grant, roles_to_revoke, query.admin_option);

    if (need_check_grantees_are_allowed)
        current_user_access->checkGranteesAreAllowed(grantees);

    AccessRights new_rights;
```

**HEAD-only code before that (lines 419–420):**

```419:420:src/Interpreters/Access/InterpreterGrantQuery.cpp
    query.replaceCurrentUserTag(getContext()->getUserName());
    query.access_rights_elements.eraseNotGrantable();
```

**Drift / risk:**
- Patch inserts **after** grantee checks; on HEAD that is still valid for the block itself.
- **Critical:** `eraseNotGrantable()` at `:420` runs first and may strip the synthetic `AccessType::ALL` element the parser creates. The patch block reads `query.access_rights_elements[0].database` — port must handle this **before** `:420` or skip erasure when `default_replicated_db_privileges` is set.
- HEAD adds TABLE ENGINE validation (`:430–440`) and `#include <Storages/StorageFactory.h>` — patch includes differ.
- Patch uses `query.grantees->names[0]`; HEAD resolves grantees to UUIDs at `:442`. Use AST grantee names or resolve earlier.
- Patch includes dead `#include "base/sleep.h"` — omit when porting.
- ON CLUSTER rejection for this statement should mirror `current_grants` at `:462–463`.

---

### 4. `src/Interpreters/Context.cpp` + `.h` (security-critical)

| Patch addition | HEAD status |
|---|---|
| `setGlobalContext()`: lock `mutex`; `user_id = {}`; `need_recalculate_access = true` | **Members exist; method absent; mechanism compatible** |

**Members on HEAD (`src/Interpreters/Context.h:362–367, 702`):**

```362:367:src/Interpreters/Context.h
    std::optional<UUID> user_id;
    // ...
    mutable std::shared_ptr<const ContextAccess> access;
    mutable bool need_recalculate_access = true;
```

```702:702:src/Interpreters/Context.h
    mutable ContextSharedMutex mutex;
```

**Model setter nearby (`src/Interpreters/Context.cpp:1932–1941`):**

```1932:1941:src/Interpreters/Context.cpp
void Context::setUserIDWithLock(const UUID & user_id_, const std::lock_guard<ContextSharedMutex> &)
{
    user_id = user_id_;
    need_recalculate_access = true;
}

void Context::setUserID(const UUID & user_id_)
{
    std::lock_guard lock(mutex);
    setUserIDWithLock(user_id_, lock);
}
```

**Declaration placement:** after `setUser` / `getUser` (`src/Interpreters/Context.h:831–834`).

**Access recalculation on HEAD (`src/Interpreters/Context.cpp:2077–2135`):**

```2082:2090:src/Interpreters/Context.cpp
        /// If setUserID() was never called then this must be the global context with the full access.
        bool full_access = !user_id;
        // ...
        return ContextAccessParams{
            user_id, full_access, /* use_default_roles= */ false, current_roles, external_roles, *settings, current_database, client_info, initial_user_id};
```

When `user_id` is empty (`std::nullopt`), `full_access = true` → `ContextAccess::initialize()` sets `AccessRights::getFullAccess()` (`src/Access/ContextAccess.cpp:329–333`).

**Verdict:** `setGlobalContext()` matches HEAD’s existing elevation model. Clearing `user_id` on a **copy** triggers recalculation on next `getAccess()` because `need_recalculate_access = true`. Session context is unaffected.

**Note:** copy ctor copies cached `access` (`Context.cpp:1205–1209`), but `need_recalculate_access = true` prevents stale reuse (`:2098–2108`).

---

### 5. `src/Interpreters/InterpreterCreateQuery.cpp` + `.h`

| Patch change | HEAD status |
|---|---|
| Extract `checkMaxDatabaseNumToThrow()` | **Inline logic still in `createDatabase`; differs slightly** |
| Add `createReplicatedDatabaseByClient()`, `checkDatabaseNameAllowed()` | **Absent** |
| Special-user path in `execute()` | **Heavy structural drift** |

**HEAD `createDatabase` still has inline limit check (`:207–229`)** with `GetDatabasesOptions{.with_datalake_catalogs = true}` and extra system DB exclusions — patch’s extracted helper used simpler `getDatabases().size()`.

**HEAD `execute()` (`:2443–2470`)** — patch inserted special-user logic **before** ON CLUSTER handling; HEAD added new ON CLUSTER gates:

```2443:2470:src/Interpreters/InterpreterCreateQuery.cpp
BlockIO InterpreterCreateQuery::execute()
{
    FunctionNameNormalizer::visit(query_ptr.get());
    auto & create = query_ptr->as<ASTCreateQuery &>();

    create.if_not_exists |= getContext()->getSettingsRef()[Setting::create_if_not_exists];

    bool is_create_database = create.database && !create.table;
    if (!create.cluster.empty() && !maybeRemoveOnCluster(query_ptr, getContext()))
    {
        if (create.attach_as_replicated.has_value())
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "ATTACH AS [NOT] REPLICATED is not supported for ON CLUSTER queries");

        auto on_cluster_version = getContext()->getSettingsRef()[Setting::distributed_ddl_entry_format_version];
        if (is_create_database || on_cluster_version < DDLLogEntry::NORMALIZE_CREATE_ON_INITIATOR_VERSION)
            return executeQueryOnCluster(create);
    }

    getContext()->checkAccess(getRequiredAccess());
    // ...
    if (is_create_database)
        return createDatabase(create);
```

**Drift:** special-user path must go **before** `:2451` ON CLUSTER block (as patch intended), and must respect new `attach_as_replicated` / `distributed_ddl_entry_format_version` behavior. `#include <Databases/DatabaseReplicated.h>` already present (`:75`).

**Private methods in `.h`:** only `createDatabase` / `createTable` at `:99–100`; patch’s three new private methods need adding.

**Error codes:** `ACCESS_DENIED` exists on HEAD (`:172`); patch’s `SETTING_CONSTRAINT_VIOLATION` and `UNSUPPORTED_PARAMETER` exist globally but are **not** declared in this file’s `ErrorCodes` block — add when porting.

---

### 6. `src/Interpreters/InterpreterDropQuery.cpp`

| Patch anchor | HEAD status |
|---|---|
| ON CLUSTER enforcement for non-`ACCESS_MANAGEMENT` database drops | **Routing anchor exists; logic absent** |
| `executeDDLQueryOnCluster(..., true)` for skip checks | **3-arg call only today** |

**HEAD drop-database path (`:96–118`):**

```96:118:src/Interpreters/InterpreterDropQuery.cpp
BlockIO InterpreterDropQuery::executeSingleDropQuery(const ASTPtr & drop_query_ptr)
{
    auto & drop = drop_query_ptr->as<ASTDropQuery &>();
    if (!drop.cluster.empty() && drop.table && !drop.if_empty && !maybeRemoveOnCluster(current_query_ptr, getContext()))
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccessForDDLOnCluster();
        return executeDDLQueryOnCluster(current_query_ptr, getContext(), params);
    }

    if (getContext()->getSettingsRef()[Setting::database_atomic_wait_for_drop_and_detach_synchronously])
        drop.sync = true;

    if (drop.table)
        return executeToTable(drop);
    if (drop.database && !drop.cluster.empty() && !maybeRemoveOnCluster(current_query_ptr, getContext()))
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccessForDDLOnCluster();
        return executeDDLQueryOnCluster(current_query_ptr, getContext(), params);
    }
    if (drop.database)
        return executeToDatabase(drop);
```

**Drift:** patch block belongs after `:107` (sync setting) and before `:109` table routing. Needs `#include <Access/ContextAccess.h>` and `#include <Core/ServerSettings.h>` (patch additions). `ACCESS_MANAGEMENT` still valid (`src/Access/Common/AccessType.h`).

---

### 7. `src/Interpreters/executeDDLQueryOnCluster.cpp` + `.h`

| Patch change | HEAD status |
|---|---|
| Add `bool skip_distributed_checks = false` parameter | **Signature unchanged; body anchors exist** |

**HEAD signature (`executeDDLQueryOnCluster.h:47`):**

```47:47:src/Interpreters/executeDDLQueryOnCluster.h
BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, ContextPtr context, const DDLQueryOnClusterParams & params = {});
```

**Patch target checks (`executeDDLQueryOnCluster.cpp`):**

```83:84:src/Interpreters/executeDDLQueryOnCluster.cpp
    if (!context->getSettingsRef()[Setting::allow_distributed_ddl])
        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Distributed DDL queries are prohibited for the user");
```

```113:114:src/Interpreters/executeDDLQueryOnCluster.cpp
    /// TODO: support per-cluster grant
    context->checkAccess(AccessType::CLUSTER);
```

**Drift:** hunks apply cleanly in spirit, but HEAD added **before** the allow_distributed_ddl check:
- Transaction guard (`:69–70`)
- `replicated_ddl_queries_enabled` check (`:87–90`)

Only the two guarded lines need wrapping with `skip_distributed_checks`.

---

### 8. Parser / AST / keywords

**`src/Parsers/CommonParsers.h`** — insert after `DEFAULT_DATABASE` (`:149`), before `DEFAULT_ROLE` (`:150`):

```149:150:src/Parsers/CommonParsers.h
    MR_MACROS(DEFAULT_DATABASE, "DEFAULT DATABASE") \
    MR_MACROS(DEFAULT_ROLE, "DEFAULT ROLE") \
```

`DEFAULT_REPLICATED_DATABASE_PRIVILEGES` absent. **Likely clean single-line insert.**

**`src/Parsers/Access/ASTGrantQuery.h:29`** — `current_grants` exists; no `default_replicated_db_privileges`. **Clean +1 field.**

**`src/Parsers/Access/ParserGrantQuery.cpp`** — **moderate drift**:

- `parseCurrentGrants` at `:22–54` is the template for patch’s `parseDefaultPrivileges`.
- HEAD parser flow (`:132–167`) adds `WITH_REPLACE_OPTION` and extra `parseOnCluster` passes **after** grantees — patch validations must be re-placed accordingly.

```132:167:src/Parsers/Access/ParserGrantQuery.cpp
    bool current_grants = false;
    if (!is_revoke && ParserKeyword{Keyword::CURRENT_GRANTS}.ignore(pos, expected))
    {
        current_grants = true;
        if (!parseCurrentGrants(pos, expected, elements))
            return false;
    }
    else
    {
        if (!parseAccessRightsElementsWithoutOptions(pos, expected, elements) && !parseRoles(pos, expected, is_revoke, attach_mode, roles))
            return false;
    }
    // ... parseToGrantees, WITH_GRANT_OPTION, WITH_REPLACE_OPTION, multiple parseOnCluster ...
```

No upstream `DEFAULT PRIVILEGES` / similar grammar on HEAD.

---

## Summary

### (a) Likely clean vs conflict

| File | Expectation |
|---|---|
| `ServerSettings.cpp` | **Clean insert** (~line 1139) |
| `CommonParsers.h` | **Clean insert** |
| `ASTGrantQuery.h` | **Clean** |
| `Context.h` / `Context.cpp` | **Mostly clean** (new method + declaration) |
| `executeDDLQueryOnCluster.h/.cpp` | **Small conflict** (4th param + 2 guarded lines; extra HEAD checks stay) |
| `InterpreterDropQuery.cpp` | **Moderate** (new block + 4th arg on one call site) |
| `ParserGrantQuery.cpp` | **Moderate** (parser flow reordered vs patch) |
| `InterpreterGrantQuery.cpp` | **Moderate** (`eraseNotGrantable` ordering) |
| `DatabaseReplicated.cpp/.h` | **High conflict** (`zookeeper_name` param; macro storage) |
| `InterpreterCreateQuery.cpp/.h` | **Highest conflict** (execute restructuring, db limit logic, new methods) |

### (b) Highest-risk drift areas

1. **`DatabaseReplicated` ctor / factory** — `zookeeper_name` split from path; must add `shard_macros` without breaking HEAD macro/ZK logic.
2. **`InterpreterCreateQuery::execute()` ordering** — new ON CLUSTER / `attach_as_replicated` / `distributed_ddl_entry_format_version` gates; special-user bypass must precede them.
3. **`InterpreterGrantQuery::execute()` + parser** — `eraseNotGrantable()` at `:420` can break synthetic ALL element; parser has extra `WITH_REPLACE_OPTION` / ON CLUSTER passes.
4. **`setGlobalContext` semantics** — mechanism is sound on HEAD, but must stay on **`createCopy` only**; verify inner `executeQuery` paths don’t leak elevated context.
5. **`checkMaxDatabaseNumToThrow`** — if extracted, align with HEAD’s datalake-aware counting, not patch’s simpler version.

### (c) Access recalculation vs `setGlobalContext`

**Compatible.** HEAD already treats empty `user_id` as `full_access` in `getAccess()` (`Context.cpp:2082–2083`), recalculates when `need_recalculate_access` is true (`:2098–2131`), and grants full rights in `ContextAccess::initialize()` (`ContextAccess.cpp:329–333`). Patch’s three-line `setGlobalContext()` is consistent with `setUserIDWithLock()`’s invalidation pattern.

### (d) Upstream GRANT / privilege changes affecting the new grammar

- **`eraseNotGrantable()`** in interpreter execute (`InterpreterGrantQuery.cpp:420`) — new grammar must bypass or run before this.
- **`elements.throwIfNotGrantable()`** in parser (`ParserGrantQuery.cpp:192`) — synthetic `AccessType::ALL` on `db.*` must remain valid through parse.
- **`WITH_REPLACE_OPTION`** parsing (`ParserGrantQuery.cpp:162–163`) — not in patch; merge carefully.
- **TABLE ENGINE validation** in execute (`:430–440`) — should not run for `default_replicated_db_privileges` shortcut (early return).
- **`StorageFactory` include** — HEAD already has it; patch did not.
- No conflicting `DEFAULT REPLICATED DATABASE PRIVILEGES` keyword on HEAD.

---

## Quick reference: HEAD anchor lines

| Location | Lines |
|---|---|
| ServerSettings insert neighbor | `1137–1142` |
| DatabaseReplicated ctor | `195–209` (.cpp), members `245–248` (.h) |
| registerDatabaseReplicated shard args | `2628–2667` |
| InterpreterGrantQuery::execute insert | before `484` (watch `420`) |
| Context members / mutex | `.h:362–367`, `.h:702` |
| setUserID model | `.cpp:1932–1941` |
| getAccess / full_access | `.cpp:2077–2135` |
| InterpreterCreateQuery::createDatabase | `.cpp:192–230` |
| InterpreterCreateQuery::execute | `.cpp:2443–2470` |
| InterpreterDropQuery database path | `.cpp:96–118` |
| executeDDLQueryOnCluster signature | `.h:47`, `.cpp:65–114` |
| Parser GRANT keywords | `ParserGrantQuery.cpp:132–167`, `CommonParsers.h:149–150` |

[REDACTED]
