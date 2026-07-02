#include <Storages/MaterializedView/RefreshTask.h>

#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <Common/Macros.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>
#include <Core/ServerSettings.h>
#include <Databases/DatabaseReplicated.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Cache/QueryResultCache.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSystemQuery.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/queryNormalization.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Storages/StorageMaterializedView.h>
#include <Common/CurrentMetrics.h>
#include <Common/QueryScope.h>
#include <Common/FailPoint.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/KeeperFeatureFlags.h>


namespace CurrentMetrics
{
    extern const Metric RefreshingViews;
}

namespace ProfileEvents
{
    extern const Event RefreshableViewRefreshSuccess;
    extern const Event RefreshableViewRefreshFailed;
    extern const Event RefreshableViewSyncReplicaSuccess;
    extern const Event RefreshableViewSyncReplicaRetry;
    extern const Event RefreshableViewLockTableRetry;
}

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 log_queries_cut_to_length;
    extern const SettingsBool stop_refreshable_materialized_views_on_startup;
    extern const SettingsSeconds lock_acquire_timeout;
}

namespace ServerSetting
{
    extern const ServerSettingsString default_replica_name;
    extern const ServerSettingsString default_replica_path;
    extern const ServerSettingsBool disable_insertion_and_mutation;
}

namespace RefreshSetting
{
    extern const RefreshSettingsBool all_replicas;
    extern const RefreshSettingsInt64 refresh_retries;
    extern const RefreshSettingsUInt64 refresh_retry_initial_backoff_ms;
    extern const RefreshSettingsUInt64 refresh_retry_max_backoff_ms;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
    extern const int REFRESH_FAILED;
    extern const int TABLE_IS_DROPPED;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_QUERY;
    extern const int ABORTED;
}

namespace FailPoints
{
extern const char refresh_task_stop_racing_for_running_refresh[];
}

namespace RefreshTimeout
{
    extern const int REFRESH_TIMEOUT_SEC = 60 * 60 * 2; // 2 hours
}

/*
 The RefreshTask class is responsible for refreshing a materialized view.
 It has 2 modes of operation: non-coordinated (non-replicated database) and coordinated (replicated).

 Coordinated mode (replica-level + shard-level coordination):

 Refreshable materialized views coordinate refreshes across the replicas of one shard using the
 root coordination znode, the ephemeral "running" znode, and the persistent `refresh_running` flag
 in the root znode. The persistent flag (upstream #104051) lets a brief Keeper connection loss not
 turn into a duplicate refresh: the replica that owns the refresh keeps `refresh_running = true` and
 `last_attempt_replica = <self>`, re-creates the ephemeral "running" znode on reconnect, and only
 after a grace period (when the ephemeral znode stays gone) do other replicas assume the owner
 crashed and take over.

 On top of that, in a sharded DatabaseReplicated, the refresh is coordinated across shards (patch
 066). Because DDL in a Replicated database propagates to all shards, finishing a refresh with a
 replicated EXCHANGE on one shard would swap the target table for a temporary table that holds no
 data on the other shards, deleting their freshly written rows. To prevent that:

  * The replica that wins the "running" znode race is the global leader. It creates a per-refresh
    directory "refresh_<timestamp>" in Keeper and publishes the temporary table's UUID under it.
  * Every shard elects a shard leader (an ephemeral znode "<refresh_dir>/<shard_name>"). The shard
    leader writes its shard's rows into the shared temporary table (identified by UUID, so all
    shards write to the exact same table regardless of DDL replication timing) and marks its shard
    finished ("<refresh_dir>/finished/<shard_name>").
  * The EXCHANGE is deferred: the global leader waits until all shards report finished, then does a
    single EXCHANGE (kept conditional on the root znode version, so a stale leader cannot clobber a
    newer refresh's data), drops the old table, and cleans up the refresh directory.

Here is the structure of the znodes in Keeper:
/ parent path "/clickhouse/tables/{uuid}/mv_refresh_qrLUb5TgIJ"
/ ├── ["running"] (ephemeral, contains global leader replica name)
/ ├── ["paused"]
/ └── "_refresh_<timestamp>"  (created for each refresh)
/     ├── "temporary_table" (contains UUID of the temporary table)
/     ├── "<shard_name>" (ephemeral, created by shard leader to claim leadership)
/     └── "finished"
/         ├── shard1 (created when shard1 completes its data write)
/         ├── shard2 (created when shard2 completes its data write)
/         └── shard3 (created when shard3 completes its data write)
*/

RefreshTask::RefreshTask(
    StorageMaterializedView * view_, ContextPtr context, const DB::ASTRefreshStrategy & strategy, bool /*attach*/, bool coordinated, bool empty, bool is_restore_from_backup)
    : view(view_)
    , refresh_schedule(strategy)
    , refresh_append(strategy.append)
{
    createLogger(view->getStorageID());

    auto component_guard = Coordination::setCurrentComponent("RefreshTask::RefreshTask");
    if (strategy.settings != nullptr)
        refresh_settings.applyChanges(strategy.settings->changes);

    coordination.root_znode.randomize();
    if (empty)
        coordination.root_znode.last_completed_timeslot = std::chrono::floor<std::chrono::seconds>(currentTime());
    if (coordinated)
    {
        coordination.coordinated = true;

        const auto & server_settings = context->getServerSettings();
        const auto macros = context->getMacros();
        Macros::MacroExpansionInfo info;
        info.table_id = view->getStorageID();
        const auto database = DatabaseCatalog::instance().getDatabase(view_->getStorageID().database_name);
        // Override default_replica_path to ensure all shards use the same path
        coordination.path = macros->expand("/clickhouse/tables/{uuid}/mv_refresh_qrLUb5TgIJ", info);
        if (const auto * replicated_db = dynamic_cast<const DatabaseReplicated *>(database.get()))
        {
            info.shard = replicated_db->getShardName();
            coordination.shard_name = replicated_db->getShardName();
        }
        coordination.replica_name = context->getMacros()->expand(server_settings[ServerSetting::default_replica_name], info);

        /// 066: the coordination znode (".../{uuid}/mv_refresh_qrLUb5TgIJ") is SHARED by every shard
        /// of a Replicated database — unlike a ReplicatedMergeTree path it is NOT keyed by {shard}.
        /// The replica-level coordination inherited from upstream #104051 identifies the replica that
        /// owns the running refresh by `last_attempt_replica == replica_name` (so the owner re-creates
        /// its ephemeral "running" znode after a brief Keeper disconnect instead of letting a peer
        /// start a duplicate refresh). For that to be correct across the shared subtree, the identifier
        /// must be unique across shards as well, but `default_replica_name` is usually just "{replica}"
        /// and collides between shards (each shard's sole replica is "replica1"). Qualify it with the
        /// shard name: it stays stable per process (so reconnect re-creation still works) and becomes
        /// globally unique. `shard_name` itself remains separate and is used for shard-leader election.
        if (!coordination.shard_name.empty())
            coordination.replica_name = coordination.shard_name + "/" + coordination.replica_name;

        auto zookeeper = context->getZooKeeper();
        bool root_znode_exists = zookeeper->exists(coordination.path);

        /// Create znodes even if it's ATTACH query. This seems weird, possibly incorrect, but
        /// currently both DatabaseReplicated and DatabaseShared seem to require this behavior.
        if (!root_znode_exists)
        {
            zookeeper->createAncestors(coordination.path);
            std::vector<zkutil::ZooKeeper::FutureCreate> futures;
            futures.emplace_back(zookeeper->asyncTryCreateNoThrow(coordination.path, coordination.root_znode.toString(), zkutil::CreateMode::Persistent));

            /// When restoring multiple tables from backup (e.g. a RESTORE DATABASE), the restored
            /// refreshable materialized views shouldn't start refreshing on any replica until all
            /// tables and their data is restored on all replicas. Otherwise things break:
            ///  * Refresh may EXCHANGE+DROP a table before its data is restored. The restore will
            ///    then fail when trying to write to a dropped table.
            ///  * Refresh may see empty source table before they're restored, producing empty
            ///    refresh result.
            ///
            /// Note that with replicated catalog a replicated database may be restored
            /// by a RESTORE running on just one replica, so one replica needs to be able to unpause
            /// refreshes on all replicas. This is the only reason why "paused" znode is a thing,
            /// otherwise we could just use stop_requested.
            if (is_restore_from_backup)
                futures.emplace_back(zookeeper->asyncTryCreateNoThrow(coordination.path + "/paused", "restored from backup", zkutil::CreateMode::Persistent));

            for (auto & future : futures)
            {
                auto res = future.get();
                if (res.error != Coordination::Error::ZOK && res.error != Coordination::Error::ZNODEEXISTS)
                    throw Coordination::Exception(res.error, "Failed to create new node {} with error {}",
                        res.path_created, Coordination::errorMessage(res.error));
            }
        }

        if (server_settings[ServerSetting::disable_insertion_and_mutation])
            coordination.read_only = true;
    }
    else
    {
        if (is_restore_from_backup)
            scheduling.stop_requested = true;
    }
}

void RefreshTask::createLogger(const StorageID & storage_id)
{
    std::lock_guard lock(logger_mutex);
    current_logger = ::getLogger(fmt::format("RefreshTask({})", storage_id.getFullTableName()));
}

LoggerPtr RefreshTask::getLogger()
{
    std::lock_guard lock(logger_mutex);
    return current_logger;
}

OwnedRefreshTask RefreshTask::create(
    StorageMaterializedView * view,
    ContextMutablePtr context,
    const DB::ASTRefreshStrategy & strategy,
    bool attach,
    bool coordinated,
    bool empty,
    bool is_restore_from_backup)
{
    auto task = std::make_shared<RefreshTask>(view, context, strategy, attach, coordinated, empty, is_restore_from_backup);

    task->scheduling_task = context->getSchedulePool().createTask(view->getStorageID(), "RefreshSched",
        [self = task.get()] { self->doScheduling(/*is_shutdown=*/ false); });
    task->execution_task = context->getSchedulePool().createTask(view->getStorageID(), "RefreshExec",
        [self = task.get()] { self->executeRefresh(); });

    task->watch_callback = std::make_shared<Coordination::WatchCallback>([w = task->coordination.watches, task_waker = task->scheduling_task->getWatchCallback()](const Coordination::WatchResponse & response)
    {
        w->should_reread_znodes.store(true);
        (*task_waker)(response);
    });

    if (strategy.dependencies)
        for (auto && dependency : strategy.dependencies->children)
            task->initial_dependencies.emplace_back(dependency->as<const ASTTableIdentifier &>());

    return OwnedRefreshTask(task);
}

bool RefreshTask::canCreateOrDropOtherTables() const
{
    return !refresh_append;
}

void RefreshTask::startup()
{
    if (view->getContext()->getSettingsRef()[Setting::stop_refreshable_materialized_views_on_startup])
        scheduling.stop_requested = true;
    auto inner_table_id = refresh_append ? std::nullopt : std::make_optional(view->getTargetTableId());
    view->getContext()->getRefreshSet().emplace(view->getStorageID(), inner_table_id, initial_dependencies, shared_from_this());

    std::lock_guard guard(mutex);
    scheduleRefresh(guard);
}

void RefreshTask::finalizeRestoreFromBackup()
{
    if (coordination.coordinated)
        startReplicated();
    else
        start();
}

void RefreshTask::shutdown()
{
    {
        std::lock_guard guard(mutex);

        if (view == nullptr)
            return; // already shut down

        scheduling.stop_requested = true;
        interruptExecution();
    }

    /// If we're in DatabaseReplicated, interrupt replicated CREATE/EXCHANGE/DROP queries in refresh task.
    /// Without this we can deadlock waiting for execution_task because this shutdown happens from the same DDL thread for which CREATE/EXCHANGE/DROP wait.
    execution.cancel_ddl_queries.request_stop();

    /// Wait for the tasks to return and prevent them from being scheduled in future.
    scheduling_task->deactivate();
    execution_task->deactivate();

    /// Best-effort final update of information in zookeeper, to reflect that this replica is not
    /// running a refresh anymore.
    try
    {
        doScheduling(/*is_shutdown=*/ true);
    }
    catch (...)
    {
        /// Avoid throwing from shutdown().
        /// If we failed to write to zookeeper, other replicas won't start refresh until our
        /// zookeeper session expires (+ grace period). This is not a problem if this
        /// shutdown() is caused by server shutdown or by table DROP, but may be bad if it's a DETACH
        /// (and we'll hold on to the session indefinitely).
        tryLogCurrentException(getLogger(), "Keeper error during RMV shutdown");
    }

    /// Remove from RefreshSet on DROP, without waiting for the IStorage to be destroyed.
    /// This matters because a table may get dropped and immediately created again with the same name,
    /// while the old table's IStorage still exists (pinned by ongoing queries).
    /// (Also, RefreshSet holds a shared_ptr to us.)
    std::lock_guard guard(mutex);
    set_handle.reset();

    view = nullptr;

    /// Wake up any threads blocked in wait(), so they can see !view and throw TABLE_IS_DROPPED.
    /// Without this, wait() would block forever after deactivate() prevents the background task
    /// from running (and therefore from ever notifying refresh_cv).
    refresh_cv.notify_all();
}

void RefreshTask::drop(ContextPtr context, bool is_shared_db)
{
    if (!coordination.coordinated)
        return;

    auto component_guard = Coordination::setCurrentComponent("RefreshTask::drop");
    auto zookeeper = context->getZooKeeper();

    /// In Shared DB, let SharedDatabaseCatalog handle node removal.
    if (is_shared_db)
        return;

    /// 066 stores per-shard coordination under the root path ("_refresh_*"/"running"/"paused");
    /// there is no per-replica "/replicas" subtree to clean up here (unlike upstream's layout).
    Coordination::Requests ops;
    String paused_path = coordination.path + "/paused";
    if (zookeeper->exists(paused_path))
        ops.emplace_back(zkutil::makeRemoveRequest(paused_path, -1));
    String running_path = coordination.path + "/running";
    if (zookeeper->exists(running_path))
    {
        /// shutdown() was supposed to delete it.
        LOG_ERROR(getLogger(), "Unexpected 'running' znode when dropping refreshable materialized view.");
        ops.emplace_back(zkutil::makeRemoveRequest(running_path, -1));
    }
    /// Remember which response corresponds to the root removal: it may legitimately fail with
    /// ZNOTEMPTY if other shards' coordination znodes still exist, which we tolerate.
    size_t root_op_idx = ops.size();
    ops.emplace_back(zkutil::makeRemoveRequest(coordination.path, -1));
    Coordination::Responses responses;
    auto code = zookeeper->tryMulti(ops, responses);
    if (responses[root_op_idx]->error != Coordination::Error::ZNOTEMPTY && responses[root_op_idx]->error != Coordination::Error::ZNONODE)
        zkutil::KeeperMultiException::check(code, ops, responses);
}

void RefreshTask::rename(StorageID new_id, StorageID new_inner_table_id)
{
    std::lock_guard guard(mutex);
    createLogger(new_id);
    if (set_handle)
        set_handle.rename(new_id, refresh_append ? std::nullopt : std::make_optional(new_inner_table_id));
}

void RefreshTask::checkAlterIsPossible(const DB::ASTRefreshStrategy & new_strategy)
{
    RefreshSettings s;
    if (new_strategy.settings)
        s.applyChanges(new_strategy.settings->changes);
    if (s[RefreshSetting::all_replicas] != refresh_settings[RefreshSetting::all_replicas])
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Altering setting 'all_replicas' is not supported.");
    if (new_strategy.append != refresh_append)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Adding or removing APPEND is not supported.");
}

void RefreshTask::alterRefreshParams(const DB::ASTRefreshStrategy & new_strategy)
{
    StorageID view_storage_id = StorageID::createEmpty();

    {
        std::lock_guard guard(mutex);

        refresh_schedule = RefreshSchedule(new_strategy);
        std::vector<StorageID> deps;
        if (new_strategy.dependencies)
            for (auto && dependency : new_strategy.dependencies->children)
                deps.emplace_back(dependency->as<const ASTTableIdentifier &>());

        /// Update dependency graph.
        if (set_handle)
            set_handle.changeDependencies(deps);

        scheduleRefresh(guard);
        scheduling.dependencies_satisfied_until = std::chrono::sys_seconds(std::chrono::seconds(-1));

        refresh_settings = {};
        if (new_strategy.settings != nullptr)
            refresh_settings.applyChanges(new_strategy.settings->changes);

        if (view)
            view_storage_id = view->getStorageID();
    }

    /// In case refresh period changed.
    if (view_storage_id)
    {
        const auto & refresh_set = Context::getGlobalContextInstance()->getRefreshSet();
        refresh_set.notifyDependents(view_storage_id);
    }
}

RefreshTask::Info RefreshTask::getInfo() const
{
    std::lock_guard guard(mutex);
    return Info {.view_id = set_handle.getID(), .state = state, .next_refresh_time = next_refresh_time, .znode = coordination.root_znode, .replica_name = coordination.replica_name, .refresh_running = coordination.root_znode.refresh_running, .progress = execution.progress.getValues(), .unexpected_error = scheduling.unexpected_error};
}

void RefreshTask::start()
{
    std::lock_guard guard(mutex);
    if (!std::exchange(scheduling.stop_requested, false))
        return;
    scheduling.unexpected_error = std::nullopt;
    scheduleRefresh(guard);
}

void RefreshTask::stop()
{
    std::lock_guard guard(mutex);
    bool was_already_stopped = std::exchange(scheduling.stop_requested, true);
    /// Always interrupt the in-flight refresh. This matters in the PAUSE-then-STOP sequence:
    /// `SYSTEM PAUSE VIEW` leaves the running refresh alone but sets `stop_requested`, and a
    /// subsequent `SYSTEM STOP VIEW` must still cancel it. `interruptExecution` is idempotent
    /// (guarded by `execution.interrupt_execution`) so repeated calls are safe.
    interruptExecution();
    if (!was_already_stopped)
        scheduleRefresh(guard);
}

void RefreshTask::pause()
{
    std::lock_guard guard(mutex);
    /// Do NOT interrupt the currently running refresh. Only prevent future refreshes.
    /// If `stop_requested` was already set (e.g. by `SYSTEM STOP VIEW`), this is a no-op.
    if (std::exchange(scheduling.stop_requested, true))
        return;
    scheduleRefresh(guard);
}

void RefreshTask::startReplicated()
{
    auto component_guard = Coordination::setCurrentComponent("RefreshTask::startReplicated");
    if (!coordination.coordinated)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Refreshable materialized view is not coordinated.");

    const auto zookeeper = [this]()
    {
        std::lock_guard guard(mutex);
        if (!view)
            throw Exception(ErrorCodes::TABLE_IS_DROPPED, "The table was dropped or detached");
        return view->getContext()->getZooKeeper();
    }();

    String path = coordination.path + "/paused";
    auto code = zookeeper->tryRemove(path);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
        throw Coordination::Exception::fromPath(code, path);
}

void RefreshTask::stopReplicated(const String & reason)
{
    if (!coordination.coordinated)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Refreshable materialized view is not coordinated.");

    auto component_guard = Coordination::setCurrentComponent("RefreshTask::stopReplicated");
    const auto zookeeper = [this]()
    {
        std::lock_guard guard(mutex);
        if (!view)
            throw Exception(ErrorCodes::TABLE_IS_DROPPED, "The table was dropped or detached");
        return view->getContext()->getZooKeeper();
    }();

    String path = coordination.path + "/paused";
    auto code = zookeeper->tryCreate(path, reason, zkutil::CreateMode::Persistent);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
        throw Coordination::Exception::fromPath(code, path);
}

void RefreshTask::run()
{
    std::lock_guard guard(mutex);
    if (std::exchange(scheduling.out_of_schedule_refresh_requested, true))
        return;
    scheduleRefresh(guard);
}

void RefreshTask::cancel()
{
    std::lock_guard guard(mutex);
    interruptExecution();
    scheduleRefresh(guard);
}

void RefreshTask::wait()
{
    auto throw_if_error = [&]
    {
        if (!view)
            throw Exception(ErrorCodes::TABLE_IS_DROPPED, "The table was dropped or detached");
        if (!coordination.root_znode.refresh_running && !coordination.root_znode.last_attempt_succeeded && coordination.root_znode.last_attempt_time.time_since_epoch().count() != 0)
            throw Exception(ErrorCodes::REFRESH_FAILED,
                "Refresh failed{}: {}", coordination.coordinated ? " (on replica " + coordination.root_znode.last_attempt_replica + ")" : "",
                coordination.root_znode.last_attempt_error.empty() ? "Replica went away" : coordination.root_znode.last_attempt_error);
    };

    std::unique_lock lock(mutex);
    refresh_cv.wait(lock, [&] {
        return !view
            || (state != RefreshState::Running && state != RefreshState::Scheduling
                && state != RefreshState::RunningOnAnotherReplica
                && (state == RefreshState::Disabled || !scheduling.out_of_schedule_refresh_requested));
    });
    throw_if_error();

    if (coordination.coordinated && !refresh_append)
    {
        /// Wait until we see the table produced by the latest refresh.
        while (true)
        {
            UUID expected_table_uuid = coordination.root_znode.last_success_table_uuid;
            StorageID storage_id = view->getTargetTableId();
            ContextPtr context = view->getContext();
            lock.unlock();

            /// (Can't use `view` here because shutdown() may unset it in parallel with us.)
            StoragePtr storage = DatabaseCatalog::instance().tryGetTable(storage_id, context);
            if (storage && storage->getStorageID().uuid == expected_table_uuid)
                return;

            std::this_thread::sleep_for(std::chrono::milliseconds(10));

            lock.lock();
            /// Re-check last_attempt_succeeded in case another refresh EXCHANGEd the table but failed to write its uuid to keeper.
            throw_if_error();
        }
    }
}

bool RefreshTask::tryJoinBackgroundTask(std::chrono::steady_clock::time_point deadline)
{
    std::unique_lock lock(mutex);

    execution.cancel_ddl_queries.request_stop();

    auto duration = deadline - std::chrono::steady_clock::now();
    /// (Manually clamping to 0 because the standard library used to have (and possibly still has?)
    ///  a bug that wait_until would wait forever if the timestamp is in the past.)
    duration = std::max(duration, std::chrono::steady_clock::duration(0));
    return refresh_cv.wait_for(lock, duration, [&]
        {
            return state != RefreshState::Running && state != RefreshState::Scheduling;
        });
}

std::chrono::sys_seconds RefreshTask::getNextRefreshTimeslot() const
{
    std::lock_guard guard(mutex);
    return refresh_schedule.advance(coordination.root_znode.last_completed_timeslot);
}

void RefreshTask::notify()
{
    std::lock_guard guard(mutex);
    if (view && view->getContext()->getRefreshSet().refreshesStopped())
        interruptExecution();
    scheduling.dependencies_satisfied_until = std::chrono::sys_seconds(std::chrono::seconds(-1));
    scheduleRefresh(guard);
}

void RefreshTask::setFakeTime(std::optional<Int64> t)
{
    std::unique_lock lock(mutex);
    Int64 val = t.value_or(INT64_MIN);
    LOG_INFO(getLogger(), "Set fake time: {}", val);
    scheduling.fake_clock.store(val, std::memory_order_relaxed);
    /// Reschedule task with shorter delay if currently scheduled.
    scheduling_task->scheduleAfter(100, /*overwrite*/ true, /*only_if_scheduled*/ true);
}

void RefreshTask::doScheduling(bool is_shutdown)
{
    auto component_guard = Coordination::setCurrentComponent("RefreshTask::doScheduling");
    std::unique_lock lock(mutex);

    /// The way this function generally works is:
    ///  * Look at state in zookeeper and in memory and at current time.
    ///  * If some change is needed (e.g. write to zookeeper or start a refresh), make that change,
    ///    do scheduling_task->schedule() (to inspect the new state after the change), and return.
    ///    (We avoid making multiple changes in one iteration because that just adds more
    ///     opportunities for bugs. This function should be able to pick up from ~any state anyway.)
    ///  * If no change is needed, we setState, optionally scheduling_task->scheduleAfter, and return.
    ///  * On error, we scheduling_task->schedule/scheduleAfter and return.

    try
    {
        setState(RefreshState::Scheduling, lock);

        std::shared_ptr<zkutil::ZooKeeper> zookeeper;
        if (coordination.coordinated)
            zookeeper = view->getContext()->getZooKeeper();
        readZnodesIfNeeded(zookeeper, lock);
        chassert(lock.owns_lock());

        /// Sync 3 pieces of information about currently running refresh:
        ///  * coordination.root_znode.refresh_running
        ///  * coordination.running_znode_exists
        ///  * execution.state
        /// (Why are there as many as 3? We need an ephemeral znode to notice server crashes, a
        ///  non-ephemeral znode to tolerate brief zookeeper connection loss, and in-memory state to
        ///  communicate with the thread that executes refresh.)

        auto running_znode_missing_since = coordination.running_znode_missing_since;
        coordination.running_znode_missing_since.reset(); // reassigned below if still missing

        if (coordination.root_znode.refresh_running && coordination.root_znode.last_attempt_replica == coordination.replica_name)
        {
            /// Our replica is allegedly running a refresh (we are the global leader).

            if (is_shutdown)
            {
                chassert(execution.state != ExecutionState::State::Running);
                if (execution.state == ExecutionState::State::Requested)
                {
                    /// execution_task was deactivated by shutdown before refresh started.
                    execution.znode.last_attempt_error = "shutdown";
                    execution.znode.refresh_running = false;
                    execution.znode.refresh_dir = "";
                    execution.state = ExecutionState::State::Finished;
                }
            }


            switch (execution.state)
            {
                case ExecutionState::State::None:
                {
                    LOG_WARNING(getLogger(), "RMV znode says this replica is running refresh, but it isn't. Maybe we crashed and restarted recently, and the state is left over from the crashed run?");
                    CoordinationZnode znode = coordination.root_znode;
                    znode.refresh_running = false;
                    znode.refresh_dir = "";
                    updateCoordinationState(znode, /*running=*/ false, zookeeper, lock);
                    scheduling_task->schedule();
                    break;
                }
                case ExecutionState::State::Finished:
                {
                    /// Report refresh completion (successful or not) to the znode.
                    if (execution.znode.version == coordination.root_znode.version)
                    {
                        if (!updateCoordinationState(execution.znode, /*running=*/ false, zookeeper, lock))
                            return;
                        chassert(!coordination.root_znode.refresh_running);

                        if (coordination.root_znode.last_attempt_succeeded)
                        {
                            lock.unlock();
                            view->getContext()->getRefreshSet().notifyDependents(view->getStorageID());
                            lock.lock();
                        }
                    }
                    else
                    {
                        LOG_ERROR(getLogger(), "RMV znode was updated (version {} -> {}) while refresh was running (without changing refresh_running and last_attempt_replica). This should only be possible if another server has the same replica name as me.", execution.znode.version, coordination.root_znode.version);
                        CoordinationZnode znode = coordination.root_znode;
                        znode.refresh_running = false;
                        znode.refresh_dir = "";
                        updateCoordinationState(znode, /*running=*/ false, zookeeper, lock);
                    }

                    chassert(lock.owns_lock());
                    execution.state = ExecutionState::State::None;
                    /// Go to Scheduled state after each refresh, even if for a moment before
                    /// starting the next refresh. This gives `wait()` a chance to complete.
                    setState(RefreshState::Scheduled, lock);
                    scheduling_task->schedule();
                    break;
                }
                case ExecutionState::State::Requested:
                case ExecutionState::State::Running:
                {
                    if (!coordination.running_znode_exists)
                    {
                        LOG_WARNING(getLogger(), "Re-creating ephemeral znode '{}', presumably lost on zookeeper reconnect.", coordination.path + "/running");
                        updateCoordinationState(coordination.root_znode, /*running=*/ true, zookeeper, lock, /*only_running_znode=*/ true);
                    }

                    setState(RefreshState::Running, lock);
                    break;
                }
            }

            return;
        }
        else
        {
            if (execution.state != ExecutionState::State::None)
            {
                LOG_ERROR(getLogger(), "RMV refresh is running locally, but keeper says there's no running refresh on this replica. This should only be possible after keeper was unavailable for a while (over 1-2 minutes).");
                if (execution.state == ExecutionState::State::Finished)
                    execution.state = ExecutionState::State::None;
                else
                    interruptExecution();
            }

            if (coordination.root_znode.refresh_running)
            {
                /// Another replica is allegedly running a refresh (it is the global leader).

                if (!coordination.running_znode_exists)
                {
                    /// If ephemeral znode unexpectedly disappears, wait for this long to give the
                    /// owner of the znode a chance to re-create it.
                    /// Currently hard-coded as 1.25x the keeper session timeout, but it doesn't
                    /// necessarily need to be longer than session timeout, since the grace period
                    /// starts after the session already expired.
                    UInt64 grace_period_ms = zookeeper->getSessionTimeoutMS();
                    grace_period_ms += grace_period_ms / 4;

                    std::chrono::system_clock::time_point now = currentTime();
                    if (!running_znode_missing_since.has_value())
                    {
                        LOG_INFO(getLogger(), "RMV coordination znode says refresh is running on replica '{}', but there's no corresponding ephemeral znode. Waiting for {} ms before assuming that the replica crashed.", coordination.root_znode.last_attempt_replica, grace_period_ms);
                        running_znode_missing_since = now;
                    }
                    coordination.running_znode_missing_since = running_znode_missing_since;

                    std::chrono::system_clock::time_point deadline = *coordination.running_znode_missing_since + std::chrono::milliseconds(grace_period_ms);
                    if (now >= deadline)
                    {
                        LOG_WARNING(getLogger(), "Replica '{}' appears to have crashed while executing a refresh. Clearing the lock in zookeeper to allow another replica to start a new refresh.", coordination.root_znode.last_attempt_replica);

                        CoordinationZnode znode = coordination.root_znode;
                        chassert(znode.refresh_running);
                        znode.refresh_running = false;
                        znode.refresh_dir = "";
                        updateCoordinationState(znode, /*running=*/ false, zookeeper, lock);
                        scheduling_task->schedule();
                        return;
                    }
                    else
                    {
                        scheduling_task->scheduleAfter(std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count());
                    }
                }
                else if (coordination.coordinated && !coordination.current_refresh_dir.empty()
                         && !scheduling.stop_requested && !coordination.paused_znode_exists && !coordination.read_only
                         && !view->getContext()->getRefreshSet().refreshesStopped())
                {
                    /// 066: a peer replica is the global leader for this refresh. In a sharded
                    /// Replicated database, this replica's shard must write its own rows into the
                    /// shared temporary table; otherwise the global leader's deferred EXCHANGE would
                    /// publish a target table that is empty on this shard and delete its data.
                    /// Claim shard leadership for our shard and, if we win it, run our shard's portion
                    /// of the refresh (the insert), then mark the shard finished.
                    lock.unlock();
                    try
                    {
                        if (tryBecomeShardLeader(zookeeper))
                        {
                            LOG_DEBUG(getLogger(), "Participating as shard leader for shard '{}' in '{}'", coordination.shard_name, coordination.current_refresh_dir);
                            /// Clear any stale interrupt left over from a previous (cancelled) attempt;
                            /// it applies only to the refresh that was in flight.
                            execution.interrupt_execution.store(false);
                            StorageID temp_table_id = getOrWaitForTemporaryTableID(zookeeper, StorageID::createEmpty());
                            if (temp_table_id.empty())
                            {
                                LOG_WARNING(getLogger(), "Temporary table for shard '{}' not found; the refresh may have already completed.", coordination.shard_name);
                            }
                            else
                            {
                                executeRefreshUnlocked(temp_table_id);
                                markShardFinished(zookeeper);
                            }
                            coordination.is_shard_leader = false;
                        }
                    }
                    catch (...)
                    {
                        coordination.is_shard_leader = false;
                        tryLogCurrentException(getLogger(), fmt::format("Shard '{}' failed to write its data during refresh", coordination.shard_name));
                    }
                    lock.lock();
                }

                setState(RefreshState::RunningOnAnotherReplica, lock);
                return;
            }
            else if (coordination.running_znode_exists)
            {
                LOG_WARNING(getLogger(), "RMV coordination znode says no refresh is running, but the ephemeral 'running' znode exists. Removing the stale znode.");
                updateCoordinationState(coordination.root_znode, /*running=*/ false, zookeeper, lock, /*only_running_znode=*/ true);
                scheduling_task->schedule();
                return;
            }
        }

        if (is_shutdown)
            return; // we just needed to propagate information into zookeeper

        /// Decide when to do the next refresh.

        updateDependenciesIfNeeded(lock);
        chassert(lock.owns_lock());

        if (scheduling.stop_requested || coordination.paused_znode_exists || view->getContext()->getRefreshSet().refreshesStopped() || coordination.read_only)
        {
            setState(RefreshState::Disabled, lock);
            return;
        }

        auto start_time = currentTime();
        auto start_time_seconds = std::chrono::floor<std::chrono::seconds>(start_time);
        auto [when, timeslot, start_znode] = determineNextRefreshTime(start_time_seconds);
        next_refresh_time = when;
        bool out_of_schedule = scheduling.out_of_schedule_refresh_requested;
        if (out_of_schedule)
        {
            chassert(start_znode.attempt_number > 0);
            start_znode.attempt_number -= 1;
        }
        else if (start_time < when)
        {
            size_t delay_ms = std::chrono::duration_cast<std::chrono::milliseconds>(when - start_time).count();
            /// If we're in a test that fakes the clock, poll every 100ms.
            if (scheduling.fake_clock.load(std::memory_order_relaxed) != INT64_MIN)
                delay_ms = 100;
            scheduling_task->scheduleAfter(delay_ms);
            setState(RefreshState::Scheduled, lock);
            return;
        }
        else if (timeslot >= scheduling.dependencies_satisfied_until)
        {
            setState(RefreshState::WaitingForDependencies, lock);
            return;
        }

        /// The time to start next refresh is now!

        /// 066: elect the global leader first without publishing refresh_dir. Only the elected
        /// leader may create the per-refresh directory and reserve its own shard. Publishing
        /// refresh_dir before the leader owns the shard lets a peer from the same shard claim it.
        String refresh_dir_name;
        bool created_refresh_dir = false;
        if (coordination.coordinated)
        {
            auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(start_time).time_since_epoch().count();
            refresh_dir_name = "_refresh_" + std::to_string(now_ms);
            start_znode.refresh_dir.clear();
        }

        /// Write to keeper. This atomically sets the root znode (refresh_running = true) and creates
        /// the ephemeral "running" znode, conditional on the root znode version. Winning this race
        /// makes us the unique global leader for this refresh.
        if (!updateCoordinationState(start_znode, /*running=*/ true, zookeeper, lock))
            return;
        chassert(lock.owns_lock());

        if (coordination.coordinated)
        {
            lock.unlock();
            try
            {
                cleanupOldRefreshDirectories(zookeeper);
                createRefreshDirectory(zookeeper, refresh_dir_name);
                created_refresh_dir = true;
                if (!tryBecomeShardLeader(zookeeper))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Global refresh leader failed to become shard leader");
            }
            catch (...)
            {
                String error_message = getCurrentExceptionMessage(true);
                if (created_refresh_dir)
                    cleanupRefreshDirectory(zookeeper);
                coordination.current_refresh_dir.clear();
                coordination.is_shard_leader = false;
                lock.lock();

                auto failed_znode = coordination.root_znode;
                failed_znode.last_attempt_error = error_message;
                failed_znode.refresh_running = false;
                failed_znode.refresh_dir.clear();
                updateCoordinationState(failed_znode, /*running=*/ false, zookeeper, lock);
                scheduling_task->schedule();
                return;
            }
            lock.lock();

            auto znode_with_refresh_dir = coordination.root_znode;
            znode_with_refresh_dir.refresh_dir = refresh_dir_name;
            if (!updateCoordinationState(znode_with_refresh_dir, /*running=*/ true, zookeeper, lock))
            {
                if (created_refresh_dir)
                {
                    lock.unlock();
                    cleanupRefreshDirectory(zookeeper);
                    coordination.is_shard_leader = false;
                    lock.lock();
                }
                return;
            }
        }
        chassert(lock.owns_lock());

        scheduling.out_of_schedule_refresh_requested = false;

        chassert(execution.state == ExecutionState::State::None);
        execution.interrupt_execution.store(false);
        execution.znode = coordination.root_znode;
        execution.start_time = start_time;
        execution.out_of_schedule = out_of_schedule;
        execution.state = ExecutionState::State::Requested;

        execution_task->schedule();
        setState(RefreshState::Running, lock);
    }
    catch (Coordination::Exception &)
    {
        tryLogCurrentException(getLogger(), "Keeper error");
        if (!lock.owns_lock())
            lock.lock();

        chassert(state == RefreshState::Scheduling);
        coordination.watches->should_reread_znodes.store(true);
        scheduling_task->scheduleAfter(5000);
    }
    catch (...)
    {
        if (!lock.owns_lock())
            lock.lock();
        scheduling.stop_requested = true;
        scheduling.unexpected_error = getCurrentExceptionMessage(true);
        coordination.watches->should_reread_znodes.store(true);
        interruptExecution();
        setState(RefreshState::Scheduling, lock);
        scheduling_task->schedule();

        tryLogCurrentException(getLogger(),
            "Unexpected exception in refresh scheduling. The view will be stopped.");
#ifdef DEBUG_OR_SANITIZER_BUILD
        abortOnFailedAssertion("Unexpected exception in refresh scheduling");
#endif
    }
}

void RefreshTask::executeRefresh()
{
    /// executeRefresh() runs on its own background-schedule-pool thread and (unlike upstream's
    /// version) performs coordination ZK ops directly via the 066 shard helpers
    /// (createRefreshDirectory, tryBecomeShardLeader, getOrWaitForTemporaryTableID, markShardFinished,
    /// checkAllShardsFinished, cleanupRefreshDirectory). Every RefreshTask entry point that touches
    /// Keeper must set the current component (dossier §5.3 / patch 008), otherwise those ops throw
    /// LOGICAL_ERROR ("Current component is empty"). setCurrentComponent is thread-local, so setting
    /// it here covers all helpers invoked on this thread.
    auto component_guard = Coordination::setCurrentComponent("RefreshTask::executeRefresh");
    std::unique_lock lock(mutex);

    chassert(execution.state == ExecutionState::State::Requested);
    execution.state = ExecutionState::State::Running;

    Stopwatch stopwatch;
    int32_t root_znode_version = execution.znode.version;
    String refresh_dir_name = execution.znode.refresh_dir;
    bool out_of_schedule = execution.out_of_schedule;
    auto start_time = execution.start_time;
    auto start_time_seconds = std::chrono::floor<std::chrono::seconds>(start_time);
    bool append = refresh_append;
    StorageID view_storage_id = view->getStorageID();

    String log_comment = fmt::format("refresh of {}", view_storage_id.getFullTableName());
    if (execution.znode.attempt_number > 1)
        log_comment += fmt::format(" (attempt {}/{})", execution.znode.attempt_number, refresh_settings[RefreshSetting::refresh_retries] + 1);

    String error_message;
    std::optional<UUID> new_table_uuid;

    /// This replica won the running-znode race in doScheduling(), so it is the global leader for
    /// this refresh. Set the leader flag and the per-refresh directory here, while we still hold the
    /// lock, so they stay stable for the whole (unlocked) refresh: readZnodesIfNeeded() will not
    /// clobber current_refresh_dir while our own refresh is running (see its we_are_running_leader
    /// guard), and the shard-coordination helpers below read these fields without the lock.
    coordination.is_global_leader = true;
    if (!coordination.coordinated)
        coordination.is_shard_leader = true;
    if (coordination.coordinated)
        coordination.current_refresh_dir = coordination.path + "/" + refresh_dir_name;
    else
        coordination.current_refresh_dir.clear();

    lock.unlock();

    std::shared_ptr<zkutil::ZooKeeper> zookeeper;
    std::optional<StorageID> table_to_drop;
    ContextMutablePtr create_context = view->createRefreshContext(log_comment);
    try
    {
        CurrentMetrics::Increment metric_inc(CurrentMetrics::RefreshingViews);

        if (coordination.coordinated)
        {
            zookeeper = view->getContext()->getZooKeeper();
            chassert(!coordination.current_refresh_dir.empty());
            chassert(coordination.is_shard_leader);
        }

        if (!append)
        {
            create_context->setParentTable(view_storage_id.uuid);
            create_context->setDDLQueryCancellation(execution.cancel_ddl_queries.get_token());
            /// Keep the CREATE of the temporary table conditional on the coordination znode version
            /// (upstream #104051): if a brief Keeper disconnect let another replica take over, the
            /// version changed and the CREATE fails instead of racing a newer refresh.
            if (root_znode_version != -1)
                create_context->setDDLAdditionalChecksOnEnqueue({zkutil::makeCheckRequest(coordination.path, root_znode_version)});
        }

        /// Create the temporary table that all shards write into.
        StorageID target_table = view->prepareTableForInsert(append, create_context);
        if (!append)
            table_to_drop = target_table;
        {
            std::lock_guard guard(mutex);
            coordination.temporary_table_name = target_table.table_name;
        }

        /// Publish the temporary table's UUID so shard leaders write to the exact same table.
        if (coordination.coordinated)
            getOrWaitForTemporaryTableID(zookeeper, target_table);

        /// Write this shard's portion of the data into the temporary table.
        new_table_uuid = executeRefreshUnlocked(target_table);

        if (coordination.coordinated)
        {
            markShardFinished(zookeeper);

            /// Defer the EXCHANGE until every shard has written its data. Without this, one shard's
            /// replicated EXCHANGE would publish a target table that is empty on the other shards and
            /// delete their freshly written rows (the data-loss bug 066 fixes).
            while (!checkAllShardsFinished(zookeeper))
            {
                if (currentTime() - start_time > std::chrono::seconds(RefreshTimeout::REFRESH_TIMEOUT_SEC))
                    throw Exception(ErrorCodes::REFRESH_FAILED, "Timeout waiting for all shards to finish the refresh");
                if (execution.interrupt_execution.load())
                    throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Refresh for view {} cancelled while waiting for shards", view_storage_id.getFullTableName());
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }

        /// All shards finished: publish the new data with a single, version-guarded EXCHANGE.
        exchangeTargetTableAfterRefresh(target_table, append, root_znode_version);

        /// After a successful EXCHANGE, the temporary-table NAME now refers to the old target's data,
        /// so dropping it removes the stale data. (On failure, below, it removes the unfinished temp.)
        if (table_to_drop.has_value())
        {
            String discard_error;
            view->dropTempTable(table_to_drop.value(), create_context, discard_error);
        }

        ProfileEvents::increment(ProfileEvents::RefreshableViewRefreshSuccess);
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::RefreshableViewRefreshFailed);
        new_table_uuid.reset();

        bool cancelled = execution.interrupt_execution.load();
        if (table_to_drop.has_value())
        {
            String discard_error;
            view->dropTempTable(table_to_drop.value(), create_context, discard_error);
        }
        if (cancelled)
            error_message = "cancelled";
        else
            error_message = getCurrentExceptionMessage(true);
        tryLogCurrentException(getLogger(), "Refresh failed");
    }

    /// Global leader cleans up the per-refresh directory (best effort).
    if (coordination.coordinated)
    {
        try
        {
            cleanupRefreshDirectory(zookeeper);
        }
        catch (...)
        {
            tryLogCurrentException(getLogger(), "Failed to clean up refresh directory");
        }
    }

    lock.lock();

    coordination.is_global_leader = false;
    coordination.is_shard_leader = false;

    auto end_time_seconds = std::chrono::floor<std::chrono::seconds>(currentTime());
    CoordinationZnode znode = execution.znode;
    znode.last_attempt_time = end_time_seconds;
    znode.last_attempt_error = error_message;
    znode.refresh_running = false;
    znode.refresh_dir = "";
    if (new_table_uuid.has_value())
    {
        znode.last_attempt_succeeded = true;
        znode.last_completed_timeslot = refresh_schedule.timeslotForCompletedRefresh(znode.last_completed_timeslot, start_time_seconds, end_time_seconds, out_of_schedule);
        znode.last_success_time = start_time_seconds;
        znode.last_success_duration = std::chrono::milliseconds(stopwatch.elapsedMilliseconds());
        znode.last_success_table_uuid = *new_table_uuid;
        znode.previous_attempt_error = "";
        znode.attempt_number = 0;
        znode.randomize();
    }
    execution.znode = znode;

    chassert(execution.state == ExecutionState::State::Running);
    execution.state = ExecutionState::State::Finished;

    scheduling_task->schedule();
}

UUID RefreshTask::executeRefreshUnlocked(const StorageID & target_table_id)
{
    /// Only executes after the replica has become a shard leader or the global leader.
    LOG_DEBUG(getLogger(), "Refreshing view {} (global_leader={}, shard_leader={})",
        view->getStorageID().getFullTableName(), coordination.is_global_leader, coordination.is_shard_leader);
    execution.progress.reset();
    StorageID view_storage_id = view->getStorageID();
    /// Tag the refresh query with the same `log_comment` the pre-066 state machine used, so it is
    /// identifiable in `system.query_log` (e.g. `log_comment LIKE 'refresh of db.v%'`). The rewrite
    /// created the refresh context with an empty comment, which hid refresh queries from such lookups.
    String log_comment = fmt::format("refresh of {}", view_storage_id.getFullTableName());
    ContextMutablePtr refresh_context = view->createRefreshContext(log_comment);
    std::optional<QueryLogElement> query_log_elem;
    boost::intrusive_ptr<ASTInsertQuery> refresh_query;
    std::shared_ptr<OpenTelemetry::SpanHolder> query_span = std::make_shared<OpenTelemetry::SpanHolder>("query");
    /// Must outlive the try/catch below: the catch calls logQueryException -> getProcessListElement,
    /// which holds only a weak_ptr into this entry. If the entry were destroyed during stack unwinding
    /// (e.g. declared inside the try), the weak_ptr would expire and logging would throw a LOGICAL_ERROR.
    ProcessList::EntryPtr process_list_entry;

    QueryScope query_scope;
    if (!coordination.is_global_leader && !coordination.is_shard_leader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid coordination state");

    /// The pre-066 state machine logged each refresh as one INSERT SELECT query to system.query_log on
    /// both success and failure. The rewrite kept the success side (logQueryStart/logQueryFinish) but
    /// dropped the failure side, so failed refreshes disappeared from query_log. Restore the failure
    /// logging and re-throw so the scheduling loop still records the error in the coordination znode
    /// and reschedules. query_for_logging starts as a placeholder so a failure during query
    /// interpretation (e.g. the definer lacks SELECT on the source -> ACCESS_DENIED, raised before
    /// logQueryStart) is still recorded as ExceptionBeforeStart, matching the pre-066 behavior.
    String query_for_logging = "(create target table)";
    UInt64 normalized_query_hash = normalizedQueryHash(query_for_logging, false);
    Stopwatch query_stopwatch;
    try
    {
        std::tie(refresh_query, query_scope) = view->prepareRefresh(refresh_context, target_table_id);

        /// Add the query to system.processes and allow it to be killed with KILL QUERY.
        query_for_logging = refresh_query->formatForLogging(
            refresh_context->getSettingsRef()[Setting::log_queries_cut_to_length]);
        normalized_query_hash = normalizedQueryHash(query_for_logging, false);

        process_list_entry = refresh_context->getProcessList().insert(
            query_for_logging, normalized_query_hash, refresh_query.get(), refresh_context, Stopwatch{CLOCK_MONOTONIC}.getStart(), /*is_internal*/ true);

        refresh_context->setProcessListElement(process_list_entry->getQueryStatus());

        /// Publish the query status before interpreting the query, not just around the pipeline
        /// executor below: planning runs nested pipelines for `IN (subquery)` sets, and only the
        /// status cancels those. Upstream #113188; re-homed into the 066 refresh path on this uplift.
        {
            std::unique_lock exec_lock(execution.executor_mutex);
            if (execution.interrupt_execution.load())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Refresh for view {} cancelled", view_storage_id.getFullTableName());
            execution.executing_query_status = process_list_entry->getQueryStatus();
        }
        SCOPE_EXIT({
            std::unique_lock exec_lock(execution.executor_mutex);
            execution.executing_query_status = nullptr;
        });

        refresh_context->setProgressCallback([this](const Progress & prog)
        {
            execution.progress.incrementPiecewiseAtomically(prog);
        });

        /// Run the query - each shard leader writes its portion of the data.
        InterpreterInsertQuery interpreter(
            refresh_query,
            refresh_context,
            /* allow_materialized */ false,
            /* no_squash */ false,
            /* no_destination */ false,
            /* async_isnert */ false);
        BlockIO block_io = interpreter.execute();
        QueryPipeline & pipeline = block_io.pipeline;

        /// We log the refresh as one INSERT SELECT query, but the timespan and exceptions also
        /// cover the surrounding CREATE, EXCHANGE, and DROP queries.
        query_log_elem = logQueryStart(
            currentTime(),
            refresh_context, query_for_logging, normalized_query_hash, refresh_query, pipeline,
            &interpreter, /*internal*/ false, view_storage_id.database_name,
            view_storage_id.table_name, /*async_insert*/ false);

        if (!pipeline.completed())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline for view {} refresh must be completed", view_storage_id.getFullTableName());

        {
            PipelineExecutor executor(pipeline.processors, pipeline.process_list_element);
            executor.setReadProgressCallback(pipeline.getReadProgressCallback());

            {
                std::unique_lock exec_lock(execution.executor_mutex);
                if (execution.interrupt_execution.load())
                    throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Refresh for view {} cancelled", view_storage_id.getFullTableName());
                execution.executor = &executor;
            }
            SCOPE_EXIT({
                std::unique_lock exec_lock(execution.executor_mutex);
                execution.executor = nullptr;
            });

            executor.execute(pipeline.getNumThreads(), pipeline.getConcurrencyControl());

            /// A cancelled PipelineExecutor may return without exception but with incomplete results.
            /// In this case make sure to:
            ///  * report exception rather than success,
            ///  * do it before destroying the QueryPipeline; otherwise it may fail assertions about
            ///    being unexpectedly destroyed before completion and without uncaught exception
            ///    (specifically, the assert in ~WriteBuffer()).
            if (execution.interrupt_execution.load())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Refresh for view {} cancelled", view_storage_id.getFullTableName());

            /// `executor` must be destroyed before `pipeline`!
        }
        logQueryFinish(*query_log_elem, refresh_context, refresh_query, std::move(pipeline), /*pulling_pipeline=*/false, query_span, QueryResultCacheUsage::None, /*internal=*/false);
        query_log_elem = std::nullopt;
        query_span = nullptr;
    }
    catch (...)
    {
        /// A cancellation is not an error. If we failed before logQueryStart (e.g. access check during
        /// interpretation), there is no started element yet, so report it as an exception-before-start.
        bool cancelled = execution.interrupt_execution.load();
        if (query_log_elem.has_value())
            logQueryException(*query_log_elem, refresh_context, query_stopwatch, refresh_query, query_span, /*internal*/ false, /*log_error*/ !cancelled);
        else
            logExceptionBeforeStart(query_for_logging, normalized_query_hash, refresh_context, /*ast*/ nullptr, query_span, query_stopwatch.elapsedMilliseconds(), /*internal*/ false);
        throw;
    }

    /// Note: the table exchange is deferred and done separately via exchangeTargetTableAfterRefresh()
    /// after all shards have finished writing data.
    return target_table_id.uuid;
}

void RefreshTask::exchangeTargetTableAfterRefresh(const StorageID & target_table_id, bool append, int32_t root_znode_version)
{
    if (append)
        return;

    /// Only the global leader (or a non-coordinated refresh) performs the EXCHANGE.
    if (coordination.coordinated && !coordination.is_global_leader)
        return;

    ContextMutablePtr refresh_context = view->createRefreshContext(/*log_comment*/ "");
    /// Keep the EXCHANGE conditional on the coordination znode version (upstream #104051): if a brief
    /// Keeper disconnect let another replica take over and start a new refresh, the version changed
    /// and this stale EXCHANGE fails instead of clobbering the newer refresh's data.
    refresh_context->setDDLQueryCancellation(execution.cancel_ddl_queries.get_token());
    if (root_znode_version != -1)
        refresh_context->setDDLAdditionalChecksOnEnqueue({zkutil::makeCheckRequest(coordination.path, root_znode_version)});

    /// The returned table_to_drop equals target_table_id (the temporary table's name, which after the
    /// EXCHANGE holds the previous target's data). The caller (executeRefresh) drops it.
    view->exchangeTargetTable(target_table_id, refresh_context);

    LOG_INFO(getLogger(), "Target table exchange completed");
}

void RefreshTask::updateDependenciesIfNeeded(std::unique_lock<std::mutex> & lock)
{
    while (true)
    {
        chassert(lock.owns_lock());
        if (scheduling.dependencies_satisfied_until.time_since_epoch().count() >= 0)
            return;
        auto deps = set_handle.getDependencies();
        if (deps.empty())
        {
            scheduling.dependencies_satisfied_until = std::chrono::sys_seconds::max();
            return;
        }
        scheduling.dependencies_satisfied_until = std::chrono::sys_seconds(std::chrono::seconds(-2));
        lock.unlock();

        /// Consider a dependency satisfied if its next scheduled refresh time is greater than ours.
        /// This seems to produce reasonable behavior in practical cases, e.g.:
        ///  * REFRESH EVERY 1 DAY depends on REFRESH EVERY 1 DAY
        ///    The second refresh starts after the first refresh completes *for the same day*.
        ///  * REFRESH EVERY 1 DAY OFFSET 2 HOUR depends on REFRESH EVERY 1 DAY OFFSET 1 HOUR
        ///    The second refresh starts after the first refresh completes for the same day as well (scheduled 1 hour earlier).
        ///  * REFRESH EVERY 1 DAY OFFSET 1 HOUR depends on REFRESH EVERY 1 DAY OFFSET 23 HOUR
        ///    The dependency's refresh on day X triggers dependent's refresh on day X+1.
        ///  * REFRESH EVERY 2 HOUR depends on REFRESH EVERY 1 HOUR
        ///    The 2 HOUR refresh happens after the 1 HOUR refresh for every other hour, e.g.
        ///    after the 2pm refresh, then after the 4pm refresh, etc.
        ///
        /// We currently don't allow dependencies in REFRESH AFTER case, because its unclear what their meaning should be.

        const RefreshSet & set = view->getContext()->getRefreshSet();
        auto min_ts = std::chrono::sys_seconds::max();
        for (const StorageID & id : deps)
        {
            auto tasks = set.findTasks(id);
            if (tasks.empty())
                min_ts = {}; // missing table, dependency unsatisfied
            else
                min_ts = std::min(min_ts, (*tasks.begin())->getNextRefreshTimeslot());
        }

        lock.lock();

        if (scheduling.dependencies_satisfied_until.time_since_epoch().count() != -2)
        {
            /// Dependencies changed again after we started looking at them. Have to re-check.
            chassert(scheduling.dependencies_satisfied_until.time_since_epoch().count() == -1);
            continue;
        }

        scheduling.dependencies_satisfied_until = min_ts;
        return;
    }
}

static std::chrono::milliseconds backoff(Int64 retry_idx, const RefreshSettings & refresh_settings)
{
    UInt64 delay_ms;
    UInt64 multiplier = UInt64(1) << std::min(retry_idx, Int64(62));
    /// Overflow check: a*b <= c iff a <= c/b iff a <= floor(c/b).
    if (refresh_settings[RefreshSetting::refresh_retry_initial_backoff_ms] <= refresh_settings[RefreshSetting::refresh_retry_max_backoff_ms] / multiplier)
        delay_ms = refresh_settings[RefreshSetting::refresh_retry_initial_backoff_ms] * multiplier;
    else
        delay_ms = refresh_settings[RefreshSetting::refresh_retry_max_backoff_ms];
    return std::chrono::milliseconds(delay_ms);
}

std::tuple<std::chrono::system_clock::time_point, std::chrono::sys_seconds, RefreshTask::CoordinationZnode>
RefreshTask::determineNextRefreshTime(std::chrono::sys_seconds now)
{
    auto znode = coordination.root_znode;
    if (refresh_settings[RefreshSetting::refresh_retries] >= 0 && znode.attempt_number > refresh_settings[RefreshSetting::refresh_retries])
    {
        /// Skip to the next scheduled refresh, as if a refresh succeeded.
        znode.last_completed_timeslot = refresh_schedule.timeslotForCompletedRefresh(znode.last_completed_timeslot, znode.last_attempt_time, znode.last_attempt_time, false);
        znode.attempt_number = 0;
    }
    auto timeslot = refresh_schedule.advance(znode.last_completed_timeslot);

    std::chrono::system_clock::time_point when;
    if (znode.attempt_number == 0)
        when = refresh_schedule.addRandomSpread(timeslot, znode.randomness);
    else
        when = znode.last_attempt_time + backoff(znode.attempt_number - 1, refresh_settings);

    znode.previous_attempt_error = "";
    if (!znode.last_attempt_succeeded && znode.last_attempt_time.time_since_epoch().count() != 0)
    {
        if (znode.last_attempt_error.empty())
            znode.previous_attempt_error = fmt::format("Replica '{}' went away", znode.last_attempt_replica);
        else
            znode.previous_attempt_error = znode.last_attempt_error;
    }

    znode.attempt_number += 1;
    znode.last_attempt_time = now;
    znode.last_attempt_replica = coordination.replica_name;
    znode.last_attempt_error = "";
    znode.last_attempt_succeeded = false;
    znode.refresh_running = true;

    return {when, timeslot, znode};
}

void RefreshTask::scheduleRefresh(std::lock_guard<std::mutex> &)
{
    if (state != RefreshState::Running)
        state = RefreshState::Scheduling;
    scheduling_task->schedule();
}

void RefreshTask::setState(RefreshState s, std::unique_lock<std::mutex> & lock)
{
    chassert(lock.owns_lock());
    state = s;
    if (s != RefreshState::Running && s != RefreshState::Scheduling)
        refresh_cv.notify_all();
}

void RefreshTask::readZnodesIfNeeded(std::shared_ptr<zkutil::ZooKeeper> zookeeper, std::unique_lock<std::mutex> & lock)
{
    chassert(lock.owns_lock());
    if (!coordination.coordinated || !coordination.watches->should_reread_znodes.load())
        return;

    coordination.watches->should_reread_znodes.store(false);
    auto prev_last_completed_timeslot = coordination.root_znode.last_completed_timeslot;

    lock.unlock();

    /// Do separate requests just to add watches.
    /// Unconditional registration (upstream #108234): do not gate on watch_active flags.
    /// No MULTI_READ requirement: Apache ZooKeeper lacks that Keeper feature; tryGet falls
    /// back to per-path reads when the flag is absent (patch-port(050)).
    zookeeper->existsWatch(coordination.path, nullptr, watch_callback);
    zookeeper->getChildrenWatch(coordination.path, nullptr, watch_callback);

    /// Read the znodes (atomic multi-read on ClickHouse Keeper; separate reads on ZooKeeper).
    Strings paths {coordination.path, coordination.path + "/running", coordination.path + "/paused"};
    auto responses = zookeeper->tryGet(paths.begin(), paths.end());

    lock.lock();

    if (responses[0].error != Coordination::Error::ZOK)
        throw Coordination::Exception::fromPath(responses[0].error, paths[0]);
    for (size_t i = 1; i < 3; ++i)
        if (responses[i].error != Coordination::Error::ZOK && responses[i].error != Coordination::Error::ZNONODE)
            throw Coordination::Exception::fromPath(responses[i].error, paths[i]);

    bool running_znode_exists = responses[1].error == Coordination::Error::ZOK;

    coordination.root_znode.parse(responses[0].data, running_znode_exists, getLogger());
    coordination.root_znode.version = responses[0].stat.version;
    coordination.running_znode_exists = running_znode_exists;
    coordination.paused_znode_exists = responses[2].error == Coordination::Error::ZOK;

    /// 066: while a refresh is live (the global leader holds its ephemeral "running" znode and the
    /// root znode points at a refresh directory), remember that directory so this replica's shard
    /// can find it and participate as a shard leader.
    ///
    /// If WE are the global leader currently executing the refresh, executeRefresh() owns
    /// current_refresh_dir (it set it under the lock and the helper functions read it without the
    /// lock). Don't touch it here, otherwise we'd data-race with that unlocked read. For a refresh
    /// owned by another replica (the shard-participation case), we do compute it here.
    bool we_are_running_leader = coordination.root_znode.refresh_running
        && coordination.root_znode.last_attempt_replica == coordination.replica_name
        && execution.state != ExecutionState::State::None;
    if (!we_are_running_leader)
    {
        coordination.current_refresh_dir.clear();
        if (coordination.running_znode_exists && !coordination.root_znode.refresh_dir.empty())
            coordination.current_refresh_dir = coordination.path + "/" + coordination.root_znode.refresh_dir;
    }

    if (coordination.root_znode.last_completed_timeslot != prev_last_completed_timeslot)
    {
        lock.unlock();
        view->getContext()->getRefreshSet().notifyDependents(view->getStorageID());
        lock.lock();
    }
}

bool RefreshTask::updateCoordinationState(CoordinationZnode root, bool running, std::shared_ptr<zkutil::ZooKeeper> zookeeper, std::unique_lock<std::mutex> & lock, bool only_running_znode)
{
    chassert(lock.owns_lock());
    int32_t version = -1;
    if (coordination.coordinated)
    {
        Coordination::Requests ops;
        if (only_running_znode)
            ops.emplace_back(zkutil::makeCheckRequest(coordination.path, root.version));
        else
            ops.emplace_back(zkutil::makeSetRequest(coordination.path, root.toString(), root.version));

        /// Aiven patch N02: `ignore_if_exists` keeps the "running" create idempotent across keeper
        /// reconnects (upstream #104051) by serializing it as the ClickHouse-Keeper-only
        /// `CreateIfNotExists` op (OpNum 502). Apache ZooKeeper cannot parse that op inside a `multi`
        /// and aborts the whole transaction with a marshalling error, which ClickHouse treats as a
        /// hardware error and finalizes the shared session — forcing every replicated table on the
        /// node into readonly. When the server doesn't advertise the `CREATE_IF_NOT_EXISTS` feature
        /// flag (i.e. real ZooKeeper), emulate the op's "no-op if already present" semantics with a
        /// plain create gated on an existence pre-check, keeping the batch a set/check/create that
        /// ZooKeeper accepts. On ClickHouse Keeper the flag is advertised and behavior is unchanged.
        /// The pre-check (rather than tolerating `ZNODEEXISTS` post-hoc) is required because `multi`
        /// is atomic: a plain create that hit `ZNODEEXISTS` would roll back the sibling set/check too.
        const bool create_if_not_exists = zookeeper->isFeatureEnabled(DB::KeeperFeatureFlag::CREATE_IF_NOT_EXISTS);
        const String running_path = coordination.path + "/running";
        const String running_replica_name = coordination.replica_name;
        const bool running_znode_exists = coordination.running_znode_exists;

        Coordination::Responses responses;

        lock.unlock();
        if (running)
        {
            if (create_if_not_exists || !zookeeper->exists(running_path))
                ops.emplace_back(zkutil::makeCreateRequest(running_path, running_replica_name, zkutil::CreateMode::Ephemeral, /*ignore_if_exists=*/ create_if_not_exists));
        }
        else
        {
            /// (Avoid `try_remove = true` because it requires a keeper feature flag TRY_REMOVE that we're otherwise not using.)
            if (running_znode_exists)
                ops.emplace_back(zkutil::makeRemoveRequest(running_path, -1));
        }
        auto code = zookeeper->tryMulti(ops, responses);
        lock.lock();

        if (running && responses[0]->error == Coordination::Error::ZBADVERSION)
        {
            /// Lost the race, this is normal, don't log a stack trace.
            /// Trigger a re-read of znodes just in case, though it shouldn't be necessary because of watches.
            /// (Can we get into a situation where such re-reads keep returning stale data, and
            ///  write attempts keep failing with version mismatch, and we keep needlessly
            ///  busy-waiting and DOSing keeper?
            ///  Based on how keeper server works, this shouldn't happen: keeper provides
            ///  read-after-write consistency within a session even for failed writes. The next read
            ///  should always see the newer znode version that caused the conflict. Idk whether
            ///  this is the case in vanilla zookeeper as well.)
            coordination.watches->should_reread_znodes.store(true);
            scheduling_task->schedule();
            return false;
        }
        zkutil::KeeperMultiException::check(code, ops, responses);
        if (only_running_znode)
            version = root.version;
        else
            version = dynamic_cast<Coordination::SetResponse &>(*responses[0]).stat.version;
    }
    coordination.root_znode = root;
    coordination.root_znode.version = version;
    coordination.running_znode_exists = running;
    return true;
}

void RefreshTask::interruptExecution()
{
    chassert(!mutex.try_lock());
    std::shared_ptr<QueryStatus> query_status;
    {
        std::unique_lock lock(execution.executor_mutex);
        if (execution.interrupt_execution.exchange(true))
            return;
        query_status = execution.executing_query_status;
        if (execution.executor)
        {
            execution.executor->cancel();
            LOG_DEBUG(getLogger(), "Cancelling refresh in {}", set_handle.getID().getFullNameNotQuoted());
        }
    }

    /// Also mark the refresh query killed, not just cancel the pipeline: a refresh blocked in I/O
    /// (e.g. a filesystem-cache download wait) doesn't observe pipeline cancellation and would keep
    /// running, so shutdown()'s deactivate() — and any DROP driving it, including SharedCatalog
    /// state apply — would block until the I/O returned on its own. Done outside executor_mutex
    /// because cancelQuery() cancels registered executors, which take their own locks.
    if (query_status)
        query_status->cancelQuery(CancelReason::CANCELLED_BY_USER);
}

std::tuple<StoragePtr, TableLockHolder> RefreshTask::getAndLockTargetTable(const StorageID & storage_id, const ContextPtr & context)
{
    ///  1. Get table by name.
    ///  2. Check that it's not dropped locally.
    ///     (After that, it can't be dropped during the query because we're holding StoragePtr and
    ///      TableLockHolder.)
    ///     If this fails, retry and expect to see a different table by the same name.
    ///  3. Do SYSTEM SYNC REPLICA. May fail if the table is being dropped.
    ///     If this fails, retry until we see a different table by the same name.

    StoragePtr prev_storage;
    bool prev_table_dropped_locally = false;
    std::exception_ptr exception;

    for (int attempt = 0; attempt < 10; ++attempt)
    {
        if (attempt > 0)
        {
            if (prev_table_dropped_locally)
            {
                ProfileEvents::increment(ProfileEvents::RefreshableViewLockTableRetry);
            }
            else
            {
                /// We're waiting for DatabaseReplicated to catch up and see the new table.
                ProfileEvents::increment(ProfileEvents::RefreshableViewSyncReplicaRetry);
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }

        StoragePtr storage = DatabaseCatalog::instance().getTable(storage_id, context);

        if (storage == prev_storage)
        {
            if (prev_table_dropped_locally)
                // Table was dropped but is still accessible in DatabaseCatalog.
                // Either ABA problem or something's broken. Don't retry.
                break;
            continue;
        }
        prev_storage = storage;

        TableLockHolder storage_lock = storage->tryLockForShare(context->getCurrentQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);
        if (!storage_lock)
        {
            prev_table_dropped_locally = true;
            continue;
        }

        if (coordination.coordinated)
        {
            std::lock_guard lock(replica_sync_mutex);
            UUID uuid = storage->getStorageID().uuid;
            if (uuid != last_synced_inner_uuid)
            {
                try
                {
                    InterpreterSystemQuery::trySyncReplica(storage, SyncReplicaMode::DEFAULT, {}, context);
                    ProfileEvents::increment(ProfileEvents::RefreshableViewSyncReplicaSuccess);
                }
                catch (Exception & e)
                {
                    if (e.code() != ErrorCodes::ABORTED)
                        throw;

                    /// Work around this race condition:
                    ///  1. Another replica does a refresh: create table X, insert, rename.
                    ///  2. This replica sees table X, but not its data yet.
                    ///  3. Another replica does another refresh: create table Y, insert, rename,
                    ///     drop table X.
                    ///  4. This replica's DatabaseReplicated shuts down table X, and the
                    ///     trySyncReplica fails with "Shutdown is called for table" exception.
                    ///     X may still not have all data. ReplicatedMergeTree shutdown stops
                    ///     data part exchange, so there's no hope of getting all data out of X.
                    /// In this case we retry table lookup in hopes of seeing the new table Y.
                    LOG_DEBUG(getLogger(), "Retrying after exception when syncing replica: {}", e.message());
                    exception = std::current_exception();
                    prev_table_dropped_locally = false;
                    continue;
                }

                /// (Race condition: this may revert from a newer uuid to an older one. This doesn't
                ///  break anything, just causes an unnecessary sync. Should be rare.)
                last_synced_inner_uuid = uuid;
            }
        }

        return {storage, storage_lock};
    }

    if (prev_table_dropped_locally)
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {} is dropped or detached", storage_id.getFullNameNotQuoted());
    else
        std::rethrow_exception(exception);
}

std::chrono::system_clock::time_point RefreshTask::currentTime() const
{
    Int64 fake = scheduling.fake_clock.load(std::memory_order::relaxed);
    if (fake == INT64_MIN)
        return std::chrono::system_clock::now();
    return std::chrono::system_clock::time_point(std::chrono::seconds(fake));
}

void RefreshTask::setRefreshSetHandleUnlock(RefreshSet::Handle && set_handle_)
{
    set_handle = std::move(set_handle_);
}

void RefreshTask::CoordinationZnode::randomize()
{
    randomness = std::uniform_int_distribution<Int64>(Int64(-1e9), Int64(1e9))(thread_local_rng);
}

String RefreshTask::CoordinationZnode::toString() const
{
    /// "format version" should be incremented when making incompatible change, to make older
    /// servers refuse to parse it. We should probably never do that.
    ///
    /// For backwards compatible changes, just add new fields at the end (!), and old servers will
    /// ignore them.
    ///
    /// Removing a field is complicated enough that maybe we should never do it. Procedure would be:
    ///  1. Make the field optional in `parse` but keep writing it here.
    ///  2. Update all servers and make sure they update all RMV znodes (e.g. do a refresh).
    ///  3. Stop writing the field here but keep recognizing (and ignoring) it in `parse`.
    ///  4. Update all servers etc.
    ///  5. Remove the field from `parse`.

    WriteBufferFromOwnString out;
    out << "format version: 1\n"
        << "last_completed_timeslot: " << Int64(last_completed_timeslot.time_since_epoch().count()) << "\n"
        << "last_success_time: " << Int64(last_success_time.time_since_epoch().count()) << "\n"
        << "last_success_duration_ms: " << Int64(last_success_duration.count()) << "\n"
        << "last_success_table_uuid: " << last_success_table_uuid << "\n"
        << "last_attempt_time: " << Int64(last_attempt_time.time_since_epoch().count()) << "\n"
        << "last_attempt_replica: " << escape << last_attempt_replica << "\n"
        << "last_attempt_error: " << escape << last_attempt_error << "\n"
        << "last_attempt_succeeded: " << last_attempt_succeeded << "\n"
        << "previous_attempt_error: " << escape << previous_attempt_error << "\n"
        << "attempt_number: " << attempt_number << "\n"
        << "randomness: " << randomness << "\n"
        << "refresh_running: " << refresh_running << "\n"
        /// 066 fields, appended at the end so older servers ignore them.
        << "refresh_dir: " << escape << refresh_dir << "\n"
        << "target_table_id: " << escape << target_table_id << "\n";
    return out.str();
}

void RefreshTask::CoordinationZnode::parse(const String & data, bool running_znode_exists, const LoggerPtr & log_)
{
    ReadBufferFromString in(data);

    String next_field_name;
    auto advance_to_next_field = [&]
    {
        next_field_name.clear();
        if (in.eof())
            return;
        assertChar('\n', in);
        if (in.eof())
            return;
        readStringUntilColon(next_field_name, in);
        assertString(": ", in);
    };
    auto try_read_field = [&](const char * name, auto & out) -> bool
    {
        using T = std::remove_reference_t<decltype(out)>;

        if (next_field_name != name)
            return false;

        if constexpr (std::is_same_v<T, std::string>)
        {
            in >> escape >> out;
        }
        else if constexpr (std::is_same_v<T, std::chrono::sys_seconds>)
        {
            Int64 v;
            in >> v;
            out = std::chrono::sys_seconds(std::chrono::seconds(v));
        }
        else if constexpr (std::is_same_v<T, std::chrono::milliseconds>)
        {
            Int64 v;
            in >> v;
            out = std::chrono::milliseconds(v);
        }
        else
        {
            in >> out;
        }

        advance_to_next_field();
        return true;
    };

    auto required_field = [&](const char * name, auto & out)
    {
        if (!try_read_field(name, out))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "RMV coordination znode fields are missing or reordered: not found field '{}'", name);
    };
    auto optional_field = [&](const char * name, auto & out, auto default_value)
    {
        if (!try_read_field(name, out))
            out = default_value;
    };

    in >> "format version: 1";
    advance_to_next_field();

    required_field("last_completed_timeslot", last_completed_timeslot);
    required_field("last_success_time", last_success_time);
    required_field("last_success_duration_ms", last_success_duration);
    required_field("last_success_table_uuid", last_success_table_uuid);
    required_field("last_attempt_time", last_attempt_time);
    required_field("last_attempt_replica", last_attempt_replica);
    required_field("last_attempt_error", last_attempt_error);
    required_field("last_attempt_succeeded", last_attempt_succeeded);
    required_field("previous_attempt_error", previous_attempt_error);
    required_field("attempt_number", attempt_number);
    required_field("randomness", randomness);
    optional_field("refresh_running", refresh_running, running_znode_exists);
    /// 066 fields. Optional so a znode written by a server without 066 (no refresh_dir) still parses.
    optional_field("refresh_dir", refresh_dir, String());
    optional_field("target_table_id", target_table_id, String());

    if (!next_field_name.empty())
    {
        LOG_INFO(log_, "Unrecognized field '{}' in RMV coordination znode. Maybe the znode was written by a newer version of the server that added this field, or maybe parsing is broken.", next_field_name);
    }
}

void RefreshTask::cleanupOldRefreshDirectories(std::shared_ptr<zkutil::ZooKeeper> zookeeper, std::chrono::seconds max_age)
{
    if (!coordination.coordinated)
        return;

    auto now = std::chrono::system_clock::now();
    auto cutoff = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count() - max_age.count();

    Strings children;
    auto code = zookeeper->tryGetChildren(coordination.path, children);
    if (code != Coordination::Error::ZOK)
        return;

    for (const auto & child : children)
    {
        if (child.starts_with("refresh_"))
        {
            try
            {
                /// Extract timestamp from "refresh_<timestamp>"
                Int64 timestamp = std::stoll(child.substr(8));
                if (timestamp < cutoff)
                {
                    String refresh_path = coordination.path + "/" + child;

                    /// Remove all children first
                    Strings refresh_children;
                    if (zookeeper->tryGetChildren(refresh_path, refresh_children) == Coordination::Error::ZOK)
                    {
                        for (const auto & refresh_child : refresh_children)
                            zookeeper->tryRemove(refresh_path + "/" + refresh_child);
                    }
                    zookeeper->tryRemove(refresh_path);
                }
            }
            catch (const std::exception & e)
            {
                LOG_WARNING(getLogger(), "Failed to parse or clean up refresh directory {}: {}", child, e.what());
            }
        }
    }
}

void RefreshTask::createRefreshDirectory(std::shared_ptr<zkutil::ZooKeeper> zookeeper, String suggested_refresh_dir)
{
    /// In non-coordinated mode there is no Keeper session (zookeeper is null) and no shard tree to
    /// populate, so there is nothing to create. Every other coordination helper guards itself the
    /// same way; this one was missing the guard in the original patch, which made a single-shard
    /// refresh dereference a null Keeper handle.
    if (!coordination.coordinated)
        return;

    /// current_refresh_dir was already set under the lock in executeRefresh() before this task was
    /// unlocked; we only create the corresponding znode here. (Fall back to the suggested name in
    /// the unlikely case it wasn't set, to stay robust.)
    if (coordination.current_refresh_dir.empty())
        coordination.current_refresh_dir = coordination.path + "/" + suggested_refresh_dir;
    auto code = zookeeper->tryCreate(coordination.current_refresh_dir, "", zkutil::CreateMode::Persistent);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
        throw Coordination::Exception::fromPath(code, coordination.current_refresh_dir);
}

bool RefreshTask::tryBecomeGlobalLeader(std::shared_ptr<zkutil::ZooKeeper> zookeeper, String suggested_refresh_dir)
{
    /// Note: in the merged state machine the global leader is elected atomically inside
    /// updateCoordinationState() (set root znode + create the ephemeral "running" znode, conditional
    /// on the root znode version), which preserves upstream #104051's keeper-loss safety. This helper
    /// is retained for the `refresh_task_stop_racing_for_running_refresh` failpoint and parity with
    /// the patch, but is not on the main scheduling path.
    if (!coordination.coordinated)
    {
        coordination.is_global_leader = true;
        return true;
    }
    String running_data = coordination.replica_name + "\n" + suggested_refresh_dir;
    bool stop_racing_for_running_refresh = false;
    fiu_do_on(FailPoints::refresh_task_stop_racing_for_running_refresh, { stop_racing_for_running_refresh = true; });
    if (stop_racing_for_running_refresh)
        return false;
    auto code = zookeeper->tryCreate(coordination.path + "/running", running_data, zkutil::CreateMode::Ephemeral);
    if (code == Coordination::Error::ZOK)
    {
        coordination.is_global_leader = true;
        coordination.running_znode_exists = true;
        LOG_DEBUG(getLogger(), "Became global leader for refresh");
        return true;
    }
    else if (code == Coordination::Error::ZNODEEXISTS)
    {
        coordination.is_global_leader = false;
        coordination.running_znode_exists = true;
        return false;
    }
    else
    {
        throw Coordination::Exception::fromPath(code, coordination.path + "/running");
    }
}

bool RefreshTask::tryBecomeShardLeader(std::shared_ptr<zkutil::ZooKeeper> zookeeper)
{
    if (!coordination.coordinated)
    {
        coordination.is_shard_leader = true;
        return true;
    }

    if (coordination.current_refresh_dir.empty() && !coordination.is_global_leader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot become shard leader without a current refresh directory");

    /// Try to create znode for this shard in the refresh directory
    String shard_leader_path = coordination.current_refresh_dir + "/" + coordination.shard_name;

    auto code = zookeeper->tryCreate(shard_leader_path, coordination.replica_name, zkutil::CreateMode::Ephemeral);
    if (code == Coordination::Error::ZOK)
    {
        coordination.is_shard_leader = true;
        LOG_DEBUG(getLogger(), "Became shard leader for shard {}", coordination.shard_name);
        return true;
    }
    else if (code == Coordination::Error::ZNODEEXISTS)
    {
        /// Another replica of this shard is already the shard leader.
        coordination.is_shard_leader = false;
        return false;
    }
    else if (code == Coordination::Error::ZNONODE)
    {
        /// The global leader has not created the refresh directory znode yet (it sets refresh_dir in
        /// the root znode before creating the directory). Not an error: retry on the next scheduling
        /// cycle (the children watch on the root path fires when the directory appears).
        coordination.is_shard_leader = false;
        return false;
    }
    else
    {
        throw Coordination::Exception::fromPath(code, shard_leader_path);
    }
}

bool RefreshTask::isCurrentRefreshStillActive(std::shared_ptr<zkutil::ZooKeeper> zookeeper)
{
    if (!coordination.coordinated)
        return true;

    if (coordination.current_refresh_dir.empty())
        return false;

    const String prefix = coordination.path + "/";
    String expected_refresh_dir = coordination.current_refresh_dir;
    if (expected_refresh_dir.starts_with(prefix))
        expected_refresh_dir = expected_refresh_dir.substr(prefix.size());

    /// The refresh is live only while the global leader still holds its ephemeral "running" znode.
    if (!zookeeper->exists(coordination.path + "/running"))
    {
        LOG_INFO(getLogger(),
            "Refresh {} is stale: 'running' znode no longer exists while waiting as shard {}",
            coordination.current_refresh_dir,
            coordination.shard_name);
        return false;
    }

    String root_data;
    if (!zookeeper->tryGet(coordination.path, root_data))
        throw Coordination::Exception::fromPath(Coordination::Error::ZNONODE, coordination.path);

    CoordinationZnode root_znode;
    root_znode.parse(root_data, /*running_znode_exists=*/ true, getLogger());
    if (root_znode.refresh_dir != expected_refresh_dir)
    {
        LOG_INFO(getLogger(),
            "Refresh {} is stale: root znode points to {} while shard {} expected {}",
            coordination.current_refresh_dir,
            root_znode.refresh_dir,
            coordination.shard_name,
            expected_refresh_dir);
        return false;
    }

    return true;
}

StorageID RefreshTask::getOrWaitForTemporaryTableID(std::shared_ptr<zkutil::ZooKeeper> zookeeper, const StorageID & table_id_to_store)
{
    if (!coordination.coordinated)
        return table_id_to_store;

    if (coordination.current_refresh_dir.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get temporary table without a current refresh directory");

    String temp_table_path = coordination.current_refresh_dir + "/temporary_table";

    /// If we're the global leader, write the temporary table ID (name + UUID)
    /// Using UUID for synchronization ensures all shards work with the exact same table,
    /// regardless of DDL replication timing or table renames.
    if (coordination.is_global_leader && !table_id_to_store.empty())
    {
        String data = toString(table_id_to_store.uuid);
        auto code = zookeeper->tryCreate(temp_table_path, data, zkutil::CreateMode::Persistent);
        if (code != Coordination::Error::ZOK)
            throw Coordination::Exception::fromPath(code, temp_table_path);
        LOG_DEBUG(getLogger(), "Global leader created temporary_table znode, code={}", static_cast<int>(code));
        coordination.temporary_table_name = table_id_to_store.table_name;
        return table_id_to_store;
    }

    const int sleep_ms = 100;

    /// Wait for the temporary table ID to be available
    for (int attempt = 0; attempt < RefreshTimeout::REFRESH_TIMEOUT_SEC * 1000 / sleep_ms; ++attempt)
    {
        if (attempt % (1000 / sleep_ms) == 0 && !isCurrentRefreshStillActive(zookeeper))
            return StorageID::createEmpty();

        String data;
        if (attempt > 0)
            ProfileEvents::increment(ProfileEvents::RefreshableViewSyncReplicaRetry);
        if (zookeeper->tryGet(temp_table_path, data) && !data.empty())
        {
            if (execution.interrupt_execution.load())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Refresh cancelled while waiting for temporary table");
            UUID uuid = parseFromString<UUID>(data);
            // find table by uuid
            auto [db, table] = DatabaseCatalog::instance().tryGetByUUID(uuid);
            if (table)
            {
                // Wait till table is created and ready to be used
                auto storage_id = table->getStorageID();
                if (!storage_id.table_name.starts_with(".tmp_replace_"))
                {
                    LOG_DEBUG(getLogger(), "Found temporary table by UUID {}: {}", uuid, storage_id.getFullTableName());
                    return storage_id;
                }
            }
        }

        if (execution.interrupt_execution.load())
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Refresh cancelled while waiting for temporary table");

        if (attempt % (5000 / sleep_ms) == 0)  // Log every 5 seconds
            LOG_INFO(getLogger(), "Waiting for temporary table znode at {} (attempt {})", temp_table_path, attempt);

        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Timeout waiting for temporary table znode at {}", temp_table_path);
}

void RefreshTask::markShardFinished(std::shared_ptr<zkutil::ZooKeeper> zookeeper)
{
    if (!coordination.coordinated)
        return;

    if (coordination.current_refresh_dir.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot mark shard finished without a current refresh directory");

    /// Store finished status in the current refresh directory
    String finished_dir = coordination.current_refresh_dir + "/finished";
    zookeeper->tryCreate(finished_dir, "", zkutil::CreateMode::Persistent);

    String finished_path = finished_dir + "/" + coordination.shard_name;

    /// Create the finished znode to signal completion
    auto code = zookeeper->tryCreate(finished_path, "", zkutil::CreateMode::Persistent);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
        throw Coordination::Exception::fromPath(code, finished_path);

    LOG_DEBUG(getLogger(), "Marked shard {} as finished in {}", coordination.shard_name, finished_path);
}

size_t RefreshTask::getAllShardsCount(std::shared_ptr<zkutil::ZooKeeper> /* zookeeper */)
{
    if (!coordination.coordinated)
    {
        return 1;
    }

    /// Get shard names from the cluster configuration rather than ZooKeeper.
    /// This provides a consistent view of all shards and avoids race conditions
    try
    {
        const auto database = DatabaseCatalog::instance().getDatabase(view->getStorageID().database_name);
        const auto * replicated_db = dynamic_cast<const DatabaseReplicated *>(database.get());
        if (!replicated_db)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Database {} is not a replicated database", view->getStorageID().database_name);
        ClusterPtr cluster = replicated_db->tryGetCluster();
        if (!cluster)
            /// A missing cluster is NOT a logic-invariant violation: `tryGetCluster` returns null while
            /// the `DatabaseReplicated` is being dropped/detached (its Keeper cluster group is gone), and
            /// a coordinated refresh can still be mid-flight during that teardown window. Raising
            /// LOGICAL_ERROR here mislabels a benign shutdown race as a programming bug — it pollutes the
            /// logs and makes the integration harness fail the whole suite (it greps server logs for
            /// `LOGICAL_ERROR`). Use ABORTED so the refresh unwinds cleanly and the drop proceeds.
            throw Exception(ErrorCodes::ABORTED, "Cluster not found for database {} (being dropped?)", view->getStorageID().database_name);
        return cluster->getShardsInfo().size();
    }
    catch (...)
    {
        LOG_WARNING(getLogger(), "Failed to get shards from cluster: {}", getCurrentExceptionMessage(true));
        throw;
    }
}

bool RefreshTask::checkAllShardsFinished(std::shared_ptr<zkutil::ZooKeeper> zookeeper)
{
    if (!coordination.coordinated)
        return true;

    if (coordination.current_refresh_dir.empty())
        return false;

    auto all_shards = getAllShardsCount(zookeeper);
    if (all_shards == 1)
        return true;

    String finished_dir = coordination.current_refresh_dir + "/finished";

    size_t num_finished_shards = 0;
    Strings finished_children;
    if (zookeeper->tryGetChildren(finished_dir, finished_children) == Coordination::Error::ZOK)
    {
        num_finished_shards = finished_children.size();
    }
    else
    {
        /// The "finished" directory is created by the first shard to finish (markShardFinished). Until
        /// then there are simply no finished shards yet.
        return false;
    }
    if (num_finished_shards >= all_shards)
        return true;
    return false;
}

void RefreshTask::cleanupRefreshDirectory(std::shared_ptr<zkutil::ZooKeeper> zookeeper)
{
    if (!coordination.coordinated || coordination.current_refresh_dir.empty())
        return;

    /// Remove the finished subdirectory and its children
    String finished_dir = coordination.current_refresh_dir + "/finished";
    Strings finished_children;
    if (zookeeper->tryGetChildren(finished_dir, finished_children) == Coordination::Error::ZOK)
    {
        for (const auto & child : finished_children)
            zookeeper->tryRemove(finished_dir + "/" + child);
    }
    zookeeper->tryRemove(finished_dir);

    /// Remove all other children of the refresh directory
    Strings children;
    if (zookeeper->tryGetChildren(coordination.current_refresh_dir, children) == Coordination::Error::ZOK)
    {
        for (const auto & child : children)
            zookeeper->tryRemove(coordination.current_refresh_dir + "/" + child);
    }
    zookeeper->tryRemove(coordination.current_refresh_dir);

    coordination.current_refresh_dir.clear();
}

}
