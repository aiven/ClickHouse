#include <Storages/MaterializedView/RefreshTask.h>

#include <Common/CurrentMetrics.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <Common/Macros.h>
#include <Common/thread_local_rng.h>
#include <Core/ServerSettings.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSystemQuery.h>
#include <Interpreters/ProcessList.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/queryNormalization.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Storages/StorageMaterializedView.h>


namespace CurrentMetrics
{
    extern const Metric RefreshingViews;
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
}

namespace RefreshTimeout
{
    extern const int REFRESH_TIMEOUT_SEC = 60 * 60 * 2; // 2 hours
}

/*
 The RefreshTask class is responsible for refreshing a materialized view.
 It can be executed in a sharded environment where each shard can consist of multiple replicas.

 How it works:
 On a scheduled time, the RefreshTask will be executed on every replica in every shard.
 This replica will be the leader for all shards.
 It will grab the lock by creating ephemeral "running" znode with current timestamp.
 Then it will delete all old "refresh_<timestamp>" directories older then 24 hours.
 Then it will create a new temporary table and write its name to the "refresh_<timestamp>/temporary_table" znode.
 After that, every shard will read the temporary table name and will try to write new data to it.
 Every shard will elect its leader by creating a znode with its name "refresh_<timestamp>/<shard_name>".
 If the node creation is successful, the shard will be the leader and will start writing the new data to the temporary table.
 If the node creation is not successful, the shard will wait for another shard leader to finish writing the data.
 After the shard leader finishes writing the data, it will create another node with its name "shards/<shard_name>/finished".
 The global leader will be participating in the data renewal process as well.

 After that, the leader will check if all shards have finished writing the data.
 If all shards have finished writing the data, the leader will swap the temporary table with the main table and delete the "running" znode.
 After that, the leader will delete the "refresh_<timestamp>" directory.
*/

RefreshTask::RefreshTask(
    StorageMaterializedView * view_, ContextPtr context, const DB::ASTRefreshStrategy & strategy, bool  /*attach*/, bool coordinated, bool empty, bool is_restore_from_backup)
    : log(getLogger("RefreshTask"))
    , view(view_)
    , refresh_schedule(strategy)
    , refresh_append(strategy.append)
{
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
        LOG_DEBUG(log, "Coordination path: {}", coordination.path);
        if (const auto * replicated_db = dynamic_cast<const DatabaseReplicated *>(database.get()))
        {
            info.shard = replicated_db->getShardName();
            coordination.shard_name = replicated_db->getShardName();
        }
        coordination.replica_name = context->getMacros()->expand(server_settings[ServerSetting::default_replica_name], info);

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

    task->refresh_task = context->getSchedulePool().createTask("RefreshTask",
        [self = task.get()] { self->refreshTask(); });

    if (strategy.dependencies)
        for (auto && dependency : strategy.dependencies->children)
            task->initial_dependencies.emplace_back(dependency->as<const ASTTableIdentifier &>());

    return OwnedRefreshTask(task);
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
    /// Without this we can deadlock waiting for refresh_task because this shutdown happens from the same DDL thread for which CREATE/EXCHANGE/DROP wait.
    execution.cancel_ddl_queries.request_stop();

    /// Wait for the task to return and prevent it from being scheduled in future.
    refresh_task->deactivate();

    /// Remove from RefreshSet on DROP, without waiting for the IStorage to be destroyed.
    /// This matters because a table may get dropped and immediately created again with the same name,
    /// while the old table's IStorage still exists (pinned by ongoing queries).
    /// (Also, RefreshSet holds a shared_ptr to us.)
    std::lock_guard guard(mutex);
    set_handle.reset();

    view = nullptr;
}

void RefreshTask::drop(ContextPtr context)
{
    if (coordination.coordinated)
    {
        auto zookeeper = context->getZooKeeper();

        /// Redundant, refreshTask() is supposed to clean up after itself, but let's be paranoid.
        removeRunningZnodeIfMine(zookeeper);

        /// If no replicas left, remove the coordination znode.
        Coordination::Requests ops;
        String paused_path = coordination.path + "/paused";
        if (zookeeper->exists(paused_path))
            ops.emplace_back(zkutil::makeRemoveRequest(paused_path, -1));
        ops.emplace_back(zkutil::makeRemoveRequest(coordination.path, -1));
        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(ops, responses);
        if (responses[0]->error != Coordination::Error::ZNOTEMPTY && responses[0]->error != Coordination::Error::ZNONODE)
            zkutil::KeeperMultiException::check(code, ops, responses);
    }
}

void RefreshTask::rename(StorageID new_id, StorageID new_inner_table_id)
{
    std::lock_guard guard(mutex);
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
    return Info {.view_id = set_handle.getID(), .state = state, .next_refresh_time = next_refresh_time, .znode = coordination.root_znode, .refresh_running = coordination.running_znode_exists, .progress = execution.progress.getValues()};
}

void RefreshTask::start()
{
    std::lock_guard guard(mutex);
    if (!std::exchange(scheduling.stop_requested, false))
        return;
    scheduleRefresh(guard);
}

void RefreshTask::stop()
{
    std::lock_guard guard(mutex);
    if (std::exchange(scheduling.stop_requested, true))
        return;
    interruptExecution();
    refresh_task->schedule();
}

void RefreshTask::startReplicated()
{
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
    refresh_task->schedule();
}

void RefreshTask::wait()
{
    auto throw_if_error = [&]
    {
        if (!view)
            throw Exception(ErrorCodes::TABLE_IS_DROPPED, "The table was dropped or detached");
        if (!coordination.running_znode_exists && !coordination.root_znode.last_attempt_succeeded && coordination.root_znode.last_attempt_time.time_since_epoch().count() != 0)
            throw Exception(ErrorCodes::REFRESH_FAILED,
                "Refresh failed{}: {}", coordination.coordinated ? " (on replica " + coordination.root_znode.last_attempt_replica + ")" : "",
                coordination.root_znode.last_attempt_error.empty() ? "Replica went away" : coordination.root_znode.last_attempt_error);
    };
    auto start_time = std::chrono::steady_clock::now();
    auto wait_till = start_time + std::chrono::seconds(RefreshTimeout::REFRESH_TIMEOUT_SEC);

    std::unique_lock lock(mutex);
    refresh_cv.wait(lock, [&] {
        auto now = std::chrono::steady_clock::now();
        if (now > wait_till)
            throw Exception(ErrorCodes::REFRESH_FAILED, "Refresh failed while waiting for status. Current state: {}", magic_enum::enum_name(state));
        return state != RefreshState::Running && state != RefreshState::Scheduling &&
            state != RefreshState::RunningOnAnotherReplica && !scheduling.out_of_schedule_refresh_requested;
    });
    throw_if_error();

    if (coordination.coordinated && !refresh_append)
    {
        /// Wait until we see the table produced by the latest refresh.
        while (true)
        {
            auto now = std::chrono::steady_clock::now();
            UUID expected_table_uuid = coordination.root_znode.last_success_table_uuid;
            if (now > wait_till)
                throw Exception(ErrorCodes::REFRESH_FAILED, "Refresh failed while waiting for table. Current state: {} table uuid: {}",
                magic_enum::enum_name(state), expected_table_uuid);
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
    refresh_task->schedule();
}

void RefreshTask::setFakeTime(std::optional<Int64> t)
{
    std::unique_lock lock(mutex);
    scheduling.fake_clock.store(t.value_or(INT64_MIN), std::memory_order_relaxed);
    /// Reschedule task with shorter delay if currently scheduled.
    refresh_task->scheduleAfter(100, /*overwrite*/ true, /*only_if_scheduled*/ true);
}

void RefreshTask::refreshTask()
{
    std::unique_lock lock(mutex);

    auto schedule_keeper_retry = [&] {
        chassert(lock.owns_lock());
        chassert(state == RefreshState::Scheduling);
        coordination.watches->should_reread_znodes.store(true);
        refresh_task->scheduleAfter(5000);
    };

    try
    {
        bool refreshed_just_now = false;
        /// Whoever breaks out of this loop should assign state.
        while (true)
        {
            setState(RefreshState::Scheduling, lock);
            execution.interrupt_execution.store(false);

            updateDependenciesIfNeeded(lock);

            std::shared_ptr<zkutil::ZooKeeper> zookeeper;
            if (coordination.coordinated)
                zookeeper = view->getContext()->getZooKeeper();
            readZnodesIfNeeded(zookeeper, lock);
            chassert(lock.owns_lock());

            /// Check if another replica is already running a refresh.
            /// In sharded mode, we still participate even if not the global leader.
            if (coordination.running_znode_exists && !coordination.is_global_leader)
            {
                if (coordination.root_znode.last_attempt_replica == coordination.replica_name)
                {
                    LOG_ERROR(log, "Znode {} indicates that this replica is running a refresh, but it isn't. Likely a bug.", coordination.path + "/running");
#ifdef DEBUG_OR_SANITIZER_BUILD
                    abortOnFailedAssertion("Unexpected refresh lock in keeper");
#else
                    coordination.running_znode_exists = false;
                    if (coordination.coordinated)
                        removeRunningZnodeIfMine(zookeeper);
                    schedule_keeper_retry();
                    break;
#endif
                }
                else
                {
                    /// Another replica is the global leader, but we may still be a shard leader.
                    /// Check if there's an active refresh directory we should participate in.
                    if (coordination.coordinated && !coordination.current_refresh_dir.empty())
                    {
                        /// Try to become shard leader and participate in the refresh.
                        lock.unlock();

                        // Get temporary table name first, to avoid race condition with global leader
                        [[maybe_unused]] String temp_table = getOrWaitForTemporaryTable(zookeeper);
                        LOG_DEBUG(log, "Shard {} got temporary table: {}", coordination.shard_name, temp_table);

                        bool became_shard_leader = tryBecomeShardLeader(zookeeper);
                        if (became_shard_leader)
                        {
                            LOG_DEBUG(log, "Participating as shard leader for shard {}, current_refresh_dir: {}", coordination.shard_name, coordination.current_refresh_dir);

                            /// Execute our part of the refresh (write data to temporary table)
                            /// Note: In sharded mode, each shard writes its local data to the shared
                            /// temporary table. The shard leader will execute the query and write data.
                            try
                            {
                                executeRefreshUnlocked(refresh_append, -1);
                                markShardFinished(zookeeper);
                            }
                            catch (...)
                            {
                                LOG_ERROR(log, "Shard {} failed to write data: {}", coordination.shard_name, getCurrentExceptionMessage(true));
                            }

                            coordination.is_shard_leader = false;
                        }
                        else
                        {
                            /// Wait for another replica in our shard to finish
                            LOG_DEBUG(log, "Another replica is shard leader for shard {}, waiting", coordination.shard_name);
                        }

                        lock.lock();
                    }

                    setState(RefreshState::RunningOnAnotherReplica, lock);
                    break;
                }
            }

            chassert(lock.owns_lock());

            if (scheduling.stop_requested || coordination.paused_znode_exists || view->getContext()->getRefreshSet().refreshesStopped() || coordination.read_only)
            {
                /// Exit the task and wait for the user to start or resume, which will schedule the task again.
                setState(RefreshState::Disabled, lock);
                break;
            }

            /// Check if it's time to refresh.
            auto now = currentTime();
            auto start_time = std::chrono::floor<std::chrono::seconds>(now);
            auto start_time_steady = std::chrono::steady_clock::now();
            auto [when, timeslot, start_znode] = determineNextRefreshTime(start_time);
            next_refresh_time = when;
            bool out_of_schedule = scheduling.out_of_schedule_refresh_requested;
            if (out_of_schedule)
            {
                chassert(start_znode.attempt_number > 0);
                start_znode.attempt_number -= 1;
            }
            else if (now < when)
            {
                size_t delay_ms = std::chrono::duration_cast<std::chrono::milliseconds>(when - now).count();
                /// If we're in a test that fakes the clock, poll every 100ms.
                if (scheduling.fake_clock.load(std::memory_order_relaxed) != INT64_MIN)
                    delay_ms = 100;
                refresh_task->scheduleAfter(delay_ms);
                setState(RefreshState::Scheduled, lock);
                break;
            }
            else if (timeslot >= scheduling.dependencies_satisfied_until)
            {
                setState(RefreshState::WaitingForDependencies, lock);
                break;
            }

            if (refreshed_just_now)
            {
                /// If doing two refreshes in a row, go through Scheduled state first,
                /// to give wait() a chance to complete.
                setState(RefreshState::Scheduled, lock);
                refresh_task->schedule();
                break;
            }

            lock.unlock();

            /// Try to become the global leader for this refresh.
            LOG_DEBUG(log, "Attempting to become global leader for refresh");
            bool became_global_leader = tryBecomeGlobalLeader(zookeeper, start_time);
            
            if (!became_global_leader)
                break;

            /// As global leader:
            /// 1. Clean up old refresh directories older than 24 hours
            LOG_DEBUG(log, "Global leader cleaning up old refresh directories");
            cleanupOldRefreshDirectories(zookeeper);

            /// 2. Create a new refresh directory for this refresh
            LOG_DEBUG(log, "Global leader creating refresh directory");
            createRefreshDirectory(zookeeper, start_time);

            // Global reader should succeed to become shard leader, because it did not yet create a temporary table.
            [[maybe_unused]] bool became_shard_leader = tryBecomeShardLeader(zookeeper);
            LOG_DEBUG(log, "tryBecomeShardLeader result: {}", became_shard_leader);
            assert(became_shard_leader == coordination.is_global_leader);

            lock.lock();

            /// Write to keeper (update root znode).
            LOG_DEBUG(log, "Updating coordination state (is_global_leader={})", coordination.is_global_leader);
            if (!updateCoordinationState(start_znode, coordination.is_global_leader, zookeeper, lock))
            {
                /// Clean up the artifacts we created before losing the race
                if (!coordination.current_refresh_dir.empty())
                {
                    String shard_leader_path = coordination.current_refresh_dir + "/" + coordination.shard_name;
                    LOG_DEBUG(log, "Cleaning up shard leader znode: {}", shard_leader_path);
                    zookeeper->tryRemove(shard_leader_path);
                    LOG_DEBUG(log, "Cleaning up refresh directory: {}", coordination.current_refresh_dir);
                    cleanupRefreshDirectory(zookeeper);
                }
                coordination.is_global_leader = false;
                coordination.is_shard_leader = false;
                schedule_keeper_retry();
                break;
            }
            LOG_DEBUG(log, "Coordination state updated successfully");
            chassert(lock.owns_lock());

            /// Perform a refresh.
            LOG_DEBUG(log, "Performing refresh");
            setState(RefreshState::Running, lock);
            scheduling.out_of_schedule_refresh_requested = false;
            bool append = refresh_append;
            int32_t root_znode_version = coordination.coordinated ? coordination.root_znode.version : -1;
            CurrentMetrics::Increment metric_inc(CurrentMetrics::RefreshingViews);

            lock.unlock();

            bool refreshed = false;
            String error_message;
            UUID new_table_uuid;

            try
            {
                /// Execute refresh: create temporary table and write data
                new_table_uuid = executeRefreshUnlocked(append, root_znode_version);

                /// Mark our shard as finished
                if (coordination.coordinated) {
                    markShardFinished(zookeeper);

                    /// Poll until all shards are finished or timeout
                    while (true)
                    {
                        if (std::chrono::steady_clock::now() - start_time_steady > std::chrono::seconds(RefreshTimeout::REFRESH_TIMEOUT_SEC))
                        {
                            throw Exception(ErrorCodes::REFRESH_FAILED, "Timeout waiting for all shards to finish");
                        }

                        if (checkAllShardsFinished(zookeeper))
                        {
                            LOG_DEBUG(log, "All shards finished, proceeding with table swap");
                            break;
                        }

                        if (execution.interrupt_execution.load())
                            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Refresh cancelled while waiting for shards");

                        std::this_thread::sleep_for(std::chrono::milliseconds(100)); // Wait 100ms before polling again
                    }
                }
                if (!append)
                    exchangeTargetTableAfterRefresh(new_table_uuid, append);

                refreshed = true;
            }
            catch (...)
            {
                if (execution.interrupt_execution.load())
                {
                    error_message = "cancelled";
                    LOG_DEBUG(log, "{}: Refresh cancelled", view->getStorageID().getFullTableName());
                }
                else
                {
                    error_message = getCurrentExceptionMessage(true);
                    LOG_ERROR(log, "{}: Refresh failed (attempt {}/{}): {}", view->getStorageID().getFullTableName(), start_znode.attempt_number, refresh_settings[RefreshSetting::refresh_retries] + 1, error_message);
                }
            }

            /// Global leader cleans up the refresh directory
            if (coordination.coordinated)
            {
                try
                {
                    cleanupRefreshDirectory(zookeeper);
                }
                catch (...)
                {
                    LOG_WARNING(log, "Failed to cleanup refresh directory: {}", getCurrentExceptionMessage(true));
                }
            }

            lock.lock();

            /// Reset coordination state for next refresh
            coordination.is_global_leader = false;
            coordination.is_shard_leader = false;

            setState(RefreshState::Scheduling, lock);

            auto end_time = std::chrono::floor<std::chrono::seconds>(currentTime());
            auto znode = coordination.root_znode;
            znode.last_attempt_time = end_time;
            if (refreshed)
            {
                znode.last_attempt_succeeded = true;
                znode.last_completed_timeslot = refresh_schedule.timeslotForCompletedRefresh(znode.last_completed_timeslot, start_time, end_time, out_of_schedule);
                znode.last_success_time = start_time;
                znode.last_success_duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time_steady);
                znode.last_success_table_uuid = new_table_uuid;
                znode.previous_attempt_error = "";
                znode.attempt_number = 0;
                znode.randomize();
            }
            else
            {
                znode.last_attempt_error = error_message;
            }

            bool ok = updateCoordinationState(znode, false, zookeeper, lock);
            chassert(lock.owns_lock());
            if (!ok)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Refresh coordination znode was changed while refresh was in progress.");

            if (refreshed)
            {
                lock.unlock();
                view->getContext()->getRefreshSet().notifyDependents(view->getStorageID());
                lock.lock();
            }

            refreshed_just_now = true;
        }
    }
    catch (Coordination::Exception &)
    {
        tryLogCurrentException(log, "Keeper error");
        if (!lock.owns_lock())
            lock.lock();
        coordination.is_global_leader = false;
        coordination.is_shard_leader = false;
        schedule_keeper_retry();
    }
    catch (...)
    {
        if (!lock.owns_lock())
            lock.lock();
        scheduling.stop_requested = true;
        coordination.watches->should_reread_znodes.store(true);
        coordination.running_znode_exists = false;
        coordination.is_global_leader = false;
        coordination.is_shard_leader = false;
        lock.unlock();

        tryLogCurrentException(log,
            "Unexpected exception in refresh scheduling, please investigate. The view will be stopped.");
#ifdef DEBUG_OR_SANITIZER_BUILD
        /// There's at least one legitimate case where this may happen: if the user (DEFINER) was dropped.
        /// But it's unexpected in tests.
        /// Note that Coordination::Exception is caught separately above, so transient keeper errors
        /// don't go here and are just retried.
        abortOnFailedAssertion("Unexpected exception in refresh scheduling");
#else
        if (coordination.coordinated)
            removeRunningZnodeIfMine(view->getContext()->getZooKeeper());
#endif
    }
}

UUID RefreshTask::executeRefreshUnlocked(bool append, int32_t root_znode_version)
{
    // Only executes after the replica has become a shard leader or global leader.
    LOG_DEBUG(log, "Refreshing view {} (global_leader={}, shard_leader={})",
        view->getStorageID().getFullTableName(), coordination.is_global_leader, coordination.is_shard_leader);
    execution.progress.reset();

    ContextMutablePtr refresh_context = view->createRefreshContext();
    std::shared_ptr<zkutil::ZooKeeper> zookeeper;
    if (coordination.coordinated)
        zookeeper = view->getContext()->getZooKeeper();

    if (!append)
    {
        refresh_context->setParentTable(view->getStorageID().uuid);
        refresh_context->setDDLQueryCancellation(execution.cancel_ddl_queries.get_token());
        if (root_znode_version != -1)
            refresh_context->setDDLAdditionalChecksOnEnqueue({zkutil::makeCheckRequest(coordination.path, root_znode_version)});
    }

    std::optional<StorageID> table_to_drop;
    auto new_table_id = StorageID::createEmpty();
    try
    {
        std::shared_ptr<ASTInsertQuery> refresh_query;
        std::unique_ptr<CurrentThread::QueryScope> query_scope;
        /// to ZooKeeper so other shards can find it.
        if (coordination.is_global_leader)
        {
            /// Create a table (or get the shared temporary table name in multi-shard mode).
            std::tie(refresh_query, query_scope) = view->prepareRefresh(append, refresh_context, table_to_drop, new_table_id);
            new_table_id = refresh_query->table_id;
            coordination.temporary_table_name = new_table_id.table_name;
            if (coordination.coordinated)
                getOrWaitForTemporaryTable(zookeeper);  // This will write the temporary table name
        }
        /// If we're a shard leader but not the global leader, we need to get the temporary
        /// table name from ZooKeeper (the table was already created by global leader).
        else if (coordination.is_shard_leader && coordination.coordinated && !coordination.is_global_leader)
        {
            /// Note: In this case, we write to the same temporary table that the global leader created.
            /// The refresh_query->table_id should be updated to point to the shared table.
            String shared_temp_table = getOrWaitForTemporaryTable(zookeeper);
            coordination.temporary_table_name = shared_temp_table;
            // Find the table by name
            // Wait up to 10 seconds for the table to be replicated
            for (int i = 0; i < 100; i++) {
                auto table = DatabaseCatalog::instance().getDatabase(view->getStorageID().database_name)->tryGetTable(coordination.temporary_table_name, view->getContext());
                if (table) {
                    new_table_id = table->getStorageID();
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }

            std::tie(refresh_query, query_scope) = view->prepareRefresh(append, refresh_context, table_to_drop, new_table_id);
        }
        else {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid coordination state");
        }
        LOG_INFO(log, "Using new table id: {}", new_table_id.uuid);

        /// Add the query to system.processes and allow it to be killed with KILL QUERY.
        String query_for_logging = refresh_query->formatForLogging(
            refresh_context->getSettingsRef()[Setting::log_queries_cut_to_length]);
        UInt64 normalized_query_hash = normalizedQueryHash(query_for_logging, false);

        auto process_list_entry = refresh_context->getProcessList().insert(
            query_for_logging, normalized_query_hash, refresh_query.get(), refresh_context, Stopwatch{CLOCK_MONOTONIC}.getStart());

        refresh_context->setProcessListElement(process_list_entry->getQueryStatus());
        refresh_context->setProgressCallback([this](const Progress & prog)
        {
            execution.progress.incrementPiecewiseAtomically(prog);
        });

        /// Run the query - each shard leader writes its portion of the data.

        BlockIO block_io = InterpreterInsertQuery(
            refresh_query,
            refresh_context,
            /* allow_materialized */ false,
            /* no_squash */ false,
            /* no_destination */ false,
            /* async_isnert */ false).execute();
        QueryPipeline & pipeline = block_io.pipeline;

        if (!pipeline.completed())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline for view refresh must be completed");

        PipelineExecutor executor(pipeline.processors, pipeline.process_list_element);
        executor.setReadProgressCallback(pipeline.getReadProgressCallback());

        {
            std::unique_lock exec_lock(execution.executor_mutex);
            if (execution.interrupt_execution.load())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Refresh cancelled");
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
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Refresh cancelled");

        /// Note: Table exchange is done separately via exchangeTargetTableAfterRefresh()
        /// after all shards have finished writing data.
    }
    catch (...)
    {
        /// Only drop the temporary table if we created it (global leader or non-coordinated).
        if (table_to_drop.has_value() && (coordination.is_global_leader || !coordination.coordinated))
            view->dropTempTable(table_to_drop.value(), refresh_context);
        throw;
    }

    return new_table_id.uuid;
}

void RefreshTask::exchangeTargetTableAfterRefresh(UUID new_table_uuid, bool append)
{
    if (append)
        return;

    /// Only the global leader or non-coordinated refresh does the exchange.
    if (!coordination.is_global_leader && coordination.coordinated)
        return;

    LOG_DEBUG(log, "Exchanging target table with new table uuid={}", toString(new_table_uuid));

    ContextMutablePtr refresh_context = view->createRefreshContext();


    auto table = DatabaseCatalog::instance().getDatabase(view->getStorageID().database_name)->tryGetTable(coordination.temporary_table_name, view->getContext());
    if (!table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} does not exist", coordination.temporary_table_name);
    auto new_table_id = table->getStorageID();

    /// Exchange tables and get the old table to drop
    auto table_to_drop = view->exchangeTargetTable(new_table_id, refresh_context);

    /// Drop the old table
    if (table_to_drop.has_value())
        view->dropTempTable(table_to_drop.value(), refresh_context);

    LOG_INFO(log, "Target table exchange completed");
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

    return {when, timeslot, znode};
}

void RefreshTask::scheduleRefresh(std::lock_guard<std::mutex> &)
{
    state = RefreshState::Scheduling;
    refresh_task->schedule();
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

    /// Set watches. (This is a lot of code, is there a better way?)
    if (!coordination.watches->root_watch_active.load())
    {
        coordination.watches->root_watch_active.store(true);
        zookeeper->existsWatch(coordination.path, nullptr,
            [w = coordination.watches, task_waker = refresh_task->getWatchCallback()](const Coordination::WatchResponse & response)
            {
                w->root_watch_active.store(false);
                w->should_reread_znodes.store(true);
                task_waker(response);
            });
    }
    if (!coordination.watches->children_watch_active.load())
    {
        coordination.watches->children_watch_active.store(true);
        zookeeper->getChildrenWatch(coordination.path, nullptr,
            [w = coordination.watches, task_waker = refresh_task->getWatchCallback()](const Coordination::WatchResponse & response)
            {
                w->children_watch_active.store(false);
                w->should_reread_znodes.store(true);
                task_waker(response);
            });
    }

    Strings paths {coordination.path, coordination.path + "/running", coordination.path + "/paused"};
    auto responses = zookeeper->tryGet(paths.begin(), paths.end());

    lock.lock();

    if (responses[0].error != Coordination::Error::ZOK)
        throw Coordination::Exception::fromPath(responses[0].error, paths[0]);
    for (size_t i = 1; i < 3; ++i)
        if (responses[i].error != Coordination::Error::ZOK && responses[i].error != Coordination::Error::ZNONODE)
            throw Coordination::Exception::fromPath(responses[i].error, paths[i]);

    coordination.root_znode.parse(responses[0].data);
    coordination.root_znode.version = responses[0].stat.version;
    coordination.running_znode_exists = responses[1].error == Coordination::Error::ZOK;
    coordination.paused_znode_exists = responses[2].error == Coordination::Error::ZOK;

    /// Get current refresh directory from the "running" znode if it exists.
    /// The "running" znode contains "replica_name:timestamp".
    coordination.current_refresh_dir.clear();
    if (coordination.running_znode_exists && !responses[1].data.empty())
    {
        size_t colon_pos = responses[1].data.rfind(':');
        if (colon_pos != String::npos)
        {
            try
            {
                String timestamp_str = responses[1].data.substr(colon_pos + 1);
                coordination.current_refresh_dir = coordination.path + "/refresh_" + timestamp_str;
            }
            catch (...) {
                coordination.current_refresh_dir.clear();
            }
        }
    }

    if (coordination.root_znode.last_completed_timeslot != prev_last_completed_timeslot)
    {
        lock.unlock();
        view->getContext()->getRefreshSet().notifyDependents(view->getStorageID());
        lock.lock();
    }
}

bool RefreshTask::updateCoordinationState(CoordinationZnode root, bool running, std::shared_ptr<zkutil::ZooKeeper> zookeeper, std::unique_lock<std::mutex> & lock)
{
    chassert(lock.owns_lock());
    int32_t version = -1;
    if (coordination.coordinated)
    {
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeSetRequest(coordination.path, root.toString(), root.version));

        /// If we want to start running and the running znode already exists (we created it in tryBecomeGlobalLeader),
        /// just verify it exists instead of trying to create it again.
        /// If we want to stop running, remove the znode.
        /// If we want to start running and the znode doesn't exist yet, create it.
        if (running && coordination.running_znode_exists)
        {
            /// We already created the running znode in tryBecomeGlobalLeader, just check it exists
            ops.emplace_back(zkutil::makeCheckRequest(coordination.path + "/running", -1));
        }
        else if (running)
        {
            ops.emplace_back(zkutil::makeCreateRequest(coordination.path + "/running", coordination.replica_name, zkutil::CreateMode::Ephemeral));
        }
        else
        {
            ops.emplace_back(zkutil::makeRemoveRequest(coordination.path + "/running", -1));
        }

        Coordination::Responses responses;

        lock.unlock();
        auto code = zookeeper->tryMulti(ops, responses);
        lock.lock();

        if (running && (responses[0]->error == Coordination::Error::ZBADVERSION ||
                        code == Coordination::Error::ZNODEEXISTS ||
                        code == Coordination::Error::ZNONODE))
            /// Lost the race, this is normal, don't log a stack trace.
            return false;
        zkutil::KeeperMultiException::check(code, ops, responses);
        version = dynamic_cast<Coordination::SetResponse &>(*responses[0]).stat.version;

    }
    coordination.root_znode = root;
    coordination.root_znode.version = version;
    coordination.running_znode_exists = running;
    return true;
}

void RefreshTask::removeRunningZnodeIfMine(std::shared_ptr<zkutil::ZooKeeper> zookeeper)
{
    Coordination::Stat stat;
    String data;
    if (zookeeper->tryGet(coordination.path + "/running", data, &stat) && data == coordination.replica_name)
    {
        LOG_WARNING(log, "Removing unexpectedly lingering znode {}", coordination.path + "/running");
        zookeeper->tryRemove(coordination.path + "/running", stat.version);
    }
}

void RefreshTask::interruptExecution()
{
    chassert(!mutex.try_lock());
    std::unique_lock lock(execution.executor_mutex);
    if (execution.interrupt_execution.exchange(true))
        return;
    if (execution.executor)
    {
        execution.executor->cancel();
        LOG_DEBUG(log, "Cancelling refresh in {}", set_handle.getID().getFullNameNotQuoted());
    }
}

std::tuple<StoragePtr, TableLockHolder> RefreshTask::getAndLockTargetTable(const StorageID & storage_id, const ContextPtr & context)
{
    StoragePtr storage;
    TableLockHolder storage_lock;
    for (int attempt = 0; attempt < 10; ++attempt)
    {
        StoragePtr prev_storage = std::move(storage);
        storage = DatabaseCatalog::instance().getTable(storage_id, context);
        if (storage == prev_storage)
        {
            // Table was dropped but is still accessible in DatabaseCatalog.
            // Either ABA problem or something's broken. Don't retry, just in case.
            break;
        }
        storage_lock = storage->tryLockForShare(context->getCurrentQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);
        if (storage_lock)
            break;
    }
    if (!storage_lock)
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {} is dropped or detached", storage_id.getFullNameNotQuoted());

    if (coordination.coordinated)
    {
        UUID uuid = storage->getStorageID().uuid;

        std::lock_guard lock(replica_sync_mutex);
        if (uuid != last_synced_inner_uuid)
        {
            InterpreterSystemQuery::trySyncReplica(storage, SyncReplicaMode::DEFAULT, {}, context);

            /// (Race condition: this may revert from a newer uuid to an older one. This doesn't break
            ///  anything, just causes an unnecessary sync. Should be rare.)
            last_synced_inner_uuid = uuid;
        }
    }

    return {storage, storage_lock};
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
    randomness = std::uniform_int_distribution(Int64(-1e-9), Int64(1e9))(thread_local_rng);
}

String RefreshTask::CoordinationZnode::toString() const
{
    WriteBufferFromOwnString out;
    /// "format version" should be incremented when making incompatible change, to make older servers
    /// refuse to parse it. For backwards compatible changes, just add new fields at the end and old
    /// servers will ignore them.
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
        << "randomness: " << randomness << "\n";
    return out.str();
}

void RefreshTask::CoordinationZnode::parse(const String & data)
{
    ReadBufferFromString in(data);
    Int64 last_completed_timeslot_int;
    Int64 last_success_time_int;
    Int64 last_success_duration_int;
    Int64 last_attempt_time_int;
    in >> "format version: 1\n"
       >> "last_completed_timeslot: " >> last_completed_timeslot_int >> "\n"
       >> "last_success_time: " >> last_success_time_int >> "\n"
       >> "last_success_duration_ms: " >> last_success_duration_int >> "\n"
       >> "last_success_table_uuid: " >> last_success_table_uuid >> "\n"
       >> "last_attempt_time: " >> last_attempt_time_int >> "\n"
       >> "last_attempt_replica: " >> escape >> last_attempt_replica >> "\n"
       >> "last_attempt_error: " >> escape >> last_attempt_error >> "\n"
       >> "last_attempt_succeeded: " >> last_attempt_succeeded >> "\n"
       >> "previous_attempt_error: " >> escape >> previous_attempt_error >> "\n"
       >> "attempt_number: " >> attempt_number >> "\n"
       >> "randomness: " >> randomness >> "\n";
    last_completed_timeslot = std::chrono::sys_seconds(std::chrono::seconds(last_completed_timeslot_int));
    last_success_time = std::chrono::sys_seconds(std::chrono::seconds(last_success_time_int));
    last_success_duration = std::chrono::milliseconds(last_success_duration_int);
    last_attempt_time = std::chrono::sys_seconds(std::chrono::seconds(last_attempt_time_int));
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
                    LOG_DEBUG(log, "Cleaning up old refresh directory: {}", refresh_path);

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
                LOG_WARNING(log, "Failed to parse or clean up refresh directory {}: {}", child, e.what());
            }
        }
    }
}

String RefreshTask::createRefreshDirectory(std::shared_ptr<zkutil::ZooKeeper> zookeeper, std::chrono::sys_seconds timestamp)
{
    Int64 ts = std::chrono::duration_cast<std::chrono::seconds>(timestamp.time_since_epoch()).count();
    String refresh_dir = coordination.path + "/refresh_" + std::to_string(ts);

    auto code = zookeeper->tryCreate(refresh_dir, "", zkutil::CreateMode::Persistent);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
        throw Coordination::Exception::fromPath(code, refresh_dir);

    coordination.current_refresh_dir = refresh_dir;
    return refresh_dir;
}

bool RefreshTask::tryBecomeGlobalLeader(std::shared_ptr<zkutil::ZooKeeper> zookeeper, std::chrono::sys_seconds timestamp)
{
    if (!coordination.coordinated)
    {
        coordination.is_global_leader = true;
        return true;
    }

    /// Try to create ephemeral "running" znode with current timestamp
    Int64 ts = std::chrono::duration_cast<std::chrono::seconds>(timestamp.time_since_epoch()).count();
    String running_data = coordination.replica_name + ":" + std::to_string(ts);

    auto code = zookeeper->tryCreate(coordination.path + "/running", running_data, zkutil::CreateMode::Ephemeral);
    if (code == Coordination::Error::ZOK)
    {
        coordination.is_global_leader = true;
        coordination.running_znode_exists = true;
        LOG_DEBUG(log, "Became global leader for refresh");
        return true;
    }
    else if (code == Coordination::Error::ZNODEEXISTS)
    {
        coordination.is_global_leader = false;
        coordination.running_znode_exists = true;
        LOG_DEBUG(log, "Another replica is already global leader");
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

    if (coordination.current_refresh_dir.empty() and not coordination.is_global_leader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot become shard leader without a current refresh directory");

    /// Try to create znode for this shard in the refresh directory
    String shard_leader_path = coordination.current_refresh_dir + "/" + coordination.shard_name;

    auto code = zookeeper->tryCreate(shard_leader_path, coordination.replica_name, zkutil::CreateMode::Ephemeral);
    if (code == Coordination::Error::ZOK)
    {
        coordination.is_shard_leader = true;
        LOG_DEBUG(log, "Became shard leader for shard {}", coordination.shard_name);
        return true;
    }
    else if (code == Coordination::Error::ZNODEEXISTS)
    {
        coordination.is_shard_leader = false;
        LOG_DEBUG(log, "Another replica is already shard leader for shard {}", coordination.shard_name);
        return false;
    }
    else
    {
        throw Coordination::Exception::fromPath(code, shard_leader_path);
    }
}

String RefreshTask::getOrWaitForTemporaryTable(std::shared_ptr<zkutil::ZooKeeper> zookeeper)
{
    if (!coordination.coordinated)
        return coordination.temporary_table_name;

    if (coordination.current_refresh_dir.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get temporary table without a current refresh directory");

    String temp_table_path = coordination.current_refresh_dir + "/temporary_table";

    /// If we're the global leader, write the temporary table name
    if (coordination.is_global_leader && !coordination.temporary_table_name.empty())
    {
        LOG_DEBUG(log, "Global leader creating temporary_table znode at {} with value {}", temp_table_path, coordination.temporary_table_name);
        auto code = zookeeper->tryCreate(temp_table_path, coordination.temporary_table_name, zkutil::CreateMode::Persistent);
        if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
            throw Coordination::Exception::fromPath(code, temp_table_path);
        LOG_DEBUG(log, "Global leader created temporary_table znode, code={}", static_cast<int>(code));
        return coordination.temporary_table_name;
    }

    /// Wait for the temporary table name to be available
    for (int attempt = 0; attempt < RefreshTimeout::REFRESH_TIMEOUT_SEC * 10; ++attempt)
    {
        String data;
        if (zookeeper->tryGet(temp_table_path, data) && !data.empty())
        {
            LOG_DEBUG(log, "Got temporary table name from znode: {}", data);
            coordination.temporary_table_name = data;
            return data;
        }

        if (execution.interrupt_execution.load())
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Refresh cancelled while waiting for temporary table");

        if (attempt % 50 == 0)  // Log every 5 seconds
            LOG_INFO(log, "Waiting for temporary table znode at {} (attempt {})", temp_table_path, attempt);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
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

    LOG_DEBUG(log, "Marked shard {} as finished in {}", coordination.shard_name, finished_path);
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cluster not found for database {}", view->getStorageID().database_name);
        return cluster->getShardsInfo().size();
    }
    catch (...)
    {
        LOG_WARNING(log, "Failed to get shards from cluster: {}", getCurrentExceptionMessage(true));
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to get children of finished directory: {}", finished_dir);
    }
    if (num_finished_shards == all_shards)
        return true;
    LOG_DEBUG(log, "Waiting for shards to finish: {} / {}", num_finished_shards, all_shards);
    return false;
}

void RefreshTask::cleanupRefreshDirectory(std::shared_ptr<zkutil::ZooKeeper> zookeeper)
{
    if (!coordination.coordinated || coordination.current_refresh_dir.empty())
        return;

    LOG_DEBUG(log, "Cleaning up refresh directory: {}", coordination.current_refresh_dir);

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
