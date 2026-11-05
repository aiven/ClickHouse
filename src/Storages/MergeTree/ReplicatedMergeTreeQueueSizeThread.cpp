#include <Storages/MergeTree/ReplicatedMergeTreeQueueSizeThread.h>

#include <Core/BackgroundSchedulePool.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/CurrentMetrics.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/logger_useful.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace CurrentMetrics
{
    extern const Metric ReplicatedQueuesTotalSize;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool aiven_queue_size_monitor;
}

namespace ServerSetting
{
    extern const ServerSettingsBool aiven_enable_replication_queue_size_limit;
}

static constexpr size_t QUEUE_SIZE_POLL_INTERVAL_MS = 1000;

ReplicatedMergeTreeQueueSizeThread::ReplicatedMergeTreeQueueSizeThread(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , log_name(storage.getStorageID().getFullTableName() + " (ReplicatedMergeTreeQueueSizeThread)")
    , log(getLogger(log_name))
    , enabled(storage.getContext()->getServerSettings()[ServerSetting::aiven_enable_replication_queue_size_limit])
    , monitor_enabled(enabled && storage.getContext()->getSettingsRef()[Setting::aiven_queue_size_monitor])
{
    if (monitor_enabled)
        task = storage.getContext()->getSchedulePool()->createTask(storage.getStorageID(), log_name, [this] { run(); });
}

ReplicatedMergeTreeQueueSizeThread::~ReplicatedMergeTreeQueueSizeThread()
{
    stop();
    clearPublishedSize();
}

void ReplicatedMergeTreeQueueSizeThread::start()
{
    if (task)
        task->activateAndSchedule();
}

void ReplicatedMergeTreeQueueSizeThread::stop()
{
    if (task)
        task->deactivate();
}

void ReplicatedMergeTreeQueueSizeThread::clearPublishedSize()
{
    publish(0);
}

UInt64 ReplicatedMergeTreeQueueSizeThread::getQueuesTotalSize()
{
    return static_cast<UInt64>(std::max<CurrentMetrics::Value>(0, CurrentMetrics::get(CurrentMetrics::ReplicatedQueuesTotalSize)));
}

void ReplicatedMergeTreeQueueSizeThread::run()
{
    auto component_guard = Coordination::setCurrentComponent("ReplicatedMergeTreeQueueSizeThread::run");

    try
    {
        iterate();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    task->scheduleAfter(QUEUE_SIZE_POLL_INTERVAL_MS);
}

void ReplicatedMergeTreeQueueSizeThread::iterate()
{
    auto zookeeper = storage.getZooKeeper();
    const fs::path replicas_path = fs::path(storage.zookeeper_path) / "replicas";

    Strings queue_paths;
    for (const auto & replica : zookeeper->getChildren(replicas_path))
        queue_paths.push_back(replicas_path / replica / "queue");

    /// One batched request for all replicas; a replica removed meanwhile answers ZNONODE.
    auto responses = zookeeper->exists(queue_paths);
    UInt64 max_queue_size = 0;
    for (size_t i = 0; i < responses.size(); ++i)
    {
        if (responses[i].error == Coordination::Error::ZOK)
            max_queue_size = std::max<UInt64>(max_queue_size, responses[i].stat.numChildren);
    }

    LOG_TRACE(log, "Max replica queue size is {}", max_queue_size);
    publish(max_queue_size);
}

void ReplicatedMergeTreeQueueSizeThread::publish(UInt64 value)
{
    const UInt64 previous = max_replicas_queue_size.exchange(value);
    CurrentMetrics::add(
        CurrentMetrics::ReplicatedQueuesTotalSize, static_cast<CurrentMetrics::Value>(value) - static_cast<CurrentMetrics::Value>(previous));
}

}
