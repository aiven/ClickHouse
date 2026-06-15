#include <Storages/MergeTree/ReplicatedMergeTreeQueueSizeThread.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Interpreters/Context.h>
#include <Poco/Timestamp.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

namespace DB
{

ReplicatedMergeTreeQueueSizeThread::ReplicatedMergeTreeQueueSizeThread(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , log_name(storage.getStorageID().getFullTableName() + " (ReplicatedMergeTreeQueueSizeThread)")
    , log(getLogger(log_name))
{
    task = storage.getContext()->getSchedulePool().createTask(storage.getStorageID(), log_name, [this]{ run(); });
}

void ReplicatedMergeTreeQueueSizeThread::run()
{
    /// 26.3 requires every scope that issues ZooKeeper requests to declare a component
    /// (enforce_component_tracking); the 25.8 source predates this. Without it the
    /// monitor's queue poll throws LOGICAL_ERROR "Current component is empty".
    auto component_guard = Coordination::setCurrentComponent("ReplicatedMergeTreeQueueSizeThread::run");

    try
    {
        iterate();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    task->scheduleAfter(1000);
}

void ReplicatedMergeTreeQueueSizeThread::iterate()
{
    storage.updateMaxReplicasQueueSize();
}

}
