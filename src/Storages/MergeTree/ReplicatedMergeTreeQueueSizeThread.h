#pragma once

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Common/Logger_fwd.h>
#include <base/types.h>

#include <atomic>

namespace DB
{

class StorageReplicatedMergeTree;

/// Aiven: polls the size of every replica's replication queue in Keeper and publishes the largest one.
/// The value of each table is also added to the server-wide `ReplicatedQueuesTotalSize` metric, which is
/// the total that `aiven_queues_total_size_to_*_insert` compare against.
///
/// Runs only while the replica is active: the restarting thread starts it together with the other replication
/// tasks and `partialShutdown` stops it. The table's share of the total is removed only on full shutdown,
/// so a replica that is read-only for a while keeps counting towards the total.
class ReplicatedMergeTreeQueueSizeThread
{
public:
    explicit ReplicatedMergeTreeQueueSizeThread(StorageReplicatedMergeTree & storage_);

    /// Stops the task and removes this table's share from the total, also if the storage shutdown threw.
    ~ReplicatedMergeTreeQueueSizeThread();

    /// No-op unless both `aiven_enable_replication_queue_size_limit` and `aiven_queue_size_monitor` are enabled.
    void start();

    void stop();

    /// Removes this table's share from the server-wide total. Must be called after `stop`.
    void clearPublishedSize();

    /// Whether the insert gate applies to this table.
    bool isEnabled() const { return enabled; }

    UInt64 getMaxReplicasQueueSize() const { return max_replicas_queue_size.load(std::memory_order_relaxed); }

    static UInt64 getQueuesTotalSize();

private:
    StorageReplicatedMergeTree & storage;
    const String log_name;
    const LoggerPtr log;
    const bool enabled;
    const bool monitor_enabled;

    BackgroundSchedulePoolTaskHolder task;

    /// Also the amount this table contributes to `ReplicatedQueuesTotalSize`.
    std::atomic<UInt64> max_replicas_queue_size{0};

    void run();
    void iterate();
    void publish(UInt64 value);
};

}
