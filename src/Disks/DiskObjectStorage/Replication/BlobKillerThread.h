#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>

#include <Core/BackgroundSchedulePoolTaskHolder.h>

namespace DB
{

class BlobKillerThread
{
    void run();

    int64_t trigger();
    void waitRound(int64_t expected_round);

public:
    BlobKillerThread(
        std::string disk_name,
        ContextPtr context,
        ClusterConfigurationPtr cluster_,
        MetadataStoragePtr metadata_storage_,
        ObjectStorageRouterPtr object_storages_,
        std::shared_ptr<BlobKillerThread> wrapped_blob_killer_);

    void startup();
    void shutdown();
    void triggerAndWait();
    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

    /// Detach the wrapped (inner) killer so triggerAndWait stops chaining into it.
    /// Used by the backup disk layer: the wrapped disk's killer routes blob removals to the
    /// raw object storage (physical unlink), which must NOT run for a backup-wrapped disk.
    void detachWrapped();

    /// Permanently stop and disable this killer, STICKILY: a subsequent `applyNewSettings` (config
    /// reload) will NOT resurrect it from the config default. Used to silence a wrapped disk's killer
    /// once a backup layer takes over as the sole (soft-)deleter. The wrapped disk's removal queue is
    /// separately kept empty at the source via `setRecordRemovals` on the concrete metadata storage,
    /// so a disabled killer has nothing to drain. Idempotent and safe to call before/after startup
    /// (the task is created in the constructor).
    void disable();

private:
    const std::string disk_name;
    const ClusterConfigurationPtr cluster;
    const MetadataStoragePtr metadata_storage;
    const ObjectStorageRouterPtr object_storages;
    /// Not const: detachWrapped resets it to nullptr to break the killer chain (see above).
    std::shared_ptr<BlobKillerThread> wrapped_blob_killer;
    const LoggerPtr log;

    std::atomic<bool> started{false};
    std::atomic<bool> enabled{true};
    /// Set by `disable()`. Once true this killer stays disabled across config reloads and skips the
    /// shutdown final-cleanup, so it never physically unlinks blobs a backup layer soft-deleted.
    std::atomic<bool> force_disabled{false};
    std::atomic<int64_t> finished_rounds{0};
    std::atomic<int64_t> reschedule_interval_sec{0};
    std::atomic<int64_t> metadata_request_batch{0};
    std::atomic<int64_t> max_blobs_in_task{0};
    ThreadPool remove_tasks_pool;
    ThreadPoolCallbackRunnerLocal<bool> remove_tasks_runner;

    BackgroundSchedulePoolTaskHolder task;
};

using BlobKillerThreadPtr = std::shared_ptr<BlobKillerThread>;

}
