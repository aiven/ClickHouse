#include <Storages/MergeTree/ReplicationQueueSizeInsertGate.h>

#include <Core/Settings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event RejectedInserts;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 aiven_queue_size_to_delay_insert;
    extern const SettingsUInt64 aiven_queue_size_to_throw_insert;
    extern const SettingsUInt64 aiven_queues_total_size_to_delay_insert;
    extern const SettingsUInt64 aiven_queues_total_size_to_throw_insert;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 aiven_queue_size_to_delay_insert;
    extern const MergeTreeSettingsUInt64 aiven_queue_size_to_throw_insert;
    extern const MergeTreeSettingsUInt64 aiven_queues_total_size_to_delay_insert;
    extern const MergeTreeSettingsUInt64 aiven_queues_total_size_to_throw_insert;
    extern const MergeTreeSettingsUInt64 min_delay_to_insert_ms;
    extern const MergeTreeSettingsUInt64 max_delay_to_insert;
}

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
}

namespace
{

/// Fraction of the way from the delay threshold to the throw threshold, capped at 1.
/// The size can be past the throw threshold when throwing is not allowed at this call site.
double delayFactor(UInt64 size, UInt64 delay_threshold, UInt64 throw_threshold)
{
    if (size < delay_threshold)
        return 0;
    if (throw_threshold <= delay_threshold)
        return 1;
    const UInt64 over_threshold = size - delay_threshold + 1;
    return std::min(1.0, static_cast<double>(over_threshold) / static_cast<double>(throw_threshold - delay_threshold));
}

}

UInt64 checkReplicationQueueSizesForInsert(
    const std::optional<ReplicationQueueSizes> & sizes,
    const Settings & query_settings,
    const MergeTreeSettings & table_settings,
    bool allow_throw,
    bool allow_delay,
    const LoggerPtr & log)
{
    if (!sizes)
        return 0;

    const UInt64 queue_size_to_delay_insert = query_settings[Setting::aiven_queue_size_to_delay_insert].changed
        ? query_settings[Setting::aiven_queue_size_to_delay_insert]
        : table_settings[MergeTreeSetting::aiven_queue_size_to_delay_insert];
    const UInt64 queue_size_to_throw_insert = query_settings[Setting::aiven_queue_size_to_throw_insert].changed
        ? query_settings[Setting::aiven_queue_size_to_throw_insert]
        : table_settings[MergeTreeSetting::aiven_queue_size_to_throw_insert];
    const UInt64 queues_total_size_to_delay_insert = query_settings[Setting::aiven_queues_total_size_to_delay_insert].changed
        ? query_settings[Setting::aiven_queues_total_size_to_delay_insert]
        : table_settings[MergeTreeSetting::aiven_queues_total_size_to_delay_insert];
    const UInt64 queues_total_size_to_throw_insert = query_settings[Setting::aiven_queues_total_size_to_throw_insert].changed
        ? query_settings[Setting::aiven_queues_total_size_to_throw_insert]
        : table_settings[MergeTreeSetting::aiven_queues_total_size_to_throw_insert];

    if (allow_throw && sizes->max_replicas_queue_size >= queue_size_to_throw_insert)
    {
        ProfileEvents::increment(ProfileEvents::RejectedInserts);
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Too large replication queue ({}). One of the replicas is too slow for inserts",
            sizes->max_replicas_queue_size);
    }
    if (allow_throw && sizes->queues_total_size >= queues_total_size_to_throw_insert)
    {
        ProfileEvents::increment(ProfileEvents::RejectedInserts);
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Too many items in replication queues ({}). One or multiple replicas are too slow for inserts",
            sizes->queues_total_size);
    }

    if (!allow_delay)
        return 0;

    const double delay_factor = std::max(
        delayFactor(sizes->max_replicas_queue_size, queue_size_to_delay_insert, queue_size_to_throw_insert),
        delayFactor(sizes->queues_total_size, queues_total_size_to_delay_insert, queues_total_size_to_throw_insert));
    if (delay_factor == 0)
        return 0;

    const UInt64 max_delay_milliseconds
        = table_settings[MergeTreeSetting::max_delay_to_insert] > 0 ? table_settings[MergeTreeSetting::max_delay_to_insert] * 1000 : 1000;
    const UInt64 min_delay_milliseconds = table_settings[MergeTreeSetting::min_delay_to_insert_ms];
    const UInt64 delay_milliseconds
        = std::max(min_delay_milliseconds, static_cast<UInt64>(static_cast<double>(max_delay_milliseconds) * delay_factor));

    LOG_INFO(
        log,
        "Delaying inserting block by {} ms. because the largest replication queue has {} entries and all replication queues have {}",
        delay_milliseconds,
        sizes->max_replicas_queue_size,
        sizes->queues_total_size);
    return delay_milliseconds;
}

}
