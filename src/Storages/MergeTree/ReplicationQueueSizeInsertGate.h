#pragma once

#include <Common/Logger_fwd.h>
#include <base/types.h>

#include <optional>

namespace DB
{

struct Settings;
struct MergeTreeSettings;

/// Aiven: inputs of the replication queue size limit (patch 008).
struct ReplicationQueueSizes
{
    /// The largest replication queue among the replicas of the table.
    UInt64 max_replicas_queue_size = 0;
    /// Sum of `max_replicas_queue_size` over all replicated tables of the server.
    UInt64 queues_total_size = 0;
};

/// Throws `LIMIT_EXCEEDED` if `allow_throw` and a queue size reaches its `aiven_*_to_throw_insert` threshold,
/// otherwise returns how long to delay the insert, 0 if not at all or if not `allow_delay`. A query-level
/// threshold overrides the table-level one when it is set. `sizes` is empty when the limit is disabled for the table.
UInt64 checkReplicationQueueSizesForInsert(
    const std::optional<ReplicationQueueSizes> & sizes,
    const Settings & query_settings,
    const MergeTreeSettings & table_settings,
    bool allow_throw,
    bool allow_delay,
    const LoggerPtr & log);

}
