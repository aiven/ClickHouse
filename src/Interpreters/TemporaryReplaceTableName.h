#pragma once

#include <base/types.h>

#include <optional>
#include <shared_mutex>

namespace DB
{
    struct TemporaryReplaceTableName
    {
        String name_hash;
        String random_suffix;

        String toString() const;

        static std::optional<TemporaryReplaceTableName> fromString(const String & str);
    };

    /// Returns a global shared mutex used to guard table replace operations.
    /// During replace operations, the temporary table should not be visible in table listings.
    /// - Replace operations acquire an exclusive lock
    /// - Table listing operations acquire a shared lock
    std::shared_mutex & getReplaceTableMutex();

    /// RAII guard for exclusive lock during replace operations
    class ReplaceTableExclusiveLock
    {
    public:
        ReplaceTableExclusiveLock() : lock(getReplaceTableMutex()) {}
    private:
        std::unique_lock<std::shared_mutex> lock;
    };

    /// RAII guard for shared lock during table listing operations
    class ReplaceTableSharedLock
    {
    public:
        ReplaceTableSharedLock() : lock(getReplaceTableMutex()) {}
    private:
        std::shared_lock<std::shared_mutex> lock;
    };
}
