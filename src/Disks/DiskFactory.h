#pragma once

#include <Disks/IDisk.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <boost/noncopyable.hpp>
#include <Poco/Util/AbstractConfiguration.h>

#include <functional>
#include <map>
#include <unordered_map>
#include <unordered_set>


namespace DB
{

using DisksMap = std::map<String, DiskPtr, std::less<>>;
/**
 * Disk factory. Responsible for creating new disk objects.
 */
class DiskFactory final : private boost::noncopyable
{
public:
    using Creator = std::function<DiskPtr(
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & map,
        bool attach,
        bool custom_disk)>;

    static DiskFactory & instance();

    void registerDiskType(const String & disk_type, Creator creator);

    /// Declares that this disk type's creator honours `soft_delete`; every other type is rejected
    /// when the flag is set. Call it wherever the type is registered, so the two cannot drift apart.
    void markSoftDeleteCapable(const String & disk_type);

    DiskPtr create(
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & map,
        bool attach = false,
        bool custom_disk = false,
        const std::unordered_set<String> & skip_types = {}) const;

    void clearRegistry();

private:
    using DiskTypeRegistry = std::unordered_map<String, Creator>;
    DiskTypeRegistry registry;
    std::unordered_set<String> soft_delete_capable_types;
};

}
