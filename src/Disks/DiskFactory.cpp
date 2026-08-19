#include <Disks/DiskFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

DiskFactory & DiskFactory::instance()
{
    static DiskFactory factory;
    return factory;
}

void DiskFactory::registerDiskType(const String & disk_type, Creator creator)
{
    if (!registry.emplace(disk_type, creator).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskFactory: the disk type '{}' is not unique", disk_type);
}

void DiskFactory::markSoftDeleteCapable(const String & disk_type)
{
    soft_delete_capable_types.insert(disk_type);
}

DiskPtr DiskFactory::create(
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context,
    const DisksMap & map,
    bool attach,
    bool custom_disk,
    const std::unordered_set<String> & skip_types) const
{
    const auto disk_type = config.getString(config_prefix + ".type", "local");

    const auto found = registry.find(disk_type);
    if (found == registry.end())
    {
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
                        "DiskFactory: the disk '{}' has unknown disk type: {}", name, disk_type);
    }

    if (skip_types.contains(found->first))
    {
        return nullptr;
    }

    /// `soft_delete` is honoured only by the disk that owns the blobs. A layer above it delegates the
    /// removal downwards, so the blob would be physically unlinked despite the flag; reject such a
    /// config rather than let it silently do nothing. The check lives here, at the single point every
    /// disk is created, so it cannot fall out of step as disk types are added.
    if (config.getBool(config_prefix + ".soft_delete", false) && !soft_delete_capable_types.contains(disk_type))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Disk `{}` of type `{}` does not support `soft_delete`. Set it on the object storage disk "
            "that holds the blobs, not on a layer above it",
            name, disk_type);
    }

    const auto & disk_creator = found->second;
    return disk_creator(name, config, config_prefix, context, map, attach, custom_disk);
}

void DiskFactory::clearRegistry()
{
    registry.clear();
    soft_delete_capable_types.clear();
}
}
