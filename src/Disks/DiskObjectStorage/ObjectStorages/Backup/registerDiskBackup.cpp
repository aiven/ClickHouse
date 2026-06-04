#include <Common/assert_cast.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <Disks/DiskFactory.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Backup/BackupObjectStorage.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void registerDiskBackup(DiskFactory & factory, bool global_skip_access_check)
{
    auto creator = [global_skip_access_check](const String & name,
                    const Poco::Util::AbstractConfiguration & config,
                    const String & config_prefix,
                    ContextPtr context,
                    const DisksMap & map,
                    bool /* attach */,
                    bool /* custom_disk */) -> DiskPtr
    {
        const bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);

        auto disk_name = config.getString(config_prefix + ".disk", "");
        if (disk_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk Backup requires `disk` field in config");

        auto disk_it = map.find(disk_name);
        if (disk_it == map.end())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot wrap disk `{}` with backup layer `{}`: there is no such disk (it should be initialized before backup disk)",
                disk_name, name);
        }

        auto disk = disk_it->second;
        if (!dynamic_cast<const DiskObjectStorage *>(disk.get()))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot wrap disk `{}` with backup layer `{}`: backup disk is allowed only on top of object storage",
                disk_name, name);

        auto backup_base_path = config.getString(config_prefix + ".path", fs::path(context->getPath()) / "disks" / name / "backup/");
        if (!fs::exists(backup_base_path))
            fs::create_directories(backup_base_path);

        auto backup_disk_object_storage = std::dynamic_pointer_cast<DiskObjectStorage>(disk)->wrapWithBackup(name, backup_base_path);
        backup_disk_object_storage->startup(skip_access_check);

        LOG_INFO(
            getLogger("DiskBackup"),
            "Registered backup disk (`{}`) with structure: {}",
            name, assert_cast<DiskObjectStorage *>(backup_disk_object_storage.get())->getStructure());

        return backup_disk_object_storage;
    };

    factory.registerDiskType("backup", creator);
}

}
