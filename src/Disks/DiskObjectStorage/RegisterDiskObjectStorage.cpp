#include <Disks/DiskObjectStorage/MetadataStorages/MetadataStorageFactory.h>
#include <Disks/DiskObjectStorage/ObjectStorages/SoftDelete/SoftDeleteObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageFactory.h>
#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/ReadOnlyDiskWrapper.h>
#include <Disks/DiskFactory.h>
#include <Disks/IDisk.h>
#include <Interpreters/Context.h>
#include <Common/filesystemHelpers.h>

#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void registerObjectStorages();
void registerMetadataStorages();

namespace
{

ObjectStoragePtr wrapIfSoftDelete(
    ObjectStoragePtr object_storage,
    const String & disk_name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const ContextPtr & context)
{
    if (!config.getBool(config_prefix + ".soft_delete", false))
        return object_storage;

    auto markers_path = config.getString(
        config_prefix + ".soft_delete_markers_path",
        fs::path(context->getPath()) / "disks" / disk_name / "soft_deleted/");
    fs::create_directories(markers_path);

    LOG_INFO(
        getLogger("registerDiskObjectStorage"),
        "Disk `{}`: soft delete enabled, blob removals are recorded as markers under {} instead of deleting the blob",
        disk_name, markers_path);

    return std::make_shared<SoftDeleteObjectStorage>(std::move(object_storage), markers_path, disk_name);
}

}

void registerDiskObjectStorage(DiskFactory & factory, bool global_skip_access_check)
{
    registerObjectStorages();
    registerMetadataStorages();

    auto creator = [global_skip_access_check](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & /* map */,
        bool, bool) -> DiskPtr
    {
        const bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);

        std::unordered_map<Location, ObjectStoragePtr> object_storage_registry;
        std::unordered_map<Location, LocationInfo> cluster_registry;
        if (config.has(config_prefix + ".locations"))
        {
            Locations locations;
            config.keys(config_prefix + ".locations", locations);
            LOG_DEBUG(getLogger("registerDiskObjectStorage"), "Configuring DiskObjectStorage with multiple locations: [{}]", fmt::join(locations, ", "));

            /// Blob replication between locations has no notion of a soft-deleted object, so a marker
            /// written at one location would not stop the others from resurrecting or dropping the blob.
            if (locations.size() > 1 && config.getBool(config_prefix + ".soft_delete", false))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Disk `{}`: `soft_delete` is only supported on single-location object storage disks, but {} locations are configured",
                    name, locations.size());

            for (const auto & location : locations)
            {
                const std::string object_storage_config_prefix = config_prefix + ".locations." + location;
                const bool local = config.getBool(object_storage_config_prefix + ".local");
                const bool enabled = config.getBool(object_storage_config_prefix + ".enabled");
                ObjectStoragePtr object_storage = ObjectStorageFactory::instance().create(fmt::format("{}.{}", name, location), config, object_storage_config_prefix, context, /*skip_access_check=*/skip_access_check || !enabled);
                object_storage_registry[location] = wrapIfSoftDelete(std::move(object_storage), name, config, config_prefix, context);
                cluster_registry[location] = {enabled, local, object_storage_config_prefix};
            }
        }
        else
        {
            ObjectStoragePtr object_storage = ObjectStorageFactory::instance().create(name, config, config_prefix, context, skip_access_check);
            object_storage_registry["main"] = wrapIfSoftDelete(std::move(object_storage), name, config, config_prefix, context);
            cluster_registry["main"] = { .enabled = true, .local = true, .config_prefix = config_prefix };
        }

        ClusterConfigurationPtr cluster = std::make_shared<ClusterConfiguration>(name, std::move(cluster_registry));
        ObjectStorageRouterPtr object_storages = std::make_shared<ObjectStorageRouter>(std::move(object_storage_registry));

        std::string compatibility_metadata_type_hint;
        if (!config.has(config_prefix + ".metadata_type"))
        {
            auto type = config.getString(config_prefix + ".type", "");

            if (type.contains("with_keeper"))
                compatibility_metadata_type_hint = "keeper";
            else if (type.contains("plain") && type.contains("rewritable"))
                compatibility_metadata_type_hint = "plain_rewritable";
            else if (type.contains("plain"))
                compatibility_metadata_type_hint = "plain";
            else
                compatibility_metadata_type_hint = MetadataStorageFactory::getCompatibilityMetadataTypeHint(cluster, object_storages);
        }

        LOG_DEBUG(getLogger("registerDiskObjectStorage"), "Metadata type hint: {}", compatibility_metadata_type_hint);
        auto metadata_storage = MetadataStorageFactory::instance().create(name, config, config_prefix, cluster, object_storages, compatibility_metadata_type_hint);

        bool use_fake_transaction = config.getBool(config_prefix + ".use_fake_transaction", metadata_storage->getType() != MetadataStorageType::Keeper);
        DiskPtr disk = std::make_shared<DiskObjectStorage>(
            name,
            std::move(cluster),
            std::move(metadata_storage),
            std::move(object_storages),
            /*wrapped_disk=*/nullptr,
            config,
            config_prefix,
            use_fake_transaction);

        /// If this disk was created "on the fly" in order to serve as a temporary read-only disk.
        bool is_read_only_disk = config.getBool(config_prefix + ".read_only", false);
        if (is_read_only_disk)
        {
            LOG_DEBUG(getLogger("registerDiskObjectStorage"), "Using read-only disk wrapper");
            disk = std::make_shared<ReadOnlyDiskWrapper>(disk);
        }

        disk->startup(skip_access_check);
        return disk;
    };

    /// The creator above is the only one that reads `soft_delete`, so every type it backs is
    /// registered as soft-delete capable and everything else is rejected by `DiskFactory::create`.
    auto register_type = [&](const String & disk_type)
    {
        factory.registerDiskType(disk_type, creator);
        factory.markSoftDeleteCapable(disk_type);
    };

    register_type("object_storage");
#if USE_AWS_S3
    register_type("s3"); /// For compatibility
    register_type("s3_plain"); /// For compatibility
    register_type("s3_with_keeper"); /// For compatibility
    register_type("s3_plain_rewritable"); // For compatibility
#endif
#if USE_HDFS
    register_type("hdfs"); /// For compatibility
#endif
#if USE_AZURE_BLOB_STORAGE
    register_type("azure_blob_storage"); /// For compatibility
#endif
    register_type("local_blob_storage"); /// For compatibility
    register_type("web"); /// For compatibility
}

}
