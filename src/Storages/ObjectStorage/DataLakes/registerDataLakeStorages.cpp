#include "config.h"

#if USE_AWS_S3

#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/IStorageDataLake.h>
#include <Storages/ObjectStorage/DataLakes/IcebergMetadata.h>
#include <Storages/ObjectStorage/S3/Configuration.h>


namespace DB
{

#if USE_AVRO /// StorageIceberg depending on Avro to parse metadata with Avro format.

void registerStorageIceberg(StorageFactory & factory)
{
    factory.registerStorage(
        "Iceberg",
        [&](const StorageFactory::Arguments & args)
        {
            auto configuration = std::make_shared<StorageS3Configuration>();
            const ContextPtr context = args.getLocalContext();
            const Settings &settings = context->getSettingsRef();
            const std::optional<String> collection_name = StorageObjectStorage::Configuration::initialize(
                *configuration,
                args.engine_args,
                context,
                false,
                args.allow_missing_named_collection || settings.allow_missing_named_collections
            );

            return StorageIceberg::create(
                configuration, args.getContext(), args.table_id, args.columns,
                args.constraints, args.comment, std::nullopt, collection_name, args.mode);
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });
}

#endif

#if USE_PARQUET
void registerStorageDeltaLake(StorageFactory & factory)
{
    factory.registerStorage(
        "DeltaLake",
        [&](const StorageFactory::Arguments & args)
        {
            auto configuration = std::make_shared<StorageS3Configuration>();
            const ContextPtr context = args.getLocalContext();
            const Settings &settings = context->getSettingsRef();
            const bool allow_missing_named_collection = args.allow_missing_named_collection || settings.allow_missing_named_collections;
            LOG_INFO(
                getLogger("StorageDeltaLake"),
                "allow_missing_named_collection: args? {}, settings? {}",
                args.allow_missing_named_collection, settings.allow_missing_named_collections);
            const std::optional<String> collection_name = StorageObjectStorage::Configuration::initialize(
                *configuration,
                args.engine_args,
                context,
                false,
                allow_missing_named_collection
            );

            return StorageDeltaLake::create(
                configuration, args.getContext(), args.table_id, args.columns,
                args.constraints, args.comment, std::nullopt, collection_name, args.mode);
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });
}
#endif

void registerStorageHudi(StorageFactory & factory)
{
    factory.registerStorage(
        "Hudi",
        [&](const StorageFactory::Arguments & args)
        {
            auto configuration = std::make_shared<StorageS3Configuration>();
            const ContextPtr context = args.getLocalContext();
            const Settings &settings = context->getSettingsRef();
            const bool allow_missing_named_collection = args.allow_missing_named_collection || settings.allow_missing_named_collections;
            const std::optional<String> collection_name = StorageObjectStorage::Configuration::initialize(
                *configuration,
                args.engine_args,
                context,
                false,
                allow_missing_named_collection
            );

            return StorageHudi::create(
                configuration, args.getContext(), args.table_id, args.columns,
                args.constraints, args.comment, std::nullopt, collection_name, args.mode);
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });
}

}

#endif
