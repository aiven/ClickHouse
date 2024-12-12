#pragma once

#include "config.h"
#include "Storages/NamedCollectionsHelpers.h"

#if USE_AWS_S3 && USE_AVRO

#include <Formats/FormatFactory.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/HudiMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>

namespace DB
{

/// Storage for read-only integration with Apache Iceberg tables in Amazon S3 (see https://iceberg.apache.org/)
/// Right now it's implemented on top of StorageS3 and right now it doesn't support
/// many Iceberg features like schema evolution, partitioning, positional and equality deletes.
template <typename DataLakeMetadata>
class IStorageDataLake final : public StorageObjectStorage
{
public:
    using Storage = StorageObjectStorage;
    using ConfigurationPtr = Storage::ConfigurationPtr;

    static StoragePtr create(
        ConfigurationPtr base_configuration,
        ContextPtr context,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment_,
        std::optional<FormatSettings> format_settings_,
        NamedCollectionNameOpt named_collection_name_,
        LoadingStrictnessLevel mode)
    {
        ObjectStoragePtr object_storage = base_configuration->createObjectStorage(context, /* is_readonly */true);

        DataLakeMetadataPtr metadata;
        NamesAndTypesList schema_from_metadata;
        const bool use_schema_from_metadata = columns_.empty();

        if (base_configuration->format == "auto")
            base_configuration->format = "Parquet";

        ConfigurationPtr configuration = base_configuration->clone();

        try
        {
            if (object_storage) {
                metadata = DataLakeMetadata::create(object_storage, base_configuration, context);
                configuration->setPaths(metadata->getDataFiles());
                if (use_schema_from_metadata)
                    schema_from_metadata = metadata->getTableSchema();
            }
        }
        catch (...)
        {
            if (mode <= LoadingStrictnessLevel::CREATE)
                throw;

            metadata.reset();
            configuration->setPaths({});
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        return std::make_shared<IStorageDataLake<DataLakeMetadata>>(
            base_configuration, std::move(metadata), configuration, object_storage,
            context, table_id_,
            use_schema_from_metadata ? ColumnsDescription(schema_from_metadata) : columns_,
            constraints_, comment_, format_settings_, named_collection_name_);
    }

    String getName() const override { return DataLakeMetadata::name; }

    static ColumnsDescription getTableStructureFromData(
        ObjectStoragePtr object_storage_,
        ConfigurationPtr base_configuration,
        const std::optional<FormatSettings> & format_settings_,
        ContextPtr local_context)
    {
        auto metadata = DataLakeMetadata::create(object_storage_, base_configuration, local_context);

        auto schema_from_metadata = metadata->getTableSchema();
        if (!schema_from_metadata.empty())
        {
            return ColumnsDescription(std::move(schema_from_metadata));
        }
        else
        {
            ConfigurationPtr configuration = base_configuration->clone();
            configuration->setPaths(metadata->getDataFiles());
            std::string sample_path;
            return Storage::resolveSchemaFromData(
                object_storage_, configuration, format_settings_, sample_path, local_context);
        }
    }

    void updateConfiguration(ContextPtr local_context) override
    {
        LOG_INFO(log, "DataLake updateConfiguration");
        Storage::updateConfiguration(local_context);
        if (!object_storage)
            return;
        auto new_metadata = DataLakeMetadata::create(object_storage, base_configuration, local_context);
        if (current_metadata && *current_metadata == *new_metadata)
            return;
        current_metadata = std::move(new_metadata);
        configuration->setPaths(current_metadata->getDataFiles());
        configuration->setPartitionColumns(current_metadata->getPartitionColumns());
    }

    // Called when the named collection exists and has correct information
    void reload(ContextPtr context_, ASTs engine_args) override {
        // throw Exception(ErrorCodes::BAD_ARGUMENTS, "Method reload is not implemented for {}", getName());
        LOG_INFO(getLogger("DataLake"), "Reloading data lake table");
        // Start from base_configuration because the updateConfiguration method
        // above also creates the DataLakeMetadata from the base configuration.
        // If we don't update it, metadata creation might fail because of the initial
        // setup which included a missing named collection.
        ConfigurationPtr reloaded_configuration = base_configuration->clone();
        Configuration::initialize(
            *reloaded_configuration,
            engine_args,
            context_,
            /*with_table_structure*/false,
            /*allow_missing_named_collection*/false
        );
        namedCollectionRestored();
        base_configuration = std::move(reloaded_configuration);
        object_storage = base_configuration->createObjectStorage(context_, /* is_readonly */true);
        updateConfiguration(context_);
}


    template <typename... Args>
    IStorageDataLake(
        ConfigurationPtr base_configuration_,
        DataLakeMetadataPtr metadata_,
        Args &&... args)
        : Storage(std::forward<Args>(args)...)
        , base_configuration(base_configuration_)
        , current_metadata(std::move(metadata_))
    {
        if (base_configuration->format == "auto")
        {
            base_configuration->format = Storage::configuration->format;
        }

        if (current_metadata)
        {
            const auto & columns = current_metadata->getPartitionColumns();
            base_configuration->setPartitionColumns(columns);
            Storage::configuration->setPartitionColumns(columns);
        }
    }

private:
    ConfigurationPtr base_configuration;
    DataLakeMetadataPtr current_metadata;

    ReadFromFormatInfo prepareReadingFromFormat(
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        bool supports_subset_of_columns,
        ContextPtr local_context) override
    {
        assertObjectStorageExists();

        auto info = DB::prepareReadingFromFormat(requested_columns, storage_snapshot, supports_subset_of_columns);
        if (!current_metadata)
        {
            LOG_INFO(log, "DataLake prepareReadingFromFormat");
            Storage::updateConfiguration(local_context);
            current_metadata = DataLakeMetadata::create(Storage::object_storage, base_configuration, local_context);
        }
        auto column_mapping = current_metadata->getColumnNameToPhysicalNameMapping();
        if (!column_mapping.empty())
        {
            for (const auto & [column_name, physical_name] : column_mapping)
            {
                auto & column = info.format_header.getByName(column_name);
                column.name = physical_name;
            }
        }
        return info;
    }
};

using StorageIceberg = IStorageDataLake<IcebergMetadata>;
using StorageDeltaLake = IStorageDataLake<DeltaLakeMetadata>;
using StorageHudi = IStorageDataLake<HudiMetadata>;

}

#endif
