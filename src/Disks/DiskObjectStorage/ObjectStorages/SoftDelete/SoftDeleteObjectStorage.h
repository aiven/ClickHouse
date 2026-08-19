#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Common/Logger.h>

namespace DB
{

/// Wraps an object storage and intercepts object deletions: instead of physically
/// removing objects, it writes a local deletion-marker file per object so that an
/// external Aiven backup/GC system can decide when to physically delete. Reads and
/// listings filter out soft-deleted objects so the table view stays consistent.
class SoftDeleteObjectStorage final : public IObjectStorage
{
public:
    SoftDeleteObjectStorage(const ObjectStoragePtr & object_storage_, const std::string & markers_path_, const std::string & disk_name_);

    std::string getName() const override { return fmt::format("SoftDeleteObjectStorage-{}({})", disk_name, object_storage->getName()); }

    ObjectStorageType getType() const override { return object_storage->getType(); }

    std::string getCommonKeyPrefix() const override { return object_storage->getCommonKeyPrefix(); }

    std::string getDescription() const override { return object_storage->getDescription(); }

    bool exists(const StoredObject & object) const override;

    void listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const override;

    ObjectStorageIteratorPtr iterate(
        const std::string & path_prefix,
        size_t max_keys,
        bool with_tags,
        const std::optional<std::string> & start_after) const override;

    ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const override
    {
        return object_storage->getObjectMetadata(path, with_tags);
    }

    std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override
    {
        return object_storage->tryGetObjectMetadata(path, with_tags);
    }

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {}) const override
    {
        return object_storage->readObject(object, read_settings, read_hint);
    }

    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override
    {
        return object_storage->writeObject(object, mode, attributes, buf_size, write_settings);
    }

    void removeObjectIfExists(const StoredObject & object) override;

    void removeObjectsIfExist(const StoredObjects & objects) override;

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) override
    {
        object_storage->copyObject(object_from, object_to, read_settings, write_settings, object_to_attributes);
    }

    void copyObjectToAnotherObjectStorage( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        IObjectStorage & object_storage_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) override
    {
        object_storage->copyObjectToAnotherObjectStorage(
            object_from, object_to, read_settings, write_settings, object_storage_to, object_to_attributes);
    }

    bool isRemote() const override { return object_storage->isRemote(); }

    void shutdown() override { object_storage->shutdown(); }

    void startup() override { object_storage->startup(); }

    void applyNewSettings(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context,
        const ApplyNewSettingsOptions & options) override
    {
        object_storage->applyNewSettings(config, config_prefix, context, options);
    }

    String getObjectsNamespace() const override { return object_storage->getObjectsNamespace(); }

    std::string getUniqueId(const std::string & path) const override { return object_storage->getUniqueId(path); }

    bool isReadOnly() const override { return object_storage->isReadOnly(); }

    bool supportParallelWrite() const override { return object_storage->supportParallelWrite(); }

    ReadSettings patchSettings(const ReadSettings & read_settings) const override { return object_storage->patchSettings(read_settings); }

    WriteSettings patchSettings(const WriteSettings & write_settings) const override { return object_storage->patchSettings(write_settings); }

    ObjectStorageKeyGeneratorPtr createKeyGenerator() const override { return object_storage->createKeyGenerator(); }

#if USE_AZURE_BLOB_STORAGE
    std::shared_ptr<const AzureBlobStorage::ContainerClient> getAzureBlobStorageClient() const override
    {
        return object_storage->getAzureBlobStorageClient();
    }

    AzureBlobStorage::AuthMethod getAzureBlobStorageAuthMethod() const override
    {
        return object_storage->getAzureBlobStorageAuthMethod();
    }
#endif

#if USE_AWS_S3
    std::shared_ptr<const S3::Client> getS3StorageClient() override
    {
        return object_storage->getS3StorageClient();
    }

    std::shared_ptr<const S3::Client> tryGetS3StorageClient() override
    {
        return object_storage->tryGetS3StorageClient();
    }
#endif

#if USE_AZURE_BLOB_STORAGE || USE_AWS_S3
    void tagObjects(const StoredObjects & objects, const std::string & tag_key, const std::string & tag_value) override
    {
        object_storage->tagObjects(objects, tag_key, tag_value);
    }
#endif

private:
    bool isSoftDeleted(const std::string & object_path) const;

    std::string getRemovedMarkerPath(const std::string & object_path) const;

    void removeObjectImpl(const std::string & object_path) const;

    ObjectStoragePtr object_storage;
    std::string markers_path;
    std::string disk_name;
    LoggerPtr log;
};

}
