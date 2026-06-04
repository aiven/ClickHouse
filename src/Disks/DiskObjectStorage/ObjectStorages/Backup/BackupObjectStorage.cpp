#include "BackupObjectStorage.h"

#include <filesystem>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <Common/escapeForFileName.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>

namespace fs = std::filesystem;

namespace DB
{

BackupObjectStorage::BackupObjectStorage(
    const ObjectStoragePtr & object_storage_, const std::string & backup_base_path_, const std::string & backup_config_name_)
    : object_storage(object_storage_)
    , backup_base_path(backup_base_path_)
    , backup_config_name(backup_config_name_)
    , log(getLogger(getName()))
{
}

void BackupObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    LOG_DEBUG(log, "removeObjectIfExists: {} -> {}", object.remote_path, object.local_path);
    removeObjectImpl(object.remote_path);
}

void BackupObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    for (const auto & object : objects)
    {
        LOG_DEBUG(log, "removeObjectsIfExist: {} -> {}", object.remote_path, object.local_path);
        removeObjectImpl(object.remote_path);
    }
}

bool BackupObjectStorage::exists(const StoredObject & object) const
{
    return !isSoftDeleted(object.remote_path) && object_storage->exists(object);
}

void BackupObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
{
    RelativePathsWithMetadata all_children;
    object_storage->listObjects(path, all_children, max_keys);
    for (const auto & child : all_children)
    {
        if (!isSoftDeleted(child->getPath()))
        {
            children.push_back(child);
        }
    }
}

ObjectStorageIteratorPtr BackupObjectStorage::iterate(
    const std::string & path_prefix,
    size_t max_keys,
    bool /* with_tags */,
    const std::optional<std::string> & /* start_after */) const
{
    /// Reuse the filtering listObjects so iterate observes the same soft-delete view.
    RelativePathsWithMetadata children;
    listObjects(path_prefix, children, max_keys);
    return std::make_shared<ObjectStorageIteratorFromList>(std::move(children));
}

bool BackupObjectStorage::isSoftDeleted(const std::string & object_path) const
{
    return FS::exists(getRemovedMarkerPath(object_path));
}

std::string BackupObjectStorage::getRemovedMarkerPath(const std::string & object_path) const
{
    return fs::path(backup_base_path) / escapeForFileName(object_path);
}

void BackupObjectStorage::removeObjectImpl(const std::string & object_path) const
{
    const std::string removed_marker_path = getRemovedMarkerPath(object_path);
    LOG_DEBUG(log, "adding removed marker: {}", removed_marker_path);
    FS::createFile(removed_marker_path);
}

}
