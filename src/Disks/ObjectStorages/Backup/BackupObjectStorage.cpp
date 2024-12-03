#include "BackupObjectStorage.h"

#include <filesystem>
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
    , log(&Poco::Logger::get(getName()))
{
}

void BackupObjectStorage::removeObject(const StoredObject & object)
{
    LOG_DEBUG(log, "removeObject: {} -> {}", object.remote_path, object.local_path);
    removeObjectImpl(object.remote_path);
}

void BackupObjectStorage::removeObjects(const StoredObjects & objects)
{
    for (const auto & object : objects)
    {
        LOG_DEBUG(log, "removeObjects: {} -> {}", object.remote_path, object.local_path);
        removeObjectImpl(object.remote_path);
    }
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
ObjectStorageKey
BackupObjectStorage::generateObjectKeyForPath(const std::string & path, const std::optional<std::string> & key_prefix) const
{
    return object_storage->generateObjectKeyForPath(path, key_prefix);
}

}
