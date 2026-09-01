#include "SoftDeleteObjectStorage.h"

#include <filesystem>
#include <fcntl.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <Common/ErrnoException.h>
#include <Common/escapeForFileName.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CREATE_FILE;
}

SoftDeleteObjectStorage::SoftDeleteObjectStorage(
    const ObjectStoragePtr & object_storage_, const std::string & markers_path_, const std::string & disk_name_)
    : object_storage(object_storage_)
    , markers_path(markers_path_)
    , disk_name(disk_name_)
    , log(getLogger(getName()))
{
}

void SoftDeleteObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    LOG_DEBUG(log, "removeObjectIfExists: {} -> {}", object.remote_path, object.local_path);
    removeObjectImpl(object.remote_path);
}

void SoftDeleteObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    for (const auto & object : objects)
    {
        LOG_DEBUG(log, "removeObjectsIfExist: {} -> {}", object.remote_path, object.local_path);
        removeObjectImpl(object.remote_path);
    }
}

bool SoftDeleteObjectStorage::exists(const StoredObject & object) const
{
    return !isSoftDeleted(object.remote_path) && object_storage->exists(object);
}

void SoftDeleteObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
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

ObjectStorageIteratorPtr SoftDeleteObjectStorage::iterate(
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

bool SoftDeleteObjectStorage::isSoftDeleted(const std::string & object_path) const
{
    return FS::exists(getRemovedMarkerPath(object_path));
}

std::string SoftDeleteObjectStorage::getRemovedMarkerPath(const std::string & object_path) const
{
    return fs::path(markers_path) / escapeForFileName(object_path);
}

void SoftDeleteObjectStorage::removeObjectImpl(const std::string & object_path) const
{
    const std::string removed_marker_path = getRemovedMarkerPath(object_path);
    LOG_DEBUG(log, "adding removed marker: {}", removed_marker_path);

    /// Presence-only marker: tolerate an existing marker so removing an
    /// already-removed object is a no-op (removeObjectsIfExist contract).
    int fd = ::open(removed_marker_path.c_str(), O_WRONLY | O_CREAT, 0666);
    if (fd == -1)
        ErrnoException::throwFromPath(
            ErrorCodes::CANNOT_CREATE_FILE, removed_marker_path, "Cannot create file: {}", removed_marker_path);
    ::close(fd);
}

}
