#include <Disks/DiskObjectStorage/ObjectStorages/SoftDelete/SoftDeleteObjectStorage.h>

#include <filesystem>
#include <fcntl.h>
#include <Core/BackgroundSchedulePool.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <Common/ErrnoException.h>
#include <Common/escapeForFileName.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CLOSE_FILE;
    extern const int CANNOT_CREATE_FILE;
}

namespace
{

constexpr size_t BACKGROUND_TASK_PERIOD_MS = 60'000;
constexpr size_t MARKER_FILES_MIGRATION_BATCH = 10'000;

}

SoftDeleteObjectStorage::SoftDeleteObjectStorage(
    const ObjectStoragePtr & object_storage_,
    const std::string & markers_path_,
    const std::string & disk_name_,
    const SoftDeleteSettings & settings_)
    : object_storage(object_storage_)
    , markers_path(markers_path_)
    , disk_name(disk_name_)
    , settings(settings_)
    , log(getLogger(getName()))
{
    try
    {
        const size_t key_width = object_storage->createKeyGenerator()->generate().serialize().size();
        store = std::make_unique<SoftDeleteMarkerStore>(fs::path(markers_path) / "store", key_width, log);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot open the soft-delete marker store, every soft-deleted blob gets its own marker file");
    }
}

SoftDeleteObjectStorage::~SoftDeleteObjectStorage()
{
    cancelled = true;
    if (task)
        task->deactivate();
}

void SoftDeleteObjectStorage::startup()
{
    object_storage->startup();
    if (!store || task)
        return;

    task = Context::getGlobalContextInstance()->getSchedulePool().createTask(
        StorageID::createEmpty(), getName(), [this] { backgroundTask(); });
    task->activateAndSchedule();
}

void SoftDeleteObjectStorage::shutdown()
{
    cancelled = true;
    if (task)
        task->deactivate();
    object_storage->shutdown();
}

void SoftDeleteObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    removeObjectsIfExist({object});
}

void SoftDeleteObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    std::vector<std::string> keys;
    for (const auto & object : objects)
    {
        LOG_DEBUG(log, "removeObjectsIfExist: {} -> {}", object.remote_path, object.local_path);
        if (store && object.remote_path.size() == store->keyWidth())
            keys.push_back(object.remote_path);
        else
            writeMarkerFile(object.remote_path);
    }

    if (keys.empty())
        return;

    store->add(keys, static_cast<UInt32>(time(nullptr)));
    if (task && store->deltaSize() >= settings.merge_threshold_rows)
        task->schedule();
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
    return (store && store->contains(object_path)) || FS::exists(getRemovedMarkerPath(object_path));
}

std::string SoftDeleteObjectStorage::getRemovedMarkerPath(const std::string & object_path) const
{
    return fs::path(markers_path) / escapeForFileName(object_path);
}

void SoftDeleteObjectStorage::writeMarkerFile(const std::string & object_path) const
{
    const std::string removed_marker_path = getRemovedMarkerPath(object_path);
    LOG_DEBUG(log, "adding removed marker: {}", removed_marker_path);

    /// Presence-only marker: tolerate an existing marker so removing an
    /// already-removed object is a no-op (removeObjectsIfExist contract).
    int fd = ::open(removed_marker_path.c_str(), O_WRONLY | O_CREAT, 0666);
    if (fd == -1)
        ErrnoException::throwFromPath(
            ErrorCodes::CANNOT_CREATE_FILE, removed_marker_path, "Cannot create file: {}", removed_marker_path);
    if (0 != ::close(fd))
        ErrnoException::throwFromPath(
            ErrorCodes::CANNOT_CLOSE_FILE, removed_marker_path, "Cannot close file: {}", removed_marker_path);
}

void SoftDeleteObjectStorage::backgroundTask()
{
    try
    {
        if (!marker_files_migrated)
            marker_files_migrated = migrateMarkerFiles();

        if (store->deltaSize() >= settings.merge_threshold_rows)
            store->merge(cancelled);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Soft-delete marker store maintenance failed");
    }

    if (!cancelled)
        task->scheduleAfter(BACKGROUND_TASK_PERIOD_MS);
}

bool SoftDeleteObjectStorage::migrateMarkerFiles()
{
    std::vector<std::string> keys;
    std::vector<fs::path> files;
    size_t migrated = 0;

    auto flush = [&]
    {
        store->add(keys, static_cast<UInt32>(time(nullptr)));
        store->sync();
        for (const auto & file : files)
            fs::remove(file);
        migrated += keys.size();
        keys.clear();
        files.clear();
    };

    for (const auto & entry : fs::directory_iterator(markers_path))
    {
        if (cancelled)
            return false;
        if (!entry.is_regular_file())
            continue;

        auto key = unescapeForFileName(entry.path().filename().string());
        if (key.size() != store->keyWidth())
            continue;

        keys.push_back(std::move(key));
        files.push_back(entry.path());
        if (keys.size() >= MARKER_FILES_MIGRATION_BATCH)
            flush();
    }
    if (!keys.empty())
        flush();

    if (migrated)
        LOG_INFO(log, "Moved {} soft-delete marker files into the marker store", migrated);
    return true;
}

}
