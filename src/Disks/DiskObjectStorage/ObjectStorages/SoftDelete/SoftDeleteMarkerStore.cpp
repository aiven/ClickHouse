#include <Disks/DiskObjectStorage/ObjectStorages/SoftDelete/SoftDeleteMarkerStore.h>

#include <IO/MMappedFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <base/unaligned.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <cstring>
#include <optional>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_FSYNC;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{

constexpr char MAGIC[8] = {'C', 'H', 'S', 'D', 'M', 'K', '0', '1'};
constexpr size_t HEADER_SIZE = 16;
constexpr size_t TIMESTAMP_SIZE = sizeof(UInt32);

constexpr auto MAIN_FILE = "main.dat";
constexpr auto MAIN_TMP_FILE = "main.tmp";
constexpr auto DELTA_FILE = "delta.log";
constexpr auto DELTA_NEW_FILE = "delta.new";
constexpr auto DELTA_MERGING_FILE = "delta.merging";
constexpr auto LOCK_FILE = "lock";

std::string makeHeader(size_t key_width)
{
    std::string header(HEADER_SIZE, '\0');
    memcpy(header.data(), MAGIC, sizeof(MAGIC));
    unalignedStoreLittleEndian<UInt32>(header.data() + sizeof(MAGIC), static_cast<UInt32>(key_width));
    return header;
}

size_t parseHeader(const char * data, const fs::path & path)
{
    if (memcmp(data, MAGIC, sizeof(MAGIC)) != 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Soft-delete marker file {} has a wrong header", path.string());
    return unalignedLoadLittleEndian<UInt32>(data + sizeof(MAGIC));
}

std::optional<size_t> tryReadKeyWidth(const fs::path & path)
{
    if (!fs::exists(path) || fs::file_size(path) < HEADER_SIZE)
        return {};
    ReadBufferFromFile in(path.string());
    char header[HEADER_SIZE];
    in.readStrict(header, HEADER_SIZE);
    return parseHeader(header, path);
}

int openOrThrow(const fs::path & path, int flags)
{
    int fd = ::open(path.c_str(), flags, 0644);
    if (fd == -1)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_OPEN_FILE, path.string(), "Cannot open file {}", path.string());
    return fd;
}

void closeFd(int fd)
{
    [[maybe_unused]] int err = ::close(fd);
    chassert(err == 0 || errno != EBADF);
}

void writeAll(int fd, const char * data, size_t size, const fs::path & path)
{
    while (size > 0)
    {
        ssize_t res = ::write(fd, data, size);
        if (res < 0)
        {
            if (errno == EINTR)
                continue;
            ErrnoException::throwFromPath(
                ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, path.string(), "Cannot write to file {}", path.string());
        }
        data += res;
        size -= static_cast<size_t>(res);
    }
}

void writeNewFile(const fs::path & path, const std::string & data)
{
    int fd = openOrThrow(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC);
    try
    {
        writeAll(fd, data.data(), data.size(), path);
    }
    catch (...)
    {
        closeFd(fd);
        throw;
    }
    closeFd(fd);
}

void fsyncDirectory(const fs::path & path)
{
    int fd = openOrThrow(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    int res = ::fsync(fd);
    int saved_errno = errno;
    closeFd(fd);
    errno = saved_errno;
    if (res != 0)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_FSYNC, path.string(), "Cannot fsync directory {}", path.string());
}

bool containsUnsorted(const std::string & records, std::string_view key, size_t record_size)
{
    for (size_t pos = 0; pos + record_size <= records.size(); pos += record_size)
        if (memcmp(records.data() + pos, key.data(), key.size()) == 0)
            return true;
    return false;
}

bool containsSorted(const char * records, size_t count, std::string_view key, size_t record_size)
{
    size_t lo = 0;
    size_t hi = count;
    while (lo < hi)
    {
        size_t mid = lo + (hi - lo) / 2;
        if (memcmp(records + mid * record_size, key.data(), key.size()) < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo < count && memcmp(records + lo * record_size, key.data(), key.size()) == 0;
}

}

struct SoftDeleteMarkerStore::MainFile
{
    std::unique_ptr<MMappedFile> mapped;
    size_t count = 0;

    const char * records() const { return mapped->getData() + HEADER_SIZE; }
};

SoftDeleteMarkerStore::SoftDeleteMarkerStore(const fs::path & dir_, size_t key_width_, LoggerPtr log_)
    : dir(dir_)
    , log(std::move(log_))
{
    fs::create_directories(dir);

    lock_fd = openOrThrow(pathOf(LOCK_FILE), O_RDWR | O_CREAT | O_CLOEXEC);
    if (::flock(lock_fd, LOCK_EX | LOCK_NB) != 0)
    {
        int saved_errno = errno;
        closeFd(lock_fd);
        errno = saved_errno;
        ErrnoException::throwFromPath(
            ErrorCodes::CANNOT_OPEN_FILE, dir.string(), "Soft-delete marker store {} is locked by another process", dir.string());
    }

    try
    {
        key_width = detectKeyWidth(key_width_);
        if (key_width == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Soft-delete marker store {} needs a non-zero key width", dir.string());
        record_size = key_width + TIMESTAMP_SIZE;

        fs::remove(pathOf(MAIN_TMP_FILE));
        fs::remove(pathOf(DELTA_NEW_FILE));

        main = openMain();

        const auto delta_path = pathOf(DELTA_FILE);
        if (fs::exists(delta_path) && fs::file_size(delta_path) >= HEADER_SIZE)
            delta = readRecords(delta_path, /*truncate_partial_tail=*/ true);
        else
            writeNewFile(delta_path, makeHeader(key_width));
        delta_fd = openOrThrow(delta_path, O_WRONLY | O_APPEND | O_CLOEXEC);
        delta_file_size = HEADER_SIZE + delta.size();

        /// A merge was interrupted: its delta goes back into delta.log and is merged again later.
        const auto merging_path = pathOf(DELTA_MERGING_FILE);
        if (fs::exists(merging_path))
        {
            if (fs::file_size(merging_path) >= HEADER_SIZE)
            {
                auto records = readRecords(merging_path, /*truncate_partial_tail=*/ false);
                writeAll(delta_fd, records.data(), records.size(), delta_path);
                delta_file_size += records.size();
                delta += records;
                sync();
            }
            fs::remove(merging_path);
        }
    }
    catch (...)
    {
        if (delta_fd != -1)
            closeFd(delta_fd);
        closeFd(lock_fd);
        throw;
    }

    LOG_INFO(
        log, "Opened soft-delete marker store {} with key width {}: {} sorted and {} unsorted records",
        dir.string(), key_width, mainSize(), deltaSize());
}

SoftDeleteMarkerStore::~SoftDeleteMarkerStore()
{
    if (delta_fd != -1)
        closeFd(delta_fd);
    if (lock_fd != -1)
        closeFd(lock_fd);
}

size_t SoftDeleteMarkerStore::detectKeyWidth(size_t default_width) const
{
    for (const auto * name : {MAIN_FILE, DELTA_MERGING_FILE, DELTA_FILE})
        if (auto width = tryReadKeyWidth(pathOf(name)))
            return *width;
    return default_width;
}

SoftDeleteMarkerStore::MainFilePtr SoftDeleteMarkerStore::openMain() const
{
    const auto path = pathOf(MAIN_FILE);
    if (!fs::exists(path))
        return nullptr;

    const size_t size = fs::file_size(path);
    if (size < HEADER_SIZE || (size - HEADER_SIZE) % record_size != 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Soft-delete marker file {} has unexpected size {}", path.string(), size);

    const size_t count = (size - HEADER_SIZE) / record_size;
    if (count == 0)
        return nullptr;

    auto main_file = std::make_shared<MainFile>();
    main_file->mapped = std::make_unique<MMappedFile>(path.string(), 0, size);
    main_file->count = count;
    if (parseHeader(main_file->mapped->getData(), path) != key_width)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Soft-delete marker file {} has a different key width", path.string());
    return main_file;
}

std::string SoftDeleteMarkerStore::readRecords(const fs::path & path, bool truncate_partial_tail) const
{
    std::string data;
    {
        ReadBufferFromFile in(path.string());
        readStringUntilEOF(data, in);
    }

    if (parseHeader(data.data(), path) != key_width)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Soft-delete marker file {} has a different key width", path.string());

    const size_t whole = (data.size() - HEADER_SIZE) / record_size * record_size;
    if (HEADER_SIZE + whole != data.size())
    {
        LOG_WARNING(log, "Dropping a partially written record at the end of {}", path.string());
        if (truncate_partial_tail)
            fs::resize_file(path, HEADER_SIZE + whole);
    }
    return data.substr(HEADER_SIZE, whole);
}

void SoftDeleteMarkerStore::add(const std::vector<std::string> & keys, UInt32 now)
{
    if (keys.empty())
        return;

    std::string batch;
    batch.reserve(keys.size() * record_size);
    char timestamp[TIMESTAMP_SIZE];
    unalignedStoreLittleEndian<UInt32>(timestamp, now);
    for (const auto & key : keys)
    {
        if (key.size() != key_width)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Key {} does not have the width {} of soft-delete marker store", key, key_width);
        batch.append(key);
        batch.append(timestamp, TIMESTAMP_SIZE);
    }

    std::lock_guard lock(mutex);
    try
    {
        writeAll(delta_fd, batch.data(), batch.size(), pathOf(DELTA_FILE));
    }
    catch (...)
    {
        /// Drop a partially written batch so later appends stay record-aligned.
        if (::ftruncate(delta_fd, delta_file_size) != 0)
            LOG_ERROR(log, "Cannot truncate {} after a failed write", pathOf(DELTA_FILE).string());
        throw;
    }
    delta_file_size += batch.size();
    delta += batch;
}

void SoftDeleteMarkerStore::sync()
{
    std::lock_guard lock(mutex);
    if (::fdatasync(delta_fd) != 0)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_FSYNC, pathOf(DELTA_FILE).string(), "Cannot fsync {}", pathOf(DELTA_FILE).string());
}

bool SoftDeleteMarkerStore::contains(std::string_view key) const
{
    if (key.size() != key_width)
        return false;

    MainFilePtr main_snapshot;
    {
        std::lock_guard lock(mutex);
        if (containsUnsorted(delta, key, record_size) || containsUnsorted(merging, key, record_size))
            return true;
        main_snapshot = main;
    }
    return main_snapshot && containsSorted(main_snapshot->records(), main_snapshot->count, key, record_size);
}

size_t SoftDeleteMarkerStore::deltaSize() const
{
    std::lock_guard lock(mutex);
    return delta.size() / record_size;
}

size_t SoftDeleteMarkerStore::mainSize() const
{
    std::lock_guard lock(mutex);
    return main ? main->count : 0;
}

bool SoftDeleteMarkerStore::merge(const std::atomic<bool> & cancelled)
{
    std::lock_guard merge_lock(merge_mutex);
    Stopwatch watch;

    MainFilePtr main_snapshot;
    {
        std::lock_guard lock(mutex);
        /// A cancelled merge leaves its delta in `merging`; that one is finished before rotating again.
        if (merging.empty() && !delta.empty())
        {
            const auto delta_path = pathOf(DELTA_FILE);
            const auto new_path = pathOf(DELTA_NEW_FILE);
            writeNewFile(new_path, makeHeader(key_width));
            int new_fd = openOrThrow(new_path, O_WRONLY | O_APPEND | O_CLOEXEC);
            try
            {
                fs::rename(delta_path, pathOf(DELTA_MERGING_FILE));
                fs::rename(new_path, delta_path);
            }
            catch (...)
            {
                closeFd(new_fd);
                throw;
            }
            closeFd(delta_fd);
            delta_fd = new_fd;
            delta_file_size = HEADER_SIZE;
            merging = std::move(delta);
            delta.clear();
        }
        main_snapshot = main;
    }

    /// Only this thread modifies `merging`, so it can be read without `mutex`.
    std::vector<const char *> sorted;
    sorted.reserve(merging.size() / record_size);
    for (size_t pos = 0; pos + record_size <= merging.size(); pos += record_size)
        sorted.push_back(merging.data() + pos);
    std::sort(sorted.begin(), sorted.end(), [&](const char * a, const char * b) { return memcmp(a, b, key_width) < 0; });
    if (sorted.empty())
        return true;

    const char * main_records = main_snapshot ? main_snapshot->records() : nullptr;
    const size_t main_count = main_snapshot ? main_snapshot->count : 0;

    const auto tmp_path = pathOf(MAIN_TMP_FILE);
    size_t written = 0;
    {
        WriteBufferFromFile out(tmp_path.string(), DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC);
        try
        {
            const auto header = makeHeader(key_width);
            out.write(header.data(), header.size());

            const char * last = nullptr;
            auto emit = [&](const char * record)
            {
                if (last && memcmp(last, record, key_width) == 0)
                    return;
                out.write(record, record_size);
                last = record;
                ++written;
            };

            size_t i = 0;
            size_t j = 0;
            while (i < main_count || j < sorted.size())
            {
                if (cancelled.load(std::memory_order_relaxed))
                {
                    out.cancel();
                    fs::remove(tmp_path);
                    return false;
                }

                const char * main_record = i < main_count ? main_records + i * record_size : nullptr;
                const char * delta_record = j < sorted.size() ? sorted[j] : nullptr;
                int cmp = !main_record ? 1 : (!delta_record ? -1 : memcmp(main_record, delta_record, key_width));
                if (cmp <= 0)
                {
                    ++i;
                    if (cmp == 0)
                        ++j;
                    emit(main_record);
                }
                else
                {
                    ++j;
                    emit(delta_record);
                }
            }

            out.finalize();
            out.sync();
        }
        catch (...)
        {
            out.cancel();
            fs::remove(tmp_path);
            throw;
        }
    }

    fs::rename(tmp_path, pathOf(MAIN_FILE));
    fsyncDirectory(dir);
    auto new_main = openMain();

    const size_t merged = sorted.size();
    {
        std::lock_guard lock(mutex);
        main = std::move(new_main);
        merging.clear();
        merging.shrink_to_fit();
    }
    fs::remove(pathOf(DELTA_MERGING_FILE));

    LOG_INFO(
        log, "Merged {} records into {}, now {} records, took {} ms",
        merged, pathOf(MAIN_FILE).string(), written, watch.elapsedMilliseconds());
    return true;
}

}
