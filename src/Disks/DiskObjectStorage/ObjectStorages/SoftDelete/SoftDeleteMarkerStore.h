#pragma once

#include <base/types.h>
#include <Common/Logger.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace DB
{

class MMappedFile;

/// Persistent set of soft-deleted blob keys, all of the same length.
///
/// Files in `dir`, each starting with a 16-byte header (magic, key width):
///   main.dat       sorted fixed-width records `key | u32 deleted_at`, rewritten only by `merge`
///   delta.log      unsorted records appended by `add`, also held in memory
///   delta.merging  the delta being folded into main.dat by an ongoing or interrupted merge
class SoftDeleteMarkerStore
{
public:
    /// `key_width` is only used when `dir` holds no store yet.
    SoftDeleteMarkerStore(const std::filesystem::path & dir_, size_t key_width_, LoggerPtr log_);
    ~SoftDeleteMarkerStore();

    size_t keyWidth() const { return key_width; }

    /// Every key must be `keyWidth` long.
    void add(const std::vector<std::string> & keys, UInt32 now);

    /// Makes previously added keys durable.
    void sync();

    bool contains(std::string_view key) const;

    size_t deltaSize() const;
    size_t mainSize() const;

    /// Folds the delta into main.dat. Returns false if `cancelled` was set before the merge finished.
    bool merge(const std::atomic<bool> & cancelled);

private:
    struct MainFile;
    using MainFilePtr = std::shared_ptr<const MainFile>;

    std::filesystem::path pathOf(const char * name) const { return dir / name; }

    size_t detectKeyWidth(size_t default_width) const;
    MainFilePtr openMain() const;
    std::string readRecords(const std::filesystem::path & path, bool truncate_partial_tail) const;

    const std::filesystem::path dir;
    size_t key_width = 0;
    size_t record_size = 0;
    LoggerPtr log;

    int lock_fd = -1;

    mutable std::mutex mutex;
    int delta_fd = -1;
    size_t delta_file_size = 0;
    std::string delta;
    std::string merging;
    MainFilePtr main;

    std::mutex merge_mutex;
};

}
