#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/ObjectStorages/SoftDelete/SoftDeleteMarkerStore.h>
#include <Common/Exception.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>

#include <atomic>
#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;
using DB::SoftDeleteMarkerStore;

namespace
{

constexpr size_t KEY_WIDTH = 8;

std::string key(size_t i)
{
    auto res = std::to_string(i);
    return std::string(KEY_WIDTH - res.size(), '0') + res;
}

class SoftDeleteMarkerStoreTest : public testing::Test
{
protected:
    void SetUp() override
    {
        dir = fs::temp_directory_path() / ("soft_delete_marker_store_" + DB::getRandomASCIIString(16));
    }

    void TearDown() override { fs::remove_all(dir); }

    std::unique_ptr<SoftDeleteMarkerStore> open(size_t key_width = KEY_WIDTH)
    {
        return std::make_unique<SoftDeleteMarkerStore>(dir, key_width, getLogger("SoftDeleteMarkerStoreTest"));
    }

    fs::path dir;
    std::atomic<bool> not_cancelled{false};
};

}

TEST_F(SoftDeleteMarkerStoreTest, AddContainsReopen)
{
    {
        auto store = open();
        store->add({key(3), key(1), key(2)}, 100);
        EXPECT_TRUE(store->contains(key(1)));
        EXPECT_TRUE(store->contains(key(3)));
        EXPECT_FALSE(store->contains(key(4)));
        EXPECT_FALSE(store->contains("short"));
    }

    /// The key width of an existing store wins over the one passed in.
    auto store = open(KEY_WIDTH + 1);
    EXPECT_EQ(store->keyWidth(), KEY_WIDTH);
    EXPECT_EQ(store->deltaSize(), 3u);
    EXPECT_TRUE(store->contains(key(2)));
}

TEST_F(SoftDeleteMarkerStoreTest, SecondOpenIsLocked)
{
    auto store = open();
    EXPECT_THROW(open(), DB::Exception);
}

TEST_F(SoftDeleteMarkerStoreTest, MergeKeepsSortedAndDeduplicates)
{
    auto store = open();
    for (size_t round = 0; round < 3; ++round)
    {
        std::vector<std::string> keys;
        for (size_t i = round; i < 1000; i += 3)
            keys.push_back(key(i));
        keys.push_back(key(0));
        store->add(keys, 100);
        ASSERT_TRUE(store->merge(not_cancelled));
    }

    EXPECT_EQ(store->deltaSize(), 0u);
    EXPECT_EQ(store->mainSize(), 1000u);
    for (size_t i = 0; i < 1000; ++i)
        ASSERT_TRUE(store->contains(key(i))) << i;
    EXPECT_FALSE(store->contains(key(1000)));

    store.reset();
    store = open();
    EXPECT_EQ(store->mainSize(), 1000u);
    EXPECT_TRUE(store->contains(key(999)));
}

TEST_F(SoftDeleteMarkerStoreTest, PartialRecordIsDroppedOnOpen)
{
    {
        auto store = open();
        store->add({key(1), key(2)}, 100);
    }
    {
        std::ofstream out(dir / "delta.log", std::ios::binary | std::ios::app);
        out << "0000";
    }

    auto store = open();
    EXPECT_EQ(store->deltaSize(), 2u);
    store->add({key(3)}, 100);
    store.reset();

    store = open();
    EXPECT_EQ(store->deltaSize(), 3u);
    EXPECT_TRUE(store->contains(key(3)));
}

TEST_F(SoftDeleteMarkerStoreTest, CancelledMergeIsResumed)
{
    std::atomic<bool> cancelled{true};
    {
        auto store = open();
        store->add({key(1), key(2)}, 100);
        EXPECT_FALSE(store->merge(cancelled));
        EXPECT_TRUE(fs::exists(dir / "delta.merging"));
        EXPECT_TRUE(store->contains(key(1)));

        store->add({key(3)}, 100);
        EXPECT_TRUE(store->contains(key(3)));
    }

    /// The interrupted merge's records go back into delta.log on open.
    auto store = open();
    EXPECT_FALSE(fs::exists(dir / "delta.merging"));
    EXPECT_EQ(store->deltaSize(), 3u);
    ASSERT_TRUE(store->merge(not_cancelled));
    EXPECT_EQ(store->mainSize(), 3u);
    EXPECT_TRUE(store->contains(key(1)));
    EXPECT_TRUE(store->contains(key(3)));
}
