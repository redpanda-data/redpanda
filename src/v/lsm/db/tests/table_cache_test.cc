// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "base/seastarx.h"
#include "lsm/core/internal/files.h"
#include "lsm/db/table_cache.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/sst/builder.h"
#include "test_utils/async.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

namespace {

using lsm::internal::file_handle;
using lsm::internal::operator""_file_id;
using lsm::internal::operator""_db_epoch;

class TableCacheTest : public testing::Test {
public:
    constexpr static size_t default_max_entries = 10;
    std::pair<lsm::internal::file_handle, size_t> make_sst() {
        auto filename = file_handle{.id = ++_latest_id, .epoch = 0_db_epoch};
        auto file = _persistence->open_sequential_writer(filename).get();
        lsm::sst::builder builder(std::move(file), {});
        // Just make empty SST files - the cache doesn't care about the contents
        builder.finish().get();
        builder.close().get();
        return std::make_pair(filename, builder.file_size());
    }

    lsm::db::table_cache
    make_table_cache(size_t max_entries = default_max_entries) {
        return {
          _persistence.get(),
          max_entries,
          ss::make_lw_shared<lsm::probe>(),
          ss::make_lw_shared<lsm::sst::block_cache>(
            1_MiB, ss::make_lw_shared<lsm::probe>())};
    }

    void TearDown() override { _persistence->close().get(); }

private:
    lsm::internal::file_id _latest_id;
    std::unique_ptr<lsm::io::data_persistence> _persistence
      = lsm::io::make_memory_data_persistence();
};

} // namespace

TEST_F(TableCacheTest, CanOpenFiles) {
    auto cache = make_table_cache();
    auto [id1, size1] = make_sst();
    auto it = cache.create_iterator(id1, size1).get();
    EXPECT_EQ(cache.statistics().open_file_handles, 1);
    it = nullptr;
    EXPECT_EQ(cache.statistics().open_file_handles, 1);
    cache.close().get();
}

TEST_F(TableCacheTest, ThrowsOnMissingFiles) {
    auto cache = make_table_cache();
    EXPECT_ANY_THROW(cache.create_iterator({.id = 999_file_id}, 10).get());
    EXPECT_EQ(cache.statistics().open_file_handles, 0);
    cache.close().get();
}

TEST_F(TableCacheTest, MaxEntries) {
    auto cache = make_table_cache();
    std::map<lsm::internal::file_handle, size_t> files;
    for (size_t i = 0; i < default_max_entries * 2; ++i) {
        files.insert(make_sst());
    }
    for (const auto& [h, size] : files) {
        cache.create_iterator(h, size).get();
    }
    tests::drain_task_queue().get();
    // We get 5 on the small queue (+1 over the limit) and a full ghost queue of
    // 2 entries. The main queue is empty because nothing is touched twice.
    EXPECT_EQ(cache.statistics().open_file_handles, 7) << cache.statistics();
    cache.close().get();
}

TEST_F(TableCacheTest, MaxEntriesWithOpenIterators) {
    auto cache = make_table_cache();
    std::map<lsm::internal::file_handle, size_t> files;
    for (size_t i = 0; i < default_max_entries * 2; ++i) {
        files.insert(make_sst());
    }
    std::vector<std::unique_ptr<lsm::internal::iterator>> iters;
    iters.reserve(files.size());
    for (const auto& [h, size] : files) {
        iters.push_back(cache.create_iterator(h, size).get());
    }
    // We burst over because there are open iterators. We could consider instead
    // limiting new entries to be created past the limit when there are open
    // iterators, but for now we burst.
    EXPECT_EQ(cache.statistics().open_file_handles, 20) << cache.statistics();
    iters.clear();
    tests::drain_task_queue().get();
    // But once everything is cleaned up, we only have 7 things enqueued (for
    // why 7 see the comment inMaxEntries).
    EXPECT_EQ(cache.statistics().open_file_handles, 7) << cache.statistics();
    cache.close().get();
}
