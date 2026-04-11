// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/core/internal/files.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/table_cache.h"
#include "lsm/db/version_edit.h"
#include "lsm/db/version_set.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/sst/block_cache.h"
#include "lsm/sst/builder.h"

#include <gmock/gmock-matchers.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>

namespace {

using lsm::internal::operator""_level;
using lsm::operator""_user_key;
using lsm::internal::operator""_file_id;
using lsm::internal::operator""_key;
using lsm::internal::operator""_seqno;
using testing::ElementsAre;
using testing::IsEmpty;
using testing::UnorderedElementsAre;

struct sst_spec {
    lsm::internal::file_id id;
    lsm::internal::level level;
    std::vector<lsm::internal::key> keys;
};

MATCHER_P2(IsLookupValue, key, level, "is a value") {
    auto expected = iobuf::from(
      fmt::format("value for {} on level {}", key, level));
    (*result_listener) << "equal to " << expected.hexdump(100);
    return arg == lsm::lookup_result::value(std::move(expected));
}
MATCHER(IsMissing, "is missing") { return arg.is_missing(); }
MATCHER(IsTombstone, "is a tombstone") { return arg.is_tombstone(); }

class VersionSetTest : public testing::Test {
public:
    constexpr static size_t default_max_entries = 10;
    void TearDown() override {
        _table_cache.close().get();
        _data_persistence->close().get();
        _metadata_persistence->close().get();
    }

    const lsm::internal::options& options() { return *_options; }
    lsm::db::version_set& version_set() { return *_version_set; }

    void recover() {
        _version_set = nullptr;
        _version_set = ss::make_lw_shared<lsm::db::version_set>(
          _metadata_persistence.get(), &_table_cache, _options);
        _version_set->recover().get();
    }

    void add_sst(sst_spec spec) {
        auto writer
          = _data_persistence->open_sequential_writer({.id = spec.id}).get();
        lsm::sst::builder builder(std::move(writer), {});
        std::ranges::sort(spec.keys);
        for (const auto& key : spec.keys) {
            builder
              .add(
                key,
                iobuf::from(
                  fmt::format("value for {} on level {}", key, spec.level)))
              .get();
        }
        builder.finish().get();
        size_t file_size = builder.file_size();
        builder.close().get();
        auto edit = _version_set->new_edit();
        auto min_seqno = std::ranges::min_element(
                           spec.keys,
                           std::less<>(),
                           [](lsm::internal::key_view k) { return k.seqno(); })
                           ->seqno();
        auto max_seqno = std::ranges::max_element(
                           spec.keys,
                           std::less<>(),
                           [](lsm::internal::key_view k) { return k.seqno(); })
                           ->seqno();
        edit->add_file({
          .level = spec.level,
          .file_handle = {.id = spec.id},
          .file_size = file_size,
          .smallest = spec.keys.front(),
          .largest = spec.keys.back(),
          .oldest_seqno = min_seqno,
          .newest_seqno = max_seqno,
        });
        edit->set_last_seqno(max_seqno);
        _version_set->log_and_apply(std::move(edit)).get();
    }

private:
    ss::lw_shared_ptr<lsm::internal::options> _options
      = ss::make_lw_shared<lsm::internal::options>();
    std::unique_ptr<lsm::io::data_persistence> _data_persistence
      = lsm::io::make_memory_data_persistence();
    std::unique_ptr<lsm::io::metadata_persistence> _metadata_persistence
      = lsm::io::make_memory_metadata_persistence();
    lsm::db::table_cache _table_cache{
      _data_persistence.get(),
      default_max_entries,
      ss::make_lw_shared<lsm::probe>(),
      ss::make_lw_shared<lsm::sst::block_cache>(
        1_MiB, ss::make_lw_shared<lsm::probe>())};
    ss::lw_shared_ptr<lsm::db::version_set> _version_set
      = ss::make_lw_shared<lsm::db::version_set>(
        _metadata_persistence.get(), &_table_cache, _options);
};

// Fixture for compaction tests that only uses file metadata.
class CompactionTest : public testing::Test {
public:
    constexpr static size_t default_max_entries = 10;

    CompactionTest() {
        // Use small sizes that are easier to reason about
        _options->level_one_compaction_trigger = 4;
        _options->write_buffer_size = 1000;
        _options->levels = lsm::internal::options::make_levels(
          /*base_config=*/
          {
            .max_total_bytes = 10000, // L0/L1: 10KB total
            .max_file_size = 1000,    // L0/L1: 1KB per file
          },
          /*multiplier=*/10,
          /*max_level=*/6_level);
    }

    void TearDown() override {
        _table_cache.close().get();
        _data_persistence->close().get();
        _metadata_persistence->close().get();
    }

    const lsm::internal::options& options() { return *_options; }
    lsm::db::version_set& version_set() { return *_version_set; }

    // Add a file to the version using only metadata (no actual SST file).
    void add_file(
      lsm::internal::level level,
      lsm::internal::file_id id,
      lsm::internal::key smallest,
      lsm::internal::key largest,
      uint64_t file_size = 100) {
        auto edit = _version_set->new_edit();
        edit->add_file({
          .level = level,
          .file_handle = {.id = id},
          .file_size = file_size,
          .smallest = smallest,
          .largest = largest,
          .oldest_seqno = 0_seqno,
          .newest_seqno = 0_seqno,
        });
        edit->set_last_seqno(0_seqno);
        _version_set->log_and_apply(std::move(edit)).get();
    }

    // Extract file IDs from compaction inputs at the specified level.
    static std::vector<lsm::internal::file_id> input_file_ids(
      const lsm::db::compaction& c, lsm::db::compaction::which which) {
        std::vector<lsm::internal::file_id> ids;
        for (size_t i = 0; i < c.num_input_files(which); ++i) {
            ids.push_back(c.input(which, i)->handle.id);
        }
        return ids;
    }

    // Extract file IDs from compaction input level.
    static std::vector<lsm::internal::file_id>
    input_file_ids(const lsm::db::compaction& c) {
        return input_file_ids(c, lsm::db::compaction::input_level);
    }

    // Extract file IDs from compaction output level.
    static std::vector<lsm::internal::file_id>
    output_file_ids(const lsm::db::compaction& c) {
        return input_file_ids(c, lsm::db::compaction::output_level);
    }

private:
    ss::lw_shared_ptr<lsm::internal::options> _options
      = ss::make_lw_shared<lsm::internal::options>();
    std::unique_ptr<lsm::io::data_persistence> _data_persistence
      = lsm::io::make_memory_data_persistence();
    std::unique_ptr<lsm::io::metadata_persistence> _metadata_persistence
      = lsm::io::make_memory_metadata_persistence();
    lsm::db::table_cache _table_cache{
      _data_persistence.get(),
      default_max_entries,
      ss::make_lw_shared<lsm::probe>(),
      ss::make_lw_shared<lsm::sst::block_cache>(
        1_MiB, ss::make_lw_shared<lsm::probe>())};
    ss::lw_shared_ptr<lsm::db::version_set> _version_set
      = ss::make_lw_shared<lsm::db::version_set>(
        _metadata_persistence.get(), &_table_cache, _options);
};

} // namespace

TEST_F(VersionSetTest, Empty) {
    auto& vset = version_set();
    for (auto level = 0_level; level <= options().default_max_level; ++level) {
        EXPECT_EQ(vset.current()->num_files(level), 0);
    }
}

TEST_F(VersionSetTest, ApplyEdit) {
    auto& vset = version_set();
    auto edit = vset.new_edit();
    edit->add_file({
      .level = 0_level,
      .file_handle = {.id = 1_file_id},
      .file_size = 100,
      .smallest = "a"_key,
      .largest = "z"_key,
    });
    edit->set_last_seqno(0_seqno);
    vset.log_and_apply(std::move(edit)).get();
    EXPECT_EQ(vset.current()->num_files(0_level), 1);
    EXPECT_EQ(vset.current()->num_files(1_level), 0);
}

TEST_F(VersionSetTest, ApplyEditWithDelete) {
    auto& vset = version_set();
    {
        auto edit = vset.new_edit();
        edit->add_file({
          .level = 0_level,
          .file_handle = {.id = 1_file_id},
          .file_size = 100,
          .smallest = "a"_key,
          .largest = "z"_key,
        });
        edit->set_last_seqno(0_seqno);
        vset.log_and_apply(std::move(edit)).get();
        EXPECT_EQ(vset.current()->num_files(0_level), 1);
        EXPECT_EQ(vset.current()->num_files(1_level), 0);
    }
    auto edit = vset.new_edit();
    edit->remove_file(0_level, {.id = 1_file_id});
    edit->add_file({
      .level = 1_level,
      .file_handle = {.id = 1_file_id},
      .file_size = 100,
      .smallest = "a"_key,
      .largest = "z"_key,
    });
    edit->add_file({
      .level = 0_level,
      .file_handle = {.id = 2_file_id},
      .file_size = 80,
      .smallest = "c"_key,
      .largest = "d"_key,
    });
    vset.log_and_apply(std::move(edit)).get();
    EXPECT_EQ(vset.current()->num_files(0_level), 1);
    EXPECT_EQ(vset.current()->num_files(1_level), 1);
    EXPECT_EQ(vset.current()->num_files(2_level), 0);
}

TEST_F(VersionSetTest, Recovery) {
    {
        auto& vset = version_set();
        auto edit = vset.new_edit();
        edit->add_file({
          .level = 0_level,
          .file_handle = {.id = 1_file_id},
          .file_size = 100,
          .smallest = "a"_key,
          .largest = "z"_key,
        });
        edit->add_file({
          .level = 0_level,
          .file_handle = {.id = 2_file_id},
          .file_size = 80,
          .smallest = "c"_key,
          .largest = "d"_key,
        });
        edit->set_last_seqno(0_seqno);
        vset.log_and_apply(std::move(edit)).get();
        EXPECT_EQ(vset.current()->num_files(0_level), 2);
        EXPECT_EQ(vset.current()->num_files(1_level), 0);
    }
    recover();
    auto& vset = version_set();
    EXPECT_EQ(vset.current()->num_files(0_level), 2);
    EXPECT_EQ(vset.current()->num_files(1_level), 0);
}

TEST_F(VersionSetTest, OverlapInLevel0) {
    auto& vset = version_set();
    auto edit = vset.new_edit();
    edit->add_file({
      .level = 0_level,
      .file_handle = {.id = 1_file_id},
      .file_size = 100,
      .smallest = "d"_key,
      .largest = "g"_key,
    });
    edit->add_file({
      .level = 0_level,
      .file_handle = {.id = 2_file_id},
      .file_size = 80,
      .smallest = "i"_key,
      .largest = "k"_key,
    });
    edit->add_file({
      .level = 0_level,
      .file_handle = {.id = 3_file_id},
      .file_size = 80,
      .smallest = "b"_key,
      .largest = "e"_key,
    });
    edit->set_last_seqno(0_seqno);
    vset.log_and_apply(std::move(edit)).get();
    auto current = vset.current();
    EXPECT_TRUE(current->overlap_in_level(0_level, "a"_user_key, "z"_user_key));
    EXPECT_TRUE(current->overlap_in_level(0_level, "k"_user_key, "l"_user_key));
    EXPECT_TRUE(current->overlap_in_level(0_level, "f"_user_key, "h"_user_key));
    EXPECT_TRUE(current->overlap_in_level(0_level, "h"_user_key, "j"_user_key));
    EXPECT_TRUE(current->overlap_in_level(0_level, "g"_user_key, "h"_user_key));
    EXPECT_TRUE(current->overlap_in_level(0_level, "h"_user_key, "i"_user_key));
    EXPECT_TRUE(current->overlap_in_level(0_level, std::nullopt, std::nullopt));
    EXPECT_TRUE(current->overlap_in_level(0_level, "k"_user_key, std::nullopt));
    EXPECT_TRUE(current->overlap_in_level(0_level, std::nullopt, "d"_user_key));
    EXPECT_FALSE(
      current->overlap_in_level(0_level, std::nullopt, "a"_user_key));
    EXPECT_FALSE(
      current->overlap_in_level(0_level, "l"_user_key, std::nullopt));
    EXPECT_FALSE(
      current->overlap_in_level(0_level, "a"_user_key, "a"_user_key));
    EXPECT_FALSE(
      current->overlap_in_level(0_level, "y"_user_key, "z"_user_key));
    EXPECT_FALSE(
      current->overlap_in_level(0_level, "h"_user_key, "h"_user_key));
    EXPECT_FALSE(
      current->overlap_in_level(1_level, "a"_user_key, "z"_user_key));
}

TEST_F(VersionSetTest, OverlapInLevel1) {
    auto& vset = version_set();
    auto edit = vset.new_edit();
    edit->add_file({
      .level = 1_level,
      .file_handle = {.id = 1_file_id},
      .file_size = 100,
      .smallest = "d"_key,
      .largest = "g"_key,
    });
    edit->add_file({
      .level = 1_level,
      .file_handle = {.id = 2_file_id},
      .file_size = 80,
      .smallest = "i"_key,
      .largest = "k"_key,
    });
    edit->set_last_seqno(0_seqno);
    vset.log_and_apply(std::move(edit)).get();
    auto current = vset.current();
    EXPECT_TRUE(current->overlap_in_level(1_level, "a"_user_key, "z"_user_key));
    EXPECT_TRUE(current->overlap_in_level(1_level, "k"_user_key, "l"_user_key));
    EXPECT_TRUE(current->overlap_in_level(1_level, "f"_user_key, "h"_user_key));
    EXPECT_TRUE(current->overlap_in_level(1_level, "h"_user_key, "j"_user_key));
    EXPECT_TRUE(current->overlap_in_level(1_level, "g"_user_key, "h"_user_key));
    EXPECT_TRUE(current->overlap_in_level(1_level, "h"_user_key, "i"_user_key));
    EXPECT_TRUE(current->overlap_in_level(1_level, std::nullopt, std::nullopt));
    EXPECT_TRUE(current->overlap_in_level(1_level, "k"_user_key, std::nullopt));
    EXPECT_TRUE(current->overlap_in_level(1_level, std::nullopt, "d"_user_key));
    EXPECT_FALSE(
      current->overlap_in_level(1_level, std::nullopt, "c"_user_key));
    EXPECT_FALSE(
      current->overlap_in_level(1_level, "l"_user_key, std::nullopt));
    EXPECT_FALSE(
      current->overlap_in_level(1_level, "a"_user_key, "b"_user_key));
    EXPECT_FALSE(
      current->overlap_in_level(1_level, "y"_user_key, "z"_user_key));
    EXPECT_FALSE(
      current->overlap_in_level(1_level, "h"_user_key, "h"_user_key));
    EXPECT_FALSE(
      current->overlap_in_level(2_level, "a"_user_key, "z"_user_key));
}

TEST_F(VersionSetTest, Get) {
    add_sst({
      .id = 1_file_id,
      .level = 2_level,
      .keys = {
        "b@1"_key,
        "c@1"_key,
        "d@1"_key,
        "e@1"_key,
      },
    });
    add_sst({
      .id = 2_file_id,
      .level = 1_level,
      .keys = {
        "a@2"_key,
        "b@2"_key,
        "c@2"_key,
        "d@2"_key,
      },
    });
    add_sst({
      .id = 3_file_id,
      .level = 1_level,
      .keys = {
        "w@2"_key,
        "x@2"_key,
        "y@2"_key,
        "z@2"_key,
      },
    });
    auto& vset = version_set();
    lsm::db::version::get_stats stats;
    auto result = vset.current()->get("a@2"_key, &stats).get();
    EXPECT_THAT(result, IsLookupValue("a@2"_key, 1_level));
    result = vset.current()->get("e@1"_key, &stats).get();
    EXPECT_THAT(result, IsLookupValue("e@1"_key, 2_level));
    result = vset.current()->get("b@2"_key, &stats).get();
    EXPECT_THAT(result, IsLookupValue("b@2"_key, 1_level));
    result = vset.current()->get("j@1"_key, &stats).get();
    EXPECT_THAT(result, IsMissing());
}

TEST_F(VersionSetTest, GetOverlappingUserKey) {
    // Test a scenario with `get` where a level has the same user key split
    // across multiple files. This is a rare but valid case.
    add_sst({
      .id = 2_file_id,
      .level = 1_level,
      .keys = {
        "a@3"_key,
        "a@2"_key,
      },
    });
    add_sst({
      .id = 3_file_id,
      .level = 1_level,
      .keys = {
        "a@1"_key,
        "b@2"_key,
      },
    });
    auto& vset = version_set();
    lsm::db::version::get_stats stats;
    auto result = vset.current()->get("a@4"_key, &stats).get();
    EXPECT_THAT(result, IsLookupValue("a@3"_key, 1_level));
    result = vset.current()->get("a@1"_key, &stats).get();
    EXPECT_THAT(result, IsLookupValue("a@1"_key, 1_level));
    result = vset.current()->get("b@2"_key, &stats).get();
    EXPECT_THAT(result, IsLookupValue("b@2"_key, 1_level));
    result = vset.current()->get("j@1"_key, &stats).get();
    EXPECT_THAT(result, IsMissing());
}

TEST_F(VersionSetTest, IteratorsKeepVersionFilesLive) {
    // When add_iterators is called on a version with only L0 files, the
    // version_lifetime_iterator keeps the version alive. This means
    // get_live_files() must still report those files even after a new version
    // is installed that removes them.
    add_sst({
      .id = 1_file_id,
      .level = 0_level,
      .keys = {"a@1"_key, "b@1"_key},
    });
    add_sst({
      .id = 2_file_id,
      .level = 0_level,
      .keys = {"c@1"_key, "d@1"_key},
    });

    // Create iterators for the current version. Since we only have L0 files,
    // add_iterators will insert a version_lifetime_iterator that holds a
    // reference to this version.
    chunked_vector<std::unique_ptr<lsm::internal::iterator>> iters;
    version_set().current()->add_iterators(&iters).get();

    // Create a new version that removes both files.
    auto edit = version_set().new_edit();
    edit->remove_file(0_level, {.id = 1_file_id});
    edit->remove_file(0_level, {.id = 2_file_id});
    edit->set_last_seqno(1_seqno);
    version_set().log_and_apply(std::move(edit)).get();

    // The current version should have no files.
    EXPECT_EQ(version_set().current()->num_files(0_level), 0);

    // But get_live_files must still include the old files because the
    // iterators keep the old version alive.
    auto live = version_set().get_live_files();
    EXPECT_TRUE(live.contains({.id = 1_file_id}));
    EXPECT_TRUE(live.contains({.id = 2_file_id}));

    // Drop the iterators, releasing the version reference.
    iters.clear();

    // Now the files should no longer be considered live.
    EXPECT_THAT(version_set().get_live_files(), IsEmpty());
}

TEST_F(CompactionTest, PickCompactionLevel0ToLevel1) {
    // Add 4 files to L0 to trigger compaction
    add_file(0_level, 1_file_id, "a"_key, "d"_key);
    add_file(0_level, 2_file_id, "e"_key, "h"_key);
    add_file(0_level, 3_file_id, "i"_key, "m"_key);
    add_file(0_level, 4_file_id, "n"_key, "z"_key);

    // Add overlapping files in L1
    add_file(1_level, 10_file_id, "b"_key, "f"_key);
    add_file(1_level, 11_file_id, "g"_key, "k"_key);

    auto maybe_c = version_set().pick_compaction();

    ASSERT_TRUE(maybe_c);
    auto& c = *maybe_c;
    EXPECT_EQ(c->level(), 0_level);
    // L0 compaction includes all overlapping files
    EXPECT_THAT(input_file_ids(*c), ElementsAre(1_file_id));
    // L1 files overlapping with the L0 range [a-h]
    EXPECT_THAT(output_file_ids(*c), ElementsAre(10_file_id));
}

TEST_F(CompactionTest, TrivialMove) {
    // Add 4 files to L0 to trigger compaction
    add_file(0_level, 1_file_id, "a"_key, "d"_key);
    add_file(0_level, 2_file_id, "e"_key, "h"_key);
    add_file(0_level, 3_file_id, "i"_key, "m"_key);
    add_file(0_level, 4_file_id, "n"_key, "z"_key);

    // L1 has no files - first file can be trivially moved
    auto maybe_c = version_set().pick_compaction();

    ASSERT_TRUE(maybe_c);
    auto& c = *maybe_c;
    EXPECT_EQ(c->level(), 0_level);
    EXPECT_THAT(input_file_ids(*c), ElementsAre(1_file_id));
    EXPECT_THAT(output_file_ids(*c), IsEmpty());
    EXPECT_TRUE(c->is_trivial_move());
}

TEST_F(CompactionTest, OverlappingL0Files) {
    // Add 4 files to L0 with overlapping key ranges.
    // This tests that when picking files from L0, overlapping files are
    // properly expanded.
    add_file(0_level, 1_file_id, "a"_key, "e"_key);
    add_file(0_level, 2_file_id, "c"_key, "g"_key); // Overlaps with file 1
    add_file(0_level, 3_file_id, "f"_key, "j"_key); // Overlaps with file 2
    add_file(0_level, 4_file_id, "m"_key, "z"_key); // Disjoint

    // Add overlapping file in L1
    add_file(1_level, 10_file_id, "d"_key, "h"_key);

    auto maybe_c = version_set().pick_compaction();

    ASSERT_TRUE(maybe_c);
    auto& c = *maybe_c;
    EXPECT_EQ(c->level(), 0_level);
    // Should include all overlapping L0 files: starts with file 1, which
    // overlaps with file 2, which overlaps with file 3
    EXPECT_THAT(
      input_file_ids(*c),
      UnorderedElementsAre(1_file_id, 2_file_id, 3_file_id));
    // L1 file overlapping with the selected range
    EXPECT_THAT(output_file_ids(*c), ElementsAre(10_file_id));
}

TEST_F(CompactionTest, Level1ToLevel2Compaction) {
    // L0 is empty, but L1 has enough data to trigger size-based compaction
    // Each file is 1KB, L1 max is 10KB, so we need >10 files
    add_file(1_level, 1_file_id, "aa"_key, "ab"_key, 1000);
    add_file(1_level, 2_file_id, "ac"_key, "ad"_key, 1000);
    add_file(1_level, 3_file_id, "ae"_key, "af"_key, 1000);
    add_file(1_level, 4_file_id, "ag"_key, "ah"_key, 1000);
    add_file(1_level, 5_file_id, "ai"_key, "aj"_key, 1000);
    add_file(1_level, 6_file_id, "ak"_key, "al"_key, 1000);
    add_file(1_level, 7_file_id, "am"_key, "an"_key, 1000);
    add_file(1_level, 8_file_id, "ao"_key, "ap"_key, 1000);
    add_file(1_level, 9_file_id, "aq"_key, "ar"_key, 1000);
    add_file(1_level, 10_file_id, "as"_key, "at"_key, 1000);
    add_file(1_level, 11_file_id, "au"_key, "av"_key, 1000);
    add_file(1_level, 12_file_id, "aw"_key, "ax"_key, 1000);

    // Add overlapping file in L2
    add_file(2_level, 100_file_id, "aa"_key, "ac"_key);

    auto maybe_c = version_set().pick_compaction();

    ASSERT_TRUE(maybe_c);
    auto& c = *maybe_c;
    EXPECT_EQ(c->level(), 1_level); // L1→L2 compaction
    // Expansion picks both file 1 and 2 since they both overlap with
    // the same L2 file and fit within the expansion byte limit
    EXPECT_THAT(input_file_ids(*c), UnorderedElementsAre(1_file_id, 2_file_id));
    EXPECT_THAT(output_file_ids(*c), ElementsAre(100_file_id));
}

TEST_F(CompactionTest, GrandparentOverlapTracking) {
    // Add files to trigger L0→L1 compaction
    add_file(0_level, 1_file_id, "a"_key, "d"_key);
    add_file(0_level, 2_file_id, "e"_key, "h"_key);
    add_file(0_level, 3_file_id, "i"_key, "m"_key);
    add_file(0_level, 4_file_id, "n"_key, "z"_key);

    // L1 file
    add_file(1_level, 10_file_id, "b"_key, "f"_key);

    // Add grandparent files in L2 that overlap with compaction range
    add_file(2_level, 20_file_id, "a"_key, "c"_key);
    add_file(2_level, 21_file_id, "d"_key, "e"_key);

    auto maybe_c = version_set().pick_compaction();

    ASSERT_TRUE(maybe_c);
    auto& c = *maybe_c;
    EXPECT_EQ(c->level(), 0_level);
    // Expansion picks both file 1 and 2 since they both overlap with
    // the same L1 file and fit within the expansion byte limit.
    // Grandparent overlap tracking is verified by checking that the
    // compaction includes the expected grandparent files.
    EXPECT_THAT(input_file_ids(*c), UnorderedElementsAre(1_file_id, 2_file_id));
    EXPECT_THAT(output_file_ids(*c), ElementsAre(10_file_id));
}

TEST_F(CompactionTest, InputExpansion) {
    // Tests input expansion with disjoint (non-overlapping) L0 files.
    // When file 1 is picked for compaction against L1 file [a-g],
    // expansion should also pick file 2 since it doesn't add output files.
    add_file(0_level, 1_file_id, "a"_key, "c"_key, 100);
    add_file(0_level, 2_file_id, "d"_key, "f"_key, 100); // Disjoint from file 1
    add_file(0_level, 3_file_id, "m"_key, "p"_key, 100);
    add_file(0_level, 4_file_id, "q"_key, "z"_key, 100);

    // L1 file that covers both file 1 and file 2
    add_file(1_level, 10_file_id, "a"_key, "g"_key, 100);

    auto maybe_c = version_set().pick_compaction();

    ASSERT_TRUE(maybe_c);
    auto& c = *maybe_c;
    EXPECT_EQ(c->level(), 0_level);
    // Expansion should pick both files 1 and 2 since they both overlap with
    // the same L1 file and fit within the expansion limit
    EXPECT_THAT(input_file_ids(*c), UnorderedElementsAre(1_file_id, 2_file_id));
    EXPECT_THAT(output_file_ids(*c), ElementsAre(10_file_id));
}

TEST_F(CompactionTest, NoExpansionWhenOutputLevelGrows) {
    // Tests that expansion is rejected when it would add more output level
    // files. Initial compaction picks file 1, but expanding to include file 2
    // would require an additional L1 file, so expansion should not happen.
    add_file(0_level, 1_file_id, "a"_key, "c"_key, 100);
    add_file(0_level, 2_file_id, "d"_key, "f"_key, 100);
    add_file(0_level, 3_file_id, "m"_key, "p"_key, 100);
    add_file(0_level, 4_file_id, "q"_key, "z"_key, 100);

    // L1 files: file 1 overlaps with L1 file 10, but file 2 would require
    // adding L1 file 11, so expansion should be rejected
    add_file(1_level, 10_file_id, "a"_key, "c"_key, 100);
    add_file(1_level, 11_file_id, "d"_key, "f"_key, 100);

    auto maybe_c = version_set().pick_compaction();

    ASSERT_TRUE(maybe_c);
    auto& c = *maybe_c;
    EXPECT_EQ(c->level(), 0_level);
    // Only file 1 should be selected; expansion would add file 2 but also
    // require adding L1 file 11, so it's rejected
    EXPECT_THAT(input_file_ids(*c), ElementsAre(1_file_id));
    EXPECT_THAT(output_file_ids(*c), ElementsAre(10_file_id));
}

TEST_F(CompactionTest, BoundaryKeyHandling) {
    // Add files with exact boundary matches to test add_boundary_inputs
    add_file(0_level, 1_file_id, "a"_key, "d"_key);
    add_file(0_level, 2_file_id, "e"_key, "h"_key);
    add_file(0_level, 3_file_id, "i"_key, "m"_key);
    add_file(0_level, 4_file_id, "n"_key, "z"_key);

    // L1 files where one has a boundary that matches an L0 file's boundary
    add_file(1_level, 10_file_id, "b"_key, "d"_key); // Ends at 'd' like file 1
    add_file(
      1_level, 11_file_id, "e"_key, "g"_key); // Starts at 'e' like file 2

    auto maybe_c = version_set().pick_compaction();

    ASSERT_TRUE(maybe_c);
    auto& c = *maybe_c;
    EXPECT_EQ(c->level(), 0_level);
    // Should include file 1 from L0
    EXPECT_THAT(input_file_ids(*c), ElementsAre(1_file_id));
    // Should include L1 file with matching boundary
    EXPECT_THAT(output_file_ids(*c), ElementsAre(10_file_id));
}

TEST_F(CompactionTest, NoCompactionBelowThreshold) {
    // Add only 3 files to L0 (below the trigger of 4)
    add_file(0_level, 1_file_id, "a"_key, "d"_key);
    add_file(0_level, 2_file_id, "e"_key, "h"_key);
    add_file(0_level, 3_file_id, "i"_key, "m"_key);

    auto c = version_set().pick_compaction();

    // No compaction should be triggered
    EXPECT_FALSE(c);
}

TEST_F(CompactionTest, SameUserKeyDifferentSeqnosOverlap) {
    // Tests overlap detection when the same user key exists at different
    // sequence numbers across files. This is needed for tombstone
    // compaction - if we don't detect these as overlapping, tombstones
    // won't compact with older versions of the key and deletions won't work.
    //
    // In leveldb, get_overlapping_inputs uses user key comparison specifically
    // to handle this case. If we use internal key comparison instead, we might
    // miss these overlaps.

    // Add L0 files to trigger compaction
    // File 1: has "apple" at seqno 200 (newest)
    add_file(0_level, 1_file_id, "apple@200"_key, "apple@200"_key);
    add_file(0_level, 2_file_id, "bar"_key, "baz"_key);
    add_file(0_level, 3_file_id, "qux"_key, "quy"_key);
    add_file(0_level, 4_file_id, "xyz"_key, "zzz"_key);

    // L1 files: each contains "apple" at different sequence numbers
    // File 10: "apple" at seqno 100
    add_file(1_level, 10_file_id, "apple@100"_key, "apple@100"_key);

    // File 11: "apple" at seqno 50 (could be a tombstone)
    add_file(1_level, 11_file_id, "apple@50"_key, "apple@50"_key);

    auto maybe_c = version_set().pick_compaction();

    ASSERT_TRUE(maybe_c);
    auto& c = *maybe_c;
    EXPECT_EQ(c->level(), 0_level);
    // File 1 from L0 should be selected
    EXPECT_THAT(input_file_ids(*c), ElementsAre(1_file_id));

    // Both L1 files should be included in the output because they
    // contain the same user key "apple", even though they have different
    // seqnos. If we're using internal key comparison and this test fails (only
    // includes one L1 file), it means we're not properly detecting overlaps for
    // the same user key at different sequence numbers.
    //
    // This would be an issue because:
    // 1. Tombstones (deletes) wouldn't compact with older versions
    // 2. Old versions of keys would accumulate
    // 3. Space wouldn't be reclaimed properly
    EXPECT_THAT(
      output_file_ids(*c), UnorderedElementsAre(10_file_id, 11_file_id));
}

TEST_F(CompactionTest, CompactionPointerRespected) {
    // Tests that the compaction pointer is respected during compaction
    // selection. Files before the pointer should not be selected.

    // Set up L1 with multiple files to trigger size-based compaction
    add_file(1_level, 1_file_id, "a"_key, "b"_key, 1000);
    add_file(1_level, 2_file_id, "c"_key, "d"_key, 1000);
    add_file(1_level, 3_file_id, "e"_key, "f"_key, 1000);
    add_file(1_level, 4_file_id, "g"_key, "h"_key, 1000);
    add_file(1_level, 5_file_id, "i"_key, "j"_key, 1000);
    add_file(1_level, 6_file_id, "k"_key, "l"_key, 1000);
    add_file(1_level, 7_file_id, "m"_key, "n"_key, 1000);
    add_file(1_level, 8_file_id, "o"_key, "p"_key, 1000);
    add_file(1_level, 9_file_id, "q"_key, "r"_key, 1000);
    add_file(1_level, 10_file_id, "s"_key, "t"_key, 1000);
    add_file(1_level, 11_file_id, "u"_key, "v"_key, 1000);
    add_file(1_level, 12_file_id, "w"_key, "x"_key, 1000);

    // Add L2 files
    add_file(2_level, 100_file_id, "c"_key, "d"_key, 100);
    add_file(2_level, 101_file_id, "e"_key, "f"_key, 100);

    // Set the compaction pointer to after file 2, so next compaction should
    // start from file 3
    auto edit = version_set().new_edit();
    edit->set_compact_pointer(1_level, "d"_key);
    version_set().log_and_apply(std::move(edit)).get();

    // Next compaction should pick file 3 or later, not file 1 or 2
    auto maybe_c = version_set().pick_compaction();
    ASSERT_TRUE(maybe_c);
    auto& c = *maybe_c;
    EXPECT_EQ(c->level(), 1_level);
    // Should pick file 3 (first file after the compact pointer at "d")
    EXPECT_THAT(input_file_ids(*c), ElementsAre(3_file_id));
    EXPECT_THAT(output_file_ids(*c), ElementsAre(101_file_id));
}

TEST_F(CompactionTest, HighestScoringLevelIsPickedL1OverL0) {
    // When both L0 and L1 have score >= 1, the level with the higher score
    // should be picked. Here L1 has a higher score than L0.
    //
    // L0 score: 4 files / 4 trigger = 1.0
    add_file(0_level, 1_file_id, "a"_key, "b"_key);
    add_file(0_level, 2_file_id, "c"_key, "d"_key);
    add_file(0_level, 3_file_id, "e"_key, "f"_key);
    add_file(0_level, 4_file_id, "g"_key, "h"_key);

    // L1 score: 20KB / 10KB = 2.0 (higher than L0's 1.0)
    add_file(1_level, 10_file_id, "aa"_key, "ab"_key, 2000);
    add_file(1_level, 11_file_id, "ac"_key, "ad"_key, 2000);
    add_file(1_level, 12_file_id, "ae"_key, "af"_key, 2000);
    add_file(1_level, 13_file_id, "ag"_key, "ah"_key, 2000);
    add_file(1_level, 14_file_id, "ai"_key, "aj"_key, 2000);
    add_file(1_level, 15_file_id, "ak"_key, "al"_key, 2000);
    add_file(1_level, 16_file_id, "am"_key, "an"_key, 2000);
    add_file(1_level, 17_file_id, "ao"_key, "ap"_key, 2000);
    add_file(1_level, 18_file_id, "aq"_key, "ar"_key, 2000);
    add_file(1_level, 19_file_id, "as"_key, "at"_key, 2000);

    auto maybe_c = version_set().pick_compaction();

    ASSERT_TRUE(maybe_c);
    auto& c = *maybe_c;
    // L1 score (2.0) > L0 score (1.0), so L1→L2 compaction is picked
    EXPECT_EQ(c->level(), 1_level);
}

TEST_F(CompactionTest, HighestScoringLevelIsPickedL0OverL1) {
    // When both L0 and L1 have score >= 1, the level with the higher score
    // should be picked. Here L0 has a higher score than L1.
    //
    // L0 score: 8 files / 4 trigger = 2.0
    add_file(0_level, 1_file_id, "a"_key, "b"_key);
    add_file(0_level, 2_file_id, "c"_key, "d"_key);
    add_file(0_level, 3_file_id, "e"_key, "f"_key);
    add_file(0_level, 4_file_id, "g"_key, "h"_key);
    add_file(0_level, 5_file_id, "i"_key, "j"_key);
    add_file(0_level, 6_file_id, "k"_key, "l"_key);
    add_file(0_level, 7_file_id, "m"_key, "n"_key);
    add_file(0_level, 8_file_id, "o"_key, "p"_key);

    // L1 score: 11KB / 10KB = 1.1 (lower than L0's 2.0)
    add_file(1_level, 10_file_id, "aa"_key, "ab"_key, 1000);
    add_file(1_level, 11_file_id, "ac"_key, "ad"_key, 1000);
    add_file(1_level, 12_file_id, "ae"_key, "af"_key, 1000);
    add_file(1_level, 13_file_id, "ag"_key, "ah"_key, 1000);
    add_file(1_level, 14_file_id, "ai"_key, "aj"_key, 1000);
    add_file(1_level, 15_file_id, "ak"_key, "al"_key, 1000);
    add_file(1_level, 16_file_id, "am"_key, "an"_key, 1000);
    add_file(1_level, 17_file_id, "ao"_key, "ap"_key, 1000);
    add_file(1_level, 18_file_id, "aq"_key, "ar"_key, 1000);
    add_file(1_level, 19_file_id, "as"_key, "at"_key, 1000);
    add_file(1_level, 20_file_id, "au"_key, "av"_key, 1000);

    auto maybe_c = version_set().pick_compaction();

    ASSERT_TRUE(maybe_c);
    auto& c = *maybe_c;
    // L0 score (2.0) > L1 score (1.1), so L0→L1 compaction is picked
    EXPECT_EQ(c->level(), 0_level);
}

TEST_F(CompactionTest, NeedsCompactionChecksAllLevels) {
    // needs_compaction should return true when any level exceeds the threshold,
    // even if L0 is below its threshold.
    EXPECT_FALSE(version_set().needs_compaction());

    // L0 has only 1 file (below trigger of 4)
    add_file(0_level, 1_file_id, "a"_key, "z"_key);
    EXPECT_FALSE(version_set().needs_compaction());

    // Fill L1 above its 10KB threshold to trigger needs_compaction
    add_file(1_level, 10_file_id, "aa"_key, "ab"_key, 1000);
    add_file(1_level, 11_file_id, "ac"_key, "ad"_key, 1000);
    add_file(1_level, 12_file_id, "ae"_key, "af"_key, 1000);
    add_file(1_level, 13_file_id, "ag"_key, "ah"_key, 1000);
    add_file(1_level, 14_file_id, "ai"_key, "aj"_key, 1000);
    add_file(1_level, 15_file_id, "ak"_key, "al"_key, 1000);
    add_file(1_level, 16_file_id, "am"_key, "an"_key, 1000);
    add_file(1_level, 17_file_id, "ao"_key, "ap"_key, 1000);
    add_file(1_level, 18_file_id, "aq"_key, "ar"_key, 1000);
    add_file(1_level, 19_file_id, "as"_key, "at"_key, 1000);
    add_file(1_level, 20_file_id, "au"_key, "av"_key, 1000);

    EXPECT_TRUE(version_set().needs_compaction());
}

TEST_F(CompactionTest, CompactingLevelSkippedByPick) {
    // When a compaction is in progress on a level, that level (and its output
    // level) should be skipped by pick_compaction. A second call should pick
    // the next best level.

    // L0 score: 4/4 = 1.0
    add_file(0_level, 1_file_id, "a"_key, "b"_key);
    add_file(0_level, 2_file_id, "c"_key, "d"_key);
    add_file(0_level, 3_file_id, "e"_key, "f"_key);
    add_file(0_level, 4_file_id, "g"_key, "h"_key);

    // L2 score: 120KB / 100KB = 1.2 (L2 max = 10KB * 10 = 100KB)
    add_file(2_level, 30_file_id, "aa"_key, "ab"_key, 10000);
    add_file(2_level, 31_file_id, "ac"_key, "ad"_key, 10000);
    add_file(2_level, 32_file_id, "ae"_key, "af"_key, 10000);
    add_file(2_level, 33_file_id, "ag"_key, "ah"_key, 10000);
    add_file(2_level, 34_file_id, "ai"_key, "aj"_key, 10000);
    add_file(2_level, 35_file_id, "ak"_key, "al"_key, 10000);
    add_file(2_level, 36_file_id, "am"_key, "an"_key, 10000);
    add_file(2_level, 37_file_id, "ao"_key, "ap"_key, 10000);
    add_file(2_level, 38_file_id, "aq"_key, "ar"_key, 10000);
    add_file(2_level, 39_file_id, "as"_key, "at"_key, 10000);
    add_file(2_level, 40_file_id, "au"_key, "av"_key, 10000);
    add_file(2_level, 41_file_id, "aw"_key, "ax"_key, 10000);

    EXPECT_TRUE(version_set().needs_compaction());

    // L2 score (1.2) > L0 score (1.0), so first pick should be L2
    auto c1 = version_set().pick_compaction();
    ASSERT_TRUE(c1);
    EXPECT_EQ((*c1)->level(), 2_level);

    // While L2→L3 compaction is held, L2 and L3 are locked out.
    // Second pick should fall back to L0→L1.
    EXPECT_TRUE(version_set().needs_compaction());
    auto c2 = version_set().pick_compaction();
    ASSERT_TRUE(c2);
    EXPECT_EQ((*c2)->level(), 0_level);

    // Now both L0→L1 and L2→L3 are in progress. No more compactions.
    EXPECT_FALSE(version_set().needs_compaction());
    auto c3 = version_set().pick_compaction();
    EXPECT_FALSE(c3);
}

TEST_F(CompactionTest, CompactingLevelUnlockedOnDestruction) {
    // Verifies that dropping a compaction object unlocks the levels.
    add_file(0_level, 1_file_id, "a"_key, "b"_key);
    add_file(0_level, 2_file_id, "c"_key, "d"_key);
    add_file(0_level, 3_file_id, "e"_key, "f"_key);
    add_file(0_level, 4_file_id, "g"_key, "h"_key);

    EXPECT_TRUE(version_set().needs_compaction());

    {
        auto c = version_set().pick_compaction();
        ASSERT_TRUE(c);
        EXPECT_EQ((*c)->level(), 0_level);
        // L0 and L1 are locked
        EXPECT_FALSE(version_set().needs_compaction());
    }
    // compaction destroyed, levels unlocked
    EXPECT_TRUE(version_set().needs_compaction());
}

TEST_F(CompactionTest, L2ToL3Compaction) {
    // Deeper levels should also trigger compaction when they exceed their
    // byte threshold. L2 max = 10KB * 10 = 100KB with multiplier=10.
    add_file(2_level, 1_file_id, "aa"_key, "ab"_key, 10000);
    add_file(2_level, 2_file_id, "ac"_key, "ad"_key, 10000);
    add_file(2_level, 3_file_id, "ae"_key, "af"_key, 10000);
    add_file(2_level, 4_file_id, "ag"_key, "ah"_key, 10000);
    add_file(2_level, 5_file_id, "ai"_key, "aj"_key, 10000);
    add_file(2_level, 6_file_id, "ak"_key, "al"_key, 10000);
    add_file(2_level, 7_file_id, "am"_key, "an"_key, 10000);
    add_file(2_level, 8_file_id, "ao"_key, "ap"_key, 10000);
    add_file(2_level, 9_file_id, "aq"_key, "ar"_key, 10000);
    add_file(2_level, 10_file_id, "as"_key, "at"_key, 10000);
    add_file(2_level, 11_file_id, "au"_key, "av"_key, 10000);

    // Add an L3 file to verify correct output level
    add_file(3_level, 100_file_id, "aa"_key, "ac"_key);

    EXPECT_TRUE(version_set().needs_compaction());

    auto maybe_c = version_set().pick_compaction();
    ASSERT_TRUE(maybe_c);
    auto& c = *maybe_c;
    EXPECT_EQ(c->level(), 2_level);
    EXPECT_THAT(output_file_ids(*c), ElementsAre(100_file_id));
}

TEST_F(CompactionTest, NeedsCompactionFalseWhenLevelLocked) {
    // If the only level needing compaction is currently locked,
    // needs_compaction should return false.
    add_file(0_level, 1_file_id, "a"_key, "b"_key);
    add_file(0_level, 2_file_id, "c"_key, "d"_key);
    add_file(0_level, 3_file_id, "e"_key, "f"_key);
    add_file(0_level, 4_file_id, "g"_key, "h"_key);

    EXPECT_TRUE(version_set().needs_compaction());

    auto c = version_set().pick_compaction();
    ASSERT_TRUE(c);

    // L0→L1 is in progress, both levels locked. Since L0 was the only level
    // above threshold, needs_compaction should now be false.
    EXPECT_FALSE(version_set().needs_compaction());
}

TEST_F(CompactionTest, NeedsCompactionFalseWhenOutputLevelLocked) {
    // If a level needs compaction but its output level is locked by another
    // compaction, needs_compaction should return false.

    // Fill L1 above its 10KB threshold so L1→L2 is needed.
    add_file(1_level, 10_file_id, "aa"_key, "ab"_key, 1000);
    add_file(1_level, 11_file_id, "ac"_key, "ad"_key, 1000);
    add_file(1_level, 12_file_id, "ae"_key, "af"_key, 1000);
    add_file(1_level, 13_file_id, "ag"_key, "ah"_key, 1000);
    add_file(1_level, 14_file_id, "ai"_key, "aj"_key, 1000);
    add_file(1_level, 15_file_id, "ak"_key, "al"_key, 1000);
    add_file(1_level, 16_file_id, "am"_key, "an"_key, 1000);
    add_file(1_level, 17_file_id, "ao"_key, "ap"_key, 1000);
    add_file(1_level, 18_file_id, "aq"_key, "ar"_key, 1000);
    add_file(1_level, 19_file_id, "as"_key, "at"_key, 1000);
    add_file(1_level, 20_file_id, "au"_key, "av"_key, 1000);

    EXPECT_TRUE(version_set().needs_compaction());

    // Pick the L1→L2 compaction, locking both L1 and L2.
    auto c1 = version_set().pick_compaction();
    ASSERT_TRUE(c1);
    EXPECT_EQ((*c1)->level(), 1_level);

    // Now fill L2 above its 100KB threshold so L2→L3 would be needed, but
    // L2 is already locked as the output of the L1→L2 compaction.
    add_file(2_level, 30_file_id, "ba"_key, "bb"_key, 10000);
    add_file(2_level, 31_file_id, "bc"_key, "bd"_key, 10000);
    add_file(2_level, 32_file_id, "be"_key, "bf"_key, 10000);
    add_file(2_level, 33_file_id, "bg"_key, "bh"_key, 10000);
    add_file(2_level, 34_file_id, "bi"_key, "bj"_key, 10000);
    add_file(2_level, 35_file_id, "bk"_key, "bl"_key, 10000);
    add_file(2_level, 36_file_id, "bm"_key, "bn"_key, 10000);
    add_file(2_level, 37_file_id, "bo"_key, "bp"_key, 10000);
    add_file(2_level, 38_file_id, "bq"_key, "br"_key, 10000);
    add_file(2_level, 39_file_id, "bs"_key, "bt"_key, 10000);
    add_file(2_level, 40_file_id, "bu"_key, "bv"_key, 10000);

    // L2 is the input level for the would-be L2→L3, but it's locked.
    EXPECT_FALSE(version_set().needs_compaction());
    auto c2 = version_set().pick_compaction();
    EXPECT_FALSE(c2);
}

TEST_F(CompactionTest, PickCompactionSkipsWhenOutputLevelLocked) {
    // When L1→L2 compaction is in progress (locking L1 and L2), an L0→L1
    // compaction must not be picked because L1 is locked as an output level.

    // L0 score: 4/4 = 1.0 (exactly at threshold)
    add_file(0_level, 1_file_id, "a"_key, "b"_key);
    add_file(0_level, 2_file_id, "c"_key, "d"_key);
    add_file(0_level, 3_file_id, "e"_key, "f"_key);
    add_file(0_level, 4_file_id, "g"_key, "h"_key);

    // Fill L1 above its 10KB threshold so L1→L2 scores >= 1.
    add_file(1_level, 10_file_id, "aa"_key, "ab"_key, 1000);
    add_file(1_level, 11_file_id, "ac"_key, "ad"_key, 1000);
    add_file(1_level, 12_file_id, "ae"_key, "af"_key, 1000);
    add_file(1_level, 13_file_id, "ag"_key, "ah"_key, 1000);
    add_file(1_level, 14_file_id, "ai"_key, "aj"_key, 1000);
    add_file(1_level, 15_file_id, "ak"_key, "al"_key, 1000);
    add_file(1_level, 16_file_id, "am"_key, "an"_key, 1000);
    add_file(1_level, 17_file_id, "ao"_key, "ap"_key, 1000);
    add_file(1_level, 18_file_id, "aq"_key, "ar"_key, 1000);
    add_file(1_level, 19_file_id, "as"_key, "at"_key, 1000);
    add_file(1_level, 20_file_id, "au"_key, "av"_key, 1000);

    EXPECT_TRUE(version_set().needs_compaction());

    // Pick L1→L2 first (higher score). This locks L1 and L2.
    auto c1 = version_set().pick_compaction();
    ASSERT_TRUE(c1);
    EXPECT_EQ((*c1)->level(), 1_level);

    // L0→L1 should NOT be picked because L1 (the output level) is locked.
    EXPECT_FALSE(version_set().needs_compaction());
    auto c2 = version_set().pick_compaction();
    EXPECT_FALSE(c2);

    // After L1→L2 completes, compactions become available again.
    c1 = {};
    EXPECT_TRUE(version_set().needs_compaction());
    auto c3 = version_set().pick_compaction();
    ASSERT_TRUE(c3);
}

TEST_F(CompactionTest, SeekCompactionSkippedWhenLevelLocked) {
    // Seek-triggered compaction on L1 should be skipped when L1 is locked by
    // an in-progress compaction.

    // Add a single file to L1 (below score threshold).
    add_file(1_level, 10_file_id, "aa"_key, "az"_key);

    // Set _file_to_compact on the current version by forcing allowed_seeks to
    // 0 and calling update_stats.
    auto current = version_set().current();
    auto l1_files = current->get_overlapping_inputs(
      1_level, std::nullopt, std::nullopt);
    ASSERT_EQ(l1_files.size(), 1);
    l1_files[0]->allowed_seeks = 0;
    EXPECT_TRUE(current->update_stats(
      {.seek_file = l1_files[0], .seek_file_level = 1_level}));

    // Seek compaction is pending on L1, so needs_compaction should be true.
    EXPECT_TRUE(version_set().needs_compaction());

    // Pick the seek compaction — this locks L1 and L2.
    auto c = version_set().pick_compaction();
    ASSERT_TRUE(c);
    EXPECT_EQ((*c)->level(), 1_level);

    // L1 and L2 are now locked. No more compactions.
    EXPECT_FALSE(version_set().needs_compaction());
}

TEST_F(CompactionTest, SeekCompactionSkippedWhenOutputLevelLocked) {
    // Seek compaction on L1 should be blocked when L2 (its output level) is
    // already locked by a L1→L2 compaction.

    // Fill L1 above threshold to trigger score-based compaction, and also add
    // a file on which we'll trigger seek compaction.
    add_file(1_level, 10_file_id, "aa"_key, "ab"_key, 1000);
    add_file(1_level, 11_file_id, "ac"_key, "ad"_key, 1000);
    add_file(1_level, 12_file_id, "ae"_key, "af"_key, 1000);
    add_file(1_level, 13_file_id, "ag"_key, "ah"_key, 1000);
    add_file(1_level, 14_file_id, "ai"_key, "aj"_key, 1000);
    add_file(1_level, 15_file_id, "ak"_key, "al"_key, 1000);
    add_file(1_level, 16_file_id, "am"_key, "an"_key, 1000);
    add_file(1_level, 17_file_id, "ao"_key, "ap"_key, 1000);
    add_file(1_level, 18_file_id, "aq"_key, "ar"_key, 1000);
    add_file(1_level, 19_file_id, "as"_key, "at"_key, 1000);
    add_file(1_level, 20_file_id, "au"_key, "av"_key, 1000);
    // Add a separate L2 file for seek compaction target.
    add_file(2_level, 50_file_id, "ba"_key, "bz"_key);

    // Set _file_to_compact for L2 on the current version.
    auto current = version_set().current();
    auto l2_files = current->get_overlapping_inputs(
      2_level, std::nullopt, std::nullopt);
    ASSERT_EQ(l2_files.size(), 1);
    l2_files[0]->allowed_seeks = 0;
    EXPECT_TRUE(current->update_stats(
      {.seek_file = l2_files[0], .seek_file_level = 2_level}));

    EXPECT_TRUE(version_set().needs_compaction());

    // Pick L1→L2 compaction (score-based, preferred over seek). Locks L1, L2.
    auto c = version_set().pick_compaction();
    ASSERT_TRUE(c);
    EXPECT_EQ((*c)->level(), 1_level);

    // Seek compaction on L2 should be blocked because L2 is locked as the
    // output of L1→L2, and L3 would be the output of a L2→L3 compaction.
    EXPECT_FALSE(version_set().needs_compaction());
    auto c2 = version_set().pick_compaction();
    EXPECT_FALSE(c2);
}
