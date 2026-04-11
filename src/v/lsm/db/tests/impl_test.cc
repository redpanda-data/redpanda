// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "gtest/gtest.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/impl.h"
#include "lsm/io/memory_persistence.h"
#include "random/generators.h"
#include "ssx/clock.h"
#include "test_utils/async.h"

#include <gtest/gtest.h>

#include <memory>

namespace {

namespace io = lsm::io;
using lsm::internal::file_handle;
using lsm::internal::operator""_seqno;
using lsm::internal::operator""_key;

class proxy_data_persistence : public io::data_persistence {
public:
    explicit proxy_data_persistence(io::data_persistence* p)
      : _p(p) {}

    ss::future<io::optional_pointer<io::random_access_file_reader>>
    open_random_access_reader(file_handle h) override {
        return _p->open_random_access_reader(h);
    }

    ss::future<std::unique_ptr<io::sequential_file_writer>>
    open_sequential_writer(file_handle h) override {
        return _p->open_sequential_writer(h);
    }

    ss::future<> remove_file(file_handle h) override {
        return _p->remove_file(h);
    }

    ss::coroutine::experimental::generator<file_handle> list_files() override {
        return _p->list_files();
    }

    ss::future<> close() override { co_return; };

private:
    io::data_persistence* _p;
};

class proxy_metadata_persistence : public io::metadata_persistence {
public:
    explicit proxy_metadata_persistence(io::metadata_persistence* mdp)
      : _mdp(mdp) {}

    ss::future<std::optional<iobuf>>
    read_manifest(lsm::internal::database_epoch e) override {
        return _mdp->read_manifest(e);
    }

    ss::future<>
    write_manifest(lsm::internal::database_epoch e, iobuf b) override {
        return _mdp->write_manifest(e, std::move(b));
    }

    ss::future<> close() override { co_return; };

private:
    io::metadata_persistence* _mdp;
};

// Wrapper that tracks all open_random_access_reader calls
class tracking_data_persistence : public io::data_persistence {
public:
    explicit tracking_data_persistence(io::data_persistence* underlying)
      : _underlying(underlying) {}

    ss::future<io::optional_pointer<io::random_access_file_reader>>
    open_random_access_reader(file_handle h) override {
        _opened_files.insert(h);
        return _underlying->open_random_access_reader(h);
    }

    ss::future<std::unique_ptr<io::sequential_file_writer>>
    open_sequential_writer(file_handle h) override {
        return _underlying->open_sequential_writer(h);
    }

    ss::future<> remove_file(file_handle h) override {
        return _underlying->remove_file(h);
    }

    ss::coroutine::experimental::generator<file_handle> list_files() override {
        return _underlying->list_files();
    }

    ss::future<> close() override { return _underlying->close(); }

    const std::set<file_handle>& opened_files() const { return _opened_files; }

    void reset_tracking() { _opened_files.clear(); }

private:
    io::data_persistence* _underlying;
    std::set<file_handle> _opened_files;
};

class ImplTest : public testing::Test {
public:
    struct shadow_entry {
        iobuf value;
        lsm::internal::sequence_number seqno;
    };
    using shadow_map = std::map<ss::sstring, shadow_entry>;

    void SetUp() override {
        // Make a smaller sized database so we get some actual leveling
        // happening.
        _options = make_options();
        _underlying_data_persistence = lsm::io::make_memory_data_persistence();
        _meta_persistence = lsm::io::make_memory_metadata_persistence(
          &_meta_persistence_controller);
        _tracking_data = std::make_unique<tracking_data_persistence>(
          _underlying_data_persistence.get());
        open();
    }

    ss::lw_shared_ptr<lsm::internal::options> make_options() {
        return ss::make_lw_shared<lsm::internal::options>({
          .levels = lsm::internal::options::make_levels(
            {.max_total_bytes = 1_MiB, .max_file_size = 256_KiB},
            /*multiplier=*/2,
            lsm::internal::options::default_max_level),
          .level_zero_slowdown_writes_trigger = 4,
          .level_zero_stop_writes_trigger = 6,
          .write_buffer_size = 256_KiB,
          .level_one_compaction_trigger = 2,
        });
    }

    void TearDown() override {
        _db->close().get();
        for (auto& db : _other_dbs) {
            db->close().get();
        }
        _underlying_data_persistence->close().get();
        _meta_persistence->close().get();
        _shadow.clear();
    }

    void write_at_least(size_t size, lsm::db::impl* db = nullptr) {
        if (!db) {
            db = _db.get();
        }
        auto batch = ss::make_lw_shared<lsm::db::memtable>();
        decltype(_shadow) shadow_batch;
        auto seqno = _db->max_applied_seqno().value_or(0_seqno);
        while (batch->approximate_memory_usage() < size) {
            auto key = lsm::internal::key::encode({
              .key = lsm::user_key_view(
                random_generators::gen_alphanum_max_distinct(100'000)),
              .seqno = ++seqno,
            });
            auto value = iobuf::from(
              random_generators::gen_alphanum_string(1_KiB));
            shadow_batch.insert_or_assign(
              ss::sstring(key.user_key()),
              shadow_entry(value.share(), key.seqno()));
            batch->put(key, value.share());
        }
        db->apply(std::move(batch)).get();
        // Only apply writes if db write was a success
        for (auto& [k, v] : shadow_batch) {
            _shadow.insert_or_assign(k, std::move(v));
        }
    }

    void delete_random_keys(lsm::db::impl* db = nullptr) {
        if (_shadow.empty()) {
            return;
        }
        if (!db) {
            db = _db.get();
        }
        auto batch = ss::make_lw_shared<lsm::db::memtable>();
        auto seqno = db->max_applied_seqno().value_or(0_seqno);
        std::vector<ss::sstring> keys_to_delete;
        for (auto& [k, _] : _shadow) {
            if (random_generators::get_int(0, 3) == 0) {
                auto key = lsm::internal::key::encode({
                  .key = lsm::user_key_view(k),
                  .seqno = ++seqno,
                  .type = lsm::internal::value_type::tombstone,
                });
                batch->remove(std::move(key));
                keys_to_delete.push_back(k);
            }
        }
        if (keys_to_delete.empty()) {
            return;
        }
        db->apply(std::move(batch)).get();
        for (auto& k : keys_to_delete) {
            _shadow.erase(k);
        }
    }

    testing::AssertionResult matches_shadow(lsm::db::impl* db = nullptr) {
        return matches_shadow(_shadow, nullptr, db);
    }

    shadow_map clone_shadow_map() {
        shadow_map s;
        for (auto& [k, e] : _shadow) {
            s.emplace(k, shadow_entry(e.value.share(), e.seqno));
        }
        return s;
    }

    testing::AssertionResult matches_shadow(
      const shadow_map& shadow,
      lsm::db::snapshot* snapshot,
      lsm::db::impl* db = nullptr) {
        if (!db) {
            db = _db.get();
        }
        auto iter = db->create_iterator({.snapshot = snapshot}).get();
        auto it = shadow.begin();
        std::vector<std::string> errors;
        for (iter->seek_to_first().get(); iter->valid(); iter->next().get()) {
            if (it == shadow.end()) {
                errors.emplace_back("extra elements");
                break;
            }
            bool key_eq = it->first == iter->key().user_key();
            bool val_eq = it->second.value == iter->value();
            bool seqno_eq = it->second.seqno == iter->key().seqno();
            if (!key_eq || !val_eq || !seqno_eq) {
                errors.push_back(
                  fmt::format(
                    "expected key {}, got key {}, keys equal {}, values equal "
                    "{}, expected seqno {}, got seqno {}",
                    it->first,
                    iter->key(),
                    it->first == iter->key().user_key(),
                    it->second.value == iter->value(),
                    it->second.seqno,
                    iter->key().seqno()));
            }
            ++it;
        }
        if (it != shadow.end()) {
            errors.emplace_back("missing elements");
        }
        if (errors.empty()) {
            return testing::AssertionSuccess();
        }
        return testing::AssertionFailure()
               << fmt::format("{}", fmt::join(errors, "\n"));
    }

    void restart() {
        close();
        open();
    }
    void close() {
        _db->close().get();
        _tracking_data->reset_tracking();
    }
    void open() { _db = do_open(_options); }

    lsm::db::impl* open(ss::lw_shared_ptr<lsm::internal::options> opts) {
        _other_dbs.emplace_back(do_open(std::move(opts)));
        return _other_dbs.back().get();
    }

    std::unique_ptr<lsm::db::impl>
    do_open(ss::lw_shared_ptr<lsm::internal::options> opts) {
        return lsm::db::impl::open(
                 opts,
                 {
                   .data = std::make_unique<proxy_data_persistence>(
                     _tracking_data.get()),
                   .metadata = std::make_unique<proxy_metadata_persistence>(
                     _meta_persistence.get()),
                 })
          .get();
    }

    auto max_applied_seqno() { return _db->max_applied_seqno(); }
    auto max_persisted_seqno() { return _db->max_persisted_seqno(); }

    ss::future<std::vector<file_handle>> list_files() {
        auto gen = _underlying_data_persistence->list_files();
        std::vector<file_handle> files;
        while (auto file = co_await gen()) {
            files.push_back(*file);
        }
        co_return files;
    }

    const std::set<file_handle>& opened_files() const {
        return _tracking_data->opened_files();
    }

protected:
    shadow_map _shadow;
    ss::lw_shared_ptr<lsm::internal::options> _options;
    std::unique_ptr<lsm::io::data_persistence> _underlying_data_persistence;
    lsm::io::memory_persistence_controller _meta_persistence_controller;
    std::unique_ptr<lsm::io::metadata_persistence> _meta_persistence;
    std::unique_ptr<tracking_data_persistence> _tracking_data;
    std::unique_ptr<lsm::db::impl> _db;
    std::vector<std::unique_ptr<lsm::db::impl>> _other_dbs;
};

TEST_F(ImplTest, MemtableIsFlushed) {
    EXPECT_TRUE(matches_shadow());
    write_at_least(256_KiB);
    EXPECT_TRUE(matches_shadow());
    write_at_least(256_KiB);
    EXPECT_TRUE(matches_shadow());
    write_at_least(256_KiB);
    EXPECT_TRUE(matches_shadow());
    write_at_least(256_KiB);
    EXPECT_TRUE(matches_shadow());
    RPTEST_REQUIRE_EVENTUALLY(10s, [this] {
        return list_files().then(
          [](const auto& files) { return files.size() > 0; });
    });
}

TEST_F(ImplTest, Recovery) {
    write_at_least(128_KiB);
    write_at_least(128_KiB);
    EXPECT_TRUE(matches_shadow());
    tests::drain_task_queue().get();
    _db->flush().get();
    EXPECT_EQ(max_applied_seqno(), max_persisted_seqno());
    restart();
    EXPECT_TRUE(matches_shadow());
}

TEST_F(ImplTest, FlushFailure) {
    write_at_least(128_KiB);
    EXPECT_TRUE(matches_shadow());
    tests::drain_task_queue().get();
    _meta_persistence_controller.should_fail = true;
    EXPECT_ANY_THROW(
      _db
        ->flush(
          ssx::lowres_steady_clock().now() + ssx::duration::milliseconds(100))
        .get());
    EXPECT_FALSE(max_persisted_seqno().has_value());
}

TEST_F(ImplTest, Randomized) {
#ifndef NDEBUG
    int rounds = 100;
#else
    int rounds = 1000;
#endif
    for (int i = 0; i < rounds; ++i) {
        write_at_least(128_KiB);
        EXPECT_TRUE(matches_shadow());
    }
}

TEST_F(ImplTest, RandomizedWithDeletes) {
#ifndef NDEBUG
    int rounds = 100;
#else
    int rounds = 1000;
#endif
    for (int i = 0; i < rounds; ++i) {
        write_at_least(128_KiB);
        EXPECT_TRUE(matches_shadow());
        delete_random_keys();
        EXPECT_TRUE(matches_shadow());
    }
}

TEST_F(ImplTest, ReadYourOwnWrites) {
    // Write some initial data to the database
    auto batch1 = ss::make_lw_shared<lsm::db::memtable>();
    auto seqno = _db->max_applied_seqno().value_or(0_seqno);
    auto key1 = lsm::internal::key::encode({
      .key = lsm::user_key_view("key1"),
      .seqno = ++seqno,
    });
    auto value1 = iobuf::from("value1");
    batch1->put(key1, value1.share());

    auto key2 = lsm::internal::key::encode({
      .key = lsm::user_key_view("key2"),
      .seqno = ++seqno,
    });
    auto value2 = iobuf::from("value2");
    batch1->put(key2, value2.share());

    _db->apply(batch1).get();

    // Create a pending write batch with new keys (not yet applied)
    auto pending_batch = ss::make_lw_shared<lsm::db::memtable>();
    seqno = _db->max_applied_seqno().value_or(0_seqno);

    auto key3 = lsm::internal::key::encode({
      .key = lsm::user_key_view("key3"),
      .seqno = ++seqno,
    });
    auto value3 = iobuf::from("value3");
    pending_batch->put(key3, value3.share());

    // Verify new key is visible through iterator with write batch
    auto iter = _db->create_iterator({.memtable = pending_batch}).get();
    std::map<ss::sstring, iobuf> seen;
    for (iter->seek_to_first().get(); iter->valid(); iter->next().get()) {
        seen.insert_or_assign(
          ss::sstring(iter->key().user_key()), iter->value());
    }
    EXPECT_EQ(seen.size(), 3);
    EXPECT_EQ(seen["key1"], value1);
    EXPECT_EQ(seen["key2"], value2);
    EXPECT_EQ(seen["key3"], value3);

    // Create another pending write batch that updates an existing key
    auto update_batch = ss::make_lw_shared<lsm::db::memtable>();
    seqno = _db->max_applied_seqno().value_or(0_seqno);

    auto key2_updated = lsm::internal::key::encode({
      .key = lsm::user_key_view("key2"),
      .seqno = ++seqno,
    });
    auto value2_updated = iobuf::from("value2_updated");
    update_batch->put(key2_updated, value2_updated.share());

    // Verify updated value is visible through iterator with write batch
    auto iter2 = _db->create_iterator({.memtable = update_batch}).get();
    seen.clear();
    for (iter2->seek_to_first().get(); iter2->valid(); iter2->next().get()) {
        seen.insert_or_assign(
          ss::sstring(iter2->key().user_key()), iter2->value());
    }
    EXPECT_EQ(seen.size(), 2);
    EXPECT_EQ(seen["key1"], value1);
    EXPECT_EQ(seen["key2"], value2_updated);

    // Apply the update batch and verify all data is now committed
    _db->apply(update_batch).get();

    auto iter3 = _db->create_iterator({}).get();
    seen.clear();
    for (iter3->seek_to_first().get(); iter3->valid(); iter3->next().get()) {
        seen.insert_or_assign(
          ss::sstring(iter3->key().user_key()), iter3->value());
    }
    EXPECT_EQ(seen.size(), 2);
    EXPECT_EQ(seen["key1"], value1);
    EXPECT_EQ(seen["key2"], value2_updated);
}

TEST_F(ImplTest, PreOpenFiles) {
    // Write some data and flush to create SST files
    write_at_least(512_KiB);
    write_at_least(512_KiB);
    EXPECT_TRUE(matches_shadow());
    tests::drain_task_queue().get();
    _db->flush().get();

    // Get the list of files
    auto files = list_files().get();
    EXPECT_GT(files.size(), 0);

    // Restart with max_pre_open_fibers = 0 (disabled)
    _options->max_pre_open_fibers = 0;
    restart();

    EXPECT_EQ(opened_files().size(), 0);
    EXPECT_TRUE(matches_shadow());

    // Restart with max_pre_open_fibers > 0 (enabled)
    _options->max_pre_open_fibers = 4;
    restart();

    EXPECT_EQ(opened_files().size(), files.size());
    for (const auto& file : files) {
        EXPECT_TRUE(opened_files().contains(file))
          << "File " << file << " was not pre-opened";
    }
    EXPECT_TRUE(matches_shadow());
}

TEST_F(ImplTest, Readonly) {
    // Write some data and flush to create SST files
    write_at_least(512_KiB);
    write_at_least(512_KiB);
    EXPECT_TRUE(matches_shadow());
    tests::drain_task_queue().get();
    _db->flush().get();

    // Restart in readonly mode
    close();
    _options->readonly = true;
    open();

    EXPECT_TRUE(matches_shadow());
    EXPECT_ANY_THROW(write_at_least(1_KiB));
    EXPECT_TRUE(matches_shadow());
    EXPECT_ANY_THROW(write_at_least(1_KiB));
}

TEST_F(ImplTest, ImplicitSnapshotEmpty) {
    auto it = _db->create_iterator({}).get();
    it->seek_to_first().get();
    EXPECT_FALSE(it->valid());
    write_at_least(512_KiB);
    EXPECT_TRUE(matches_shadow());
    // Even though there is now data, the DB should be empty
    it->seek_to_first().get();
    EXPECT_FALSE(it->valid());
    it->seek_to_last().get();
    EXPECT_FALSE(it->valid());
}

TEST_F(ImplTest, ExplicitSnapshotEmpty) {
    EXPECT_FALSE(_db->create_snapshot());
}

TEST_F(ImplTest, ExplicitSnapshot) {
    write_at_least(512_KiB);
    auto snap = _db->create_snapshot();
    EXPECT_TRUE(snap);
    auto shadow = clone_shadow_map();
    EXPECT_TRUE(matches_shadow(shadow, snap->get()));
    write_at_least(512_KiB);
    EXPECT_TRUE(matches_shadow(shadow, snap->get()));
    EXPECT_FALSE(matches_shadow(shadow, nullptr));
    _db->flush().get();
    EXPECT_TRUE(matches_shadow(shadow, snap->get()));
    write_at_least(512_KiB);
    write_at_least(512_KiB);
    tests::drain_task_queue().get();
    _db->flush().get();
    // Wait for any compaction to finish
    tests::drain_task_queue().get();
    EXPECT_TRUE(matches_shadow(shadow, snap->get()));
    EXPECT_FALSE(matches_shadow(shadow, nullptr));
    EXPECT_TRUE(matches_shadow());
}

// Regression test for a bug where we previously wouldn't filter on file bounds
// properly, which would previously show up as missing rows in snapshot::get().
TEST_F(ImplTest, GetFindsKeysWithDifferentSeqno) {
    // Write row "aaa" at seqno 1 and flush it so it ends up in a file.
    {
        auto batch = ss::make_lw_shared<lsm::db::memtable>();
        batch->put("aaa@1"_key, iobuf::from("value_for_aaa"));
        _db->apply(std::move(batch)).get();
    }
    _db->flush().get();
    ASSERT_EQ(max_persisted_seqno(), 1_seqno);

    {
        auto batch = ss::make_lw_shared<lsm::db::memtable>();
        batch->put("zzz@2"_key, iobuf::from("value_for_zzz"));
        _db->apply(std::move(batch)).get();
    }
    ASSERT_EQ(max_applied_seqno(), 2_seqno);

    // Create snapshot with seqno 2.
    auto snap_opt = _db->create_snapshot();
    ASSERT_TRUE(snap_opt);
    auto snap = std::move(*snap_opt);
    ASSERT_EQ(snap->seqno(), 2_seqno);

    // Sanity check that iteration finds "aaa".
    auto iter = _db->create_iterator({.snapshot = snap.get()}).get();
    iter->seek_to_first().get();
    ASSERT_TRUE(iter->valid());
    EXPECT_EQ(iter->key(), "aaa@1"_key);

    // Lookup the "aaa" at the snapshot's seqno, as is done in snapshot::get().
    // This would previously skip the file containing "aaa" at seqno 1.
    auto result = _db->get("aaa@2"_key).get();
    EXPECT_FALSE(result.is_missing());
}

TEST_F(ImplTest, RefreshOnWritableDbThrows) {
    EXPECT_THROW(_db->refresh().get(), lsm::invalid_argument_exception);
}

TEST_F(ImplTest, RefreshEmpty) {
    auto read_opts = make_options();
    read_opts->readonly = true;

    // Sanity check that the read-only database sees nothing.
    auto* read_db = open(read_opts);
    EXPECT_TRUE(matches_shadow(read_db));

    // Refresh shouldn't have issues with the persistence being empty.
    read_db->refresh().get();
    EXPECT_TRUE(matches_shadow(read_db));
    EXPECT_FALSE(read_db->max_applied_seqno().has_value());
}

TEST_F(ImplTest, RefreshNoOp) {
    write_at_least(512_KiB);
    _db->flush().get();

    auto read_opts = make_options();
    read_opts->readonly = true;
    auto* read_db = open(read_opts);
    EXPECT_TRUE(matches_shadow(read_db));

    auto max_persisted = _db->max_persisted_seqno();
    write_at_least(512_KiB);

    // Without flushing, refreshing should be a no-op.
    EXPECT_FALSE(read_db->refresh().get());
    EXPECT_LT(read_db->max_applied_seqno(), _db->max_applied_seqno());
    EXPECT_EQ(read_db->max_persisted_seqno(), max_persisted);
}

TEST_F(ImplTest, RefreshSeesChanges) {
    write_at_least(512_KiB);
    _db->flush().get();
    write_at_least(512_KiB);
    _db->flush().get();

    auto read_opts = make_options();
    read_opts->readonly = true;
    auto* read_db = open(read_opts);
    EXPECT_TRUE(matches_shadow(read_db));
    EXPECT_EQ(read_db->max_applied_seqno(), _db->max_applied_seqno());
    EXPECT_EQ(read_db->max_persisted_seqno(), _db->max_persisted_seqno());

    // Write new data.
    write_at_least(512_KiB);
    EXPECT_FALSE(matches_shadow(read_db));

    // Even when flushing the data shouldn't be visible.
    _db->flush().get();
    EXPECT_FALSE(matches_shadow(read_db));
    EXPECT_NE(read_db->max_applied_seqno(), _db->max_applied_seqno());
    EXPECT_NE(read_db->max_persisted_seqno(), _db->max_persisted_seqno());

    // But once we refresh it should match.
    EXPECT_TRUE(read_db->refresh().get());
    EXPECT_TRUE(matches_shadow(read_db));
    EXPECT_EQ(read_db->max_applied_seqno(), _db->max_applied_seqno());
    EXPECT_EQ(read_db->max_persisted_seqno(), _db->max_persisted_seqno());
}

TEST_F(ImplTest, RefreshWithSnapshot) {
    write_at_least(512_KiB);
    _db->flush().get();

    auto read_opts = make_options();
    read_opts->readonly = true;
    auto* read_db = open(read_opts);
    EXPECT_TRUE(matches_shadow(read_db));

    auto snap = read_db->create_snapshot();
    auto shadow = clone_shadow_map();
    EXPECT_TRUE(matches_shadow(shadow, snap->get(), read_db));

    write_at_least(512_KiB);
    _db->flush().get();

    // We should only match one we refresh.
    EXPECT_FALSE(matches_shadow(read_db));
    EXPECT_TRUE(read_db->refresh().get());
    EXPECT_TRUE(matches_shadow(read_db));

    // The snapshot should still match.
    EXPECT_TRUE(matches_shadow(shadow, snap->get(), read_db));
}

TEST_F(ImplTest, RefreshFailsForLowerSeqno) {
    write_at_least(512_KiB);
    _db->flush().get();

    // Nefarious case: open another database so it gets a low seqno and we can
    // flush a seqno lower than the refreshed database below.
    auto* another_db = open(make_options());

    // Flush a couple more times to bump the seqno.
    write_at_least(512_KiB);
    _db->flush().get();
    write_at_least(512_KiB);
    _db->flush().get();

    // Open a read-only database.
    auto read_opts = make_options();
    read_opts->readonly = true;
    auto* read_db = open(read_opts);

    // Write some data to the other database and flush, lowering the sequence
    // number.
    write_at_least(512_KiB, another_db);
    another_db->flush().get();

    // Refreshing shouldn't work.
    auto before_refresh = read_db->max_applied_seqno();
    EXPECT_ANY_THROW(read_db->refresh().get());
    EXPECT_EQ(read_db->max_applied_seqno(), before_refresh);
}

} // namespace
