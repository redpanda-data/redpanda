// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"
#include "random/generators.h"
#include "storage/kvstore.h"
#include "storage/tests/kvstore_fixture.h"
#include "test_utils/random_bytes.h"

#include <gtest/gtest.h>

template<typename T>
static void set_configuration(ss::sstring p_name, T v) {
    ss::smp::invoke_on_all([p_name, v = std::move(v)] {
        config::shard_local_cfg().get(p_name).set_value(v);
    }).get();
}

TEST_F(kvstore_test_fixture, key_space) {
    set_configuration("disable_metrics", true);

    auto kvs = make_kvstore();
    kvs->start().get();

    const auto value_a = bytes_to_iobuf(tests::random_bytes(100));
    const auto value_b = bytes_to_iobuf(tests::random_bytes(100));
    const auto value_c = bytes_to_iobuf(tests::random_bytes(100));
    const auto value_d = bytes_to_iobuf(tests::random_bytes(100));

    const auto empty_key = bytes();
    const auto key = tests::random_bytes(2);

    kvs->put(storage::kvstore::key_space::testing, key, value_a.copy()).get();
    kvs->put(storage::kvstore::key_space::consensus, key, value_b.copy()).get();

    kvs->put(storage::kvstore::key_space::testing, empty_key, value_c.copy())
      .get();
    kvs->put(storage::kvstore::key_space::consensus, empty_key, value_d.copy())
      .get();

    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(), value_a);
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::consensus, key).value(), value_b);
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, empty_key).value(),
      value_c);
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::consensus, empty_key).value(),
      value_d);

    std::map<bytes, iobuf> testing_kvs;
    kvs
      ->for_each(
        storage::kvstore::key_space::testing,
        [&](bytes_view key, const iobuf& val) {
            EXPECT_TRUE(testing_kvs.emplace(key, val.copy()).second);
        })
      .get();
    EXPECT_EQ(testing_kvs.size(), 2);
    EXPECT_EQ(testing_kvs.at(key), value_a);
    EXPECT_EQ(testing_kvs.at(empty_key), value_c);

    kvs->stop().get();

    // still all true after recovery
    kvs = make_kvstore();
    kvs->start().get();

    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(), value_a);
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::consensus, key).value(), value_b);
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, empty_key).value(),
      value_c);
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::consensus, empty_key).value(),
      value_d);

    kvs->stop().get();
}

TEST_F(kvstore_test_fixture, kvstore_empty) {
    set_configuration("disable_metrics", true);

    // empty started then stopped
    auto kvs = make_kvstore();
    kvs->start().get();
    kvs->stop().get();

    // and can restart from empty
    kvs = make_kvstore();
    kvs->start().get();
    kvs->stop().get();

    std::unordered_map<bytes, iobuf> truth;

    // now fill it up with some key value pairs
    kvs = make_kvstore();
    kvs->start().get();

    std::vector<ss::future<>> batch;
    for (int i = 0; i < 500; i++) {
        auto key = tests::random_bytes(2);
        auto value = bytes_to_iobuf(tests::random_bytes(100));

        truth[key] = value.copy();
        batch.push_back(kvs->put(
          storage::kvstore::key_space::testing, key, std::move(value)));
        if (batch.size() > 10) {
            ss::when_all(batch.begin(), batch.end()).get();
            batch.clear();
        }
    }
    if (!batch.empty()) {
        ss::when_all(batch.begin(), batch.end()).get();
        batch.clear();
    }

    // equal
    EXPECT_FALSE(truth.empty());
    for (auto& e : truth) {
        EXPECT_EQ(
          kvs->get(storage::kvstore::key_space::testing, e.first).value(),
          e.second);
    }

    // now remove all of the keys
    for (auto& e : truth) {
        kvs->remove(storage::kvstore::key_space::testing, e.first).get();
    }
    truth.clear();

    // the db should be empty now
    EXPECT_TRUE(kvs->empty());
    kvs->stop().get();

    // now restart the db and ensure still empty
    kvs = make_kvstore();
    kvs->start().get();
    EXPECT_TRUE(kvs->empty());
    kvs->stop().get();
}

TEST_F(kvstore_test_fixture, kvstore) {
    set_configuration("disable_metrics", true);

    std::unordered_map<bytes, iobuf> truth;

    auto kvs = make_kvstore();
    kvs->start().get();
    for (int i = 0; i < 500; i++) {
        auto key = tests::random_bytes(2);
        auto value = bytes_to_iobuf(tests::random_bytes(100));

        truth[key] = value.copy();
        kvs->put(storage::kvstore::key_space::testing, key, std::move(value))
          .get();
        EXPECT_EQ(
          kvs->get(storage::kvstore::key_space::testing, key).value(),
          truth[key]);

        // maybe delete something
        auto coin = random_generators::get_int(1000);
        if (coin < 500) {
            auto key = tests::random_bytes(2);
            truth.erase(key);
            kvs->remove(storage::kvstore::key_space::testing, key).get();
        }

        for (auto& e : truth) {
            EXPECT_EQ(
              kvs->get(storage::kvstore::key_space::testing, e.first).value(),
              e.second);
        }
    }
    kvs->stop().get();
    kvs.reset(nullptr);

    // shutdown, restart, and verify all the original key-value pairs
    kvs = make_kvstore();
    kvs->start().get();
    for (auto& e : truth) {
        EXPECT_EQ(
          kvs->get(storage::kvstore::key_space::testing, e.first).value(),
          e.second);
    }
    kvs->stop().get();
}

TEST_F(kvstore_test_fixture, stop_flushes_pending_ops) {
    set_configuration("disable_metrics", true);

    // a commit interval far beyond the test duration, so that the flush timer
    // never fires and every put stays queued until stop()
    auto kvs = make_kvstore(std::chrono::hours(1));
    kvs->start().get();

    std::unordered_map<bytes, iobuf> truth;
    std::vector<ss::future<>> puts;
    for (int i = 0; i < 10; i++) {
        auto key = tests::random_bytes(8);
        auto value = bytes_to_iobuf(tests::random_bytes(100));
        truth[key] = value.copy();
        puts.push_back(kvs->put(
          storage::kvstore::key_space::testing, key, std::move(value)));
    }

    kvs->stop().get();
    // Expect that the puts were flushed by stop()
    for (auto& f : puts) {
        EXPECT_NO_THROW(f.get());
    }
    kvs.reset(nullptr);

    kvs = make_kvstore();
    kvs->start().get();
    for (auto& e : truth) {
        EXPECT_EQ(
          kvs->get(storage::kvstore::key_space::testing, e.first).value(),
          e.second);
    }
    kvs->stop().get();
}

// persist_pre_start() durably writes a key on a fresh (empty) kvstore, before
// start(), and the value survives a restart.
TEST_F(kvstore_test_fixture, persist_pre_start_empty) {
    set_configuration("disable_metrics", true);

    const auto key = tests::random_bytes(8);
    const auto value = bytes_to_iobuf(tests::random_bytes(128));

    auto kvs = make_kvstore();
    kvs->recover().get();
    kvs
      ->persist_pre_start(
        storage::kvstore::key_space::testing, key, value.copy())
      .get();
    kvs->start().get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(), value);
    kvs->stop().get();

    // Durable across a restart (it was written to a snapshot).
    kvs = make_kvstore();
    kvs->start().get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(), value);
    kvs->stop().get();
}

// persist_pre_start() must not corrupt a kvstore that already has state on disk
// (a snapshot and segments): the pre-existing keys and the injected key must
// both be present, before and after a restart.
TEST_F(kvstore_test_fixture, persist_pre_start_preserves_existing_state) {
    set_configuration("disable_metrics", true);

    // Populate with enough data (>> the 8KiB max segment size) to force segment
    // rolls and at least one snapshot, then stop so it's all on disk.
    std::map<bytes, iobuf> truth;
    auto kvs = make_kvstore();
    kvs->start().get();
    for (int i = 0; i < 200; i++) {
        auto key = tests::random_bytes(8);
        auto value = bytes_to_iobuf(tests::random_bytes(128));
        truth[key] = value.copy();
        kvs->put(storage::kvstore::key_space::testing, key, std::move(value))
          .get();
    }
    kvs->stop().get();

    // Recover that state, inject a new key via persist_pre_start (before
    // start()), then bring the store up.
    const auto sentinel_key = tests::random_bytes(8);
    const auto sentinel_value = bytes_to_iobuf(tests::random_bytes(128));
    kvs = make_kvstore();
    kvs->recover().get();
    kvs
      ->persist_pre_start(
        storage::kvstore::key_space::testing,
        sentinel_key,
        sentinel_value.copy())
      .get();
    kvs->start().get();

    // Pre-existing state is intact and the injected key is present.
    for (const auto& e : truth) {
        EXPECT_EQ(
          kvs->get(storage::kvstore::key_space::testing, e.first).value(),
          e.second);
    }
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, sentinel_key).value(),
      sentinel_value);
    kvs->stop().get();

    // ...and both survive a subsequent restart (snapshot/segment offsets line
    // up on recovery).
    kvs = make_kvstore();
    kvs->start().get();
    for (const auto& e : truth) {
        EXPECT_EQ(
          kvs->get(storage::kvstore::key_space::testing, e.first).value(),
          e.second);
    }
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, sentinel_key).value(),
      sentinel_value);
    kvs->stop().get();
}

// The bootstrap sequence: a pre-start write followed by normal post-start
// writes. This is the key offset-bookkeeping guarantee - the pre-start snapshot
// (at offset 0) and the later segment writes must be consistent on recovery, so
// everything survives a restart.
TEST_F(
  kvstore_test_fixture, persist_pre_start_then_normal_puts_survive_restart) {
    set_configuration("disable_metrics", true);

    const auto early_key = tests::random_bytes(8);
    const auto early_value = bytes_to_iobuf(tests::random_bytes(128));

    auto kvs = make_kvstore();
    kvs->recover().get();
    kvs
      ->persist_pre_start(
        storage::kvstore::key_space::testing, early_key, early_value.copy())
      .get();
    kvs->start().get();

    // Enough normal writes to roll segments and snapshot past the pre-start op.
    std::map<bytes, iobuf> truth;
    for (int i = 0; i < 200; i++) {
        auto key = tests::random_bytes(8);
        auto value = bytes_to_iobuf(tests::random_bytes(128));
        truth[key] = value.copy();
        kvs->put(storage::kvstore::key_space::testing, key, std::move(value))
          .get();
    }
    kvs->stop().get();

    kvs = make_kvstore();
    kvs->start().get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, early_key).value(),
      early_value);
    for (const auto& e : truth) {
        EXPECT_EQ(
          kvs->get(storage::kvstore::key_space::testing, e.first).value(),
          e.second);
    }
    kvs->stop().get();
}

// Multiple pre-start writes each advance the offset and snapshot; all must be
// durable across a restart.
TEST_F(kvstore_test_fixture, persist_pre_start_multiple_keys) {
    set_configuration("disable_metrics", true);

    std::map<bytes, iobuf> truth;
    auto kvs = make_kvstore();
    kvs->recover().get();
    for (int i = 0; i < 5; i++) {
        auto key = tests::random_bytes(8);
        auto value = bytes_to_iobuf(tests::random_bytes(128));
        truth[key] = value.copy();
        kvs
          ->persist_pre_start(
            storage::kvstore::key_space::testing, key, std::move(value))
          .get();
    }
    kvs->start().get();
    for (const auto& e : truth) {
        EXPECT_EQ(
          kvs->get(storage::kvstore::key_space::testing, e.first).value(),
          e.second);
    }
    kvs->stop().get();

    kvs = make_kvstore();
    kvs->start().get();
    for (const auto& e : truth) {
        EXPECT_EQ(
          kvs->get(storage::kvstore::key_space::testing, e.first).value(),
          e.second);
    }
    kvs->stop().get();
}

// The crash scenario the feature guards against: a value is persisted before
// start(), then the process dies before doing anything else. A fresh recovery
// must still see the value (it was durably snapshotted).
TEST_F(kvstore_test_fixture, persist_pre_start_survives_crash_before_start) {
    set_configuration("disable_metrics", true);

    const auto key = tests::random_bytes(8);
    const auto value = bytes_to_iobuf(tests::random_bytes(128));

    auto kvs = make_kvstore();
    kvs->recover().get();
    kvs
      ->persist_pre_start(
        storage::kvstore::key_space::testing, key, value.copy())
      .get();
    // Simulate a crash: drop the instance without ever start()ing or stop()ing
    // it. No flush fiber was spawned, so tearing down a recover-only kvstore is
    // clean.
    kvs.reset();

    kvs = make_kvstore();
    kvs->start().get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(), value);
    kvs->stop().get();
}

// A normal write after start() to the same key supersedes the pre-start value,
// and the newer value wins on recovery (the post-start op sits at a later
// offset than the pre-start snapshot).
TEST_F(kvstore_test_fixture, persist_pre_start_overwritten_by_later_put) {
    set_configuration("disable_metrics", true);

    const auto key = tests::random_bytes(8);
    const auto pre_start_value = bytes_to_iobuf(tests::random_bytes(128));
    const auto post_start_value = bytes_to_iobuf(tests::random_bytes(128));

    auto kvs = make_kvstore();
    kvs->recover().get();
    kvs
      ->persist_pre_start(
        storage::kvstore::key_space::testing, key, pre_start_value.copy())
      .get();
    kvs->start().get();
    kvs->put(storage::kvstore::key_space::testing, key, post_start_value.copy())
      .get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(),
      post_start_value);
    kvs->stop().get();

    // The later value wins after a restart.
    kvs = make_kvstore();
    kvs->start().get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(),
      post_start_value);
    kvs->stop().get();
}

// persist_pre_start updates (not just inserts) a key that already exists in the
// recovered db, and the update is durable.
TEST_F(kvstore_test_fixture, persist_pre_start_updates_existing_key) {
    set_configuration("disable_metrics", true);

    const auto key = tests::random_bytes(8);
    const auto old_value = bytes_to_iobuf(tests::random_bytes(128));
    const auto new_value = bytes_to_iobuf(tests::random_bytes(128));

    // Write the key normally and persist it.
    auto kvs = make_kvstore();
    kvs->start().get();
    kvs->put(storage::kvstore::key_space::testing, key, old_value.copy()).get();
    kvs->stop().get();

    // Recover, then update the same key via persist_pre_start before start().
    kvs = make_kvstore();
    kvs->recover().get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(), old_value);
    kvs
      ->persist_pre_start(
        storage::kvstore::key_space::testing, key, new_value.copy())
      .get();
    kvs->start().get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(), new_value);
    kvs->stop().get();

    // The update is durable across a restart.
    kvs = make_kvstore();
    kvs->start().get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(), new_value);
    kvs->stop().get();
}

// persist_pre_start writes into the given key space only; the same key in
// another key space is unaffected (verifies its key namespacing matches the
// normal read path).
TEST_F(kvstore_test_fixture, persist_pre_start_key_space_isolation) {
    set_configuration("disable_metrics", true);

    const auto key = tests::random_bytes(8);
    const auto value = bytes_to_iobuf(tests::random_bytes(128));

    auto kvs = make_kvstore();
    kvs->recover().get();
    kvs
      ->persist_pre_start(
        storage::kvstore::key_space::testing, key, value.copy())
      .get();
    kvs->start().get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(), value);
    EXPECT_FALSE(
      kvs->get(storage::kvstore::key_space::consensus, key).has_value());
    kvs->stop().get();
}
