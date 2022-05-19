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
#include "reflection/adl.h"
#include "storage/kvstore.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/file.hh>

template<typename T>
static void set_configuration(ss::sstring p_name, T v) {
    ss::smp::invoke_on_all([p_name, v = std::move(v)] {
        config::shard_local_cfg().get(p_name).set_value(v);
    }).get0();
}

/// Call this at end of tests to avoid leaving garbage
/// directories behind
static ss::future<> cleanup_store(ss::sstring& dir) {
    std::filesystem::path dir_path{dir};
    return ss::recursive_remove_directory(dir_path);
}

/// Remove any existing store at this path, and return a config
/// for constructing a new store.
static ss::future<storage::kvstore_config> prepare_store(ss::sstring dir) {
    if (co_await ss::file_exists(dir)) {
        // Tests can fail in mysterious ways if there's already a store
        // in the location they're trying to use.  Even though tests
        // clean up on success, they might leave directories behind
        // on failure.
        co_await cleanup_store(dir);
    }

    co_return storage::kvstore_config(
      8192,
      config::mock_binding(std::chrono::milliseconds(10)),
      dir,
      storage::debug_sanitize_files::yes);
}

SEASTAR_THREAD_TEST_CASE(key_space) {
    set_configuration("disable_metrics", true);

    auto dir = ssx::sformat(
      "kvstore_test_{}", random_generators::get_int(4000));

    auto conf = prepare_store(dir).get();

    // empty started then stopped
    storage::storage_resources resources;
    auto kvs = std::make_unique<storage::kvstore>(conf, resources);
    kvs->start().get();

    const auto value_a = bytes_to_iobuf(random_generators::get_bytes(100));
    const auto value_b = bytes_to_iobuf(random_generators::get_bytes(100));
    const auto value_c = bytes_to_iobuf(random_generators::get_bytes(100));
    const auto value_d = bytes_to_iobuf(random_generators::get_bytes(100));

    const auto empty_key = bytes();
    const auto key = random_generators::get_bytes(2);

    kvs->put(storage::kvstore::key_space::testing, key, value_a.copy()).get();
    kvs->put(storage::kvstore::key_space::consensus, key, value_b.copy()).get();

    kvs->put(storage::kvstore::key_space::testing, empty_key, value_c.copy())
      .get();
    kvs->put(storage::kvstore::key_space::consensus, empty_key, value_d.copy())
      .get();

    BOOST_REQUIRE(
      kvs->get(storage::kvstore::key_space::testing, key).value() == value_a);
    BOOST_REQUIRE(
      kvs->get(storage::kvstore::key_space::consensus, key).value() == value_b);
    BOOST_REQUIRE(
      kvs->get(storage::kvstore::key_space::testing, empty_key).value()
      == value_c);
    BOOST_REQUIRE(
      kvs->get(storage::kvstore::key_space::consensus, empty_key).value()
      == value_d);

    kvs->stop().get();

    // still all true after recovery
    kvs = std::make_unique<storage::kvstore>(conf, resources);
    kvs->start().get();

    BOOST_REQUIRE(
      kvs->get(storage::kvstore::key_space::testing, key).value() == value_a);
    BOOST_REQUIRE(
      kvs->get(storage::kvstore::key_space::consensus, key).value() == value_b);
    BOOST_REQUIRE(
      kvs->get(storage::kvstore::key_space::testing, empty_key).value()
      == value_c);
    BOOST_REQUIRE(
      kvs->get(storage::kvstore::key_space::consensus, empty_key).value()
      == value_d);

    kvs->stop().get();

    cleanup_store(dir).get();
}

SEASTAR_THREAD_TEST_CASE(kvstore_empty) {
    set_configuration("disable_metrics", true);

    auto dir = ssx::sformat(
      "kvstore_test_{}", random_generators::get_int(4000));

    auto conf = prepare_store(dir).get();

    // empty started then stopped
    storage::storage_resources resources;
    auto kvs = std::make_unique<storage::kvstore>(conf, resources);
    kvs->start().get();
    kvs->stop().get();

    // and can restart from empty
    kvs = std::make_unique<storage::kvstore>(conf, resources);
    kvs->start().get();
    kvs->stop().get();

    std::unordered_map<bytes, iobuf> truth;

    // now fill it up with some key value pairs
    kvs = std::make_unique<storage::kvstore>(conf, resources);
    kvs->start().get();

    std::vector<ss::future<>> batch;
    for (int i = 0; i < 500; i++) {
        auto key = random_generators::get_bytes(2);
        auto value = bytes_to_iobuf(random_generators::get_bytes(100));

        truth[key] = value.copy();
        batch.push_back(kvs->put(
          storage::kvstore::key_space::testing, key, std::move(value)));
        if (batch.size() > 10) {
            ss::when_all(batch.begin(), batch.end()).get0();
            batch.clear();
        }
    }
    if (!batch.empty()) {
        ss::when_all(batch.begin(), batch.end()).get0();
        batch.clear();
    }

    // equal
    BOOST_REQUIRE(!truth.empty());
    for (auto& e : truth) {
        BOOST_REQUIRE(
          kvs->get(storage::kvstore::key_space::testing, e.first).value()
          == e.second);
    }

    // now remove all of the keys
    for (auto& e : truth) {
        kvs->remove(storage::kvstore::key_space::testing, e.first).get();
    }
    truth.clear();

    // the db should be empty now
    BOOST_REQUIRE(kvs->empty());
    kvs->stop().get();

    // now restart the db and ensure still empty
    kvs = std::make_unique<storage::kvstore>(conf, resources);
    kvs->start().get();
    BOOST_REQUIRE(kvs->empty());
    kvs->stop().get();

    cleanup_store(dir).get();
}

SEASTAR_THREAD_TEST_CASE(kvstore) {
    set_configuration("disable_metrics", true);

    auto dir = ssx::sformat(
      "kvstore_test_{}", random_generators::get_int(4000));

    auto conf = prepare_store(dir).get();

    std::unordered_map<bytes, iobuf> truth;

    storage::storage_resources resources;
    auto kvs = std::make_unique<storage::kvstore>(conf, resources);
    kvs->start().get();
    for (int i = 0; i < 500; i++) {
        auto key = random_generators::get_bytes(2);
        auto value = bytes_to_iobuf(random_generators::get_bytes(100));

        truth[key] = value.copy();
        kvs->put(storage::kvstore::key_space::testing, key, std::move(value))
          .get();
        BOOST_REQUIRE(
          kvs->get(storage::kvstore::key_space::testing, key).value()
          == truth[key]);

        // maybe delete something
        auto coin = random_generators::get_int(1000);
        if (coin < 500) {
            auto key = random_generators::get_bytes(2);
            truth.erase(key);
            kvs->remove(storage::kvstore::key_space::testing, key).get();
        }

        for (auto& e : truth) {
            BOOST_REQUIRE(
              kvs->get(storage::kvstore::key_space::testing, e.first).value()
              == e.second);
        }
    }
    kvs->stop().get();
    kvs.reset(nullptr);

    // shutdown, restart, and verify all the original key-value pairs
    kvs = std::make_unique<storage::kvstore>(conf, resources);
    kvs->start().get();
    for (auto& e : truth) {
        BOOST_REQUIRE(
          kvs->get(storage::kvstore::key_space::testing, e.first).value()
          == e.second);
    }
    kvs->stop().get();

    cleanup_store(dir).get();
}
