// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"
#include "features/feature_table.h"
#include "random/generators.h"
#include "storage/kvstore.h"
#include "storage/storage_resources.h"
#include "test_utils/fixture.h"

#include <seastar/util/file.hh>

// This fixture manages the dependencies needed to create kvstore instance.
// It's the responsiblity of the test to stop the kvstore instances created
// by the fixture.
class kvstore_test_fixture {
public:
    kvstore_test_fixture()
      : _test_dir(
          ssx::sformat("kvstore_test_{}", random_generators::get_int(4000)))
      , _kv_config(prepare_store().get()) {
        _feature_table.start().get();
        _feature_table
          .invoke_on_all(
            [](features::feature_table& f) { f.testing_activate_all(); })
          .get();
    }

    std::unique_ptr<storage::kvstore> make_kvstore() {
        return std::make_unique<storage::kvstore>(
          _kv_config, ss::this_shard_id(), resources, _feature_table);
    }

    ~kvstore_test_fixture() {
        _feature_table.stop().get();
        cleanup_store().get();
    }

private:
    /// Call this at end of tests to avoid leaving garbage
    /// directories behind
    ss::future<> cleanup_store() {
        std::filesystem::path dir_path{_test_dir};
        return ss::recursive_remove_directory(dir_path);
    }

    /// Remove any existing store at this path, and return a config
    /// for constructing a new store.
    ss::future<storage::kvstore_config> prepare_store() {
        if (co_await ss::file_exists(_test_dir)) {
            // Tests can fail in mysterious ways if there's already a store
            // in the location they're trying to use.  Even though tests
            // clean up on success, they might leave directories behind
            // on failure.
            co_await cleanup_store();
        }

        co_return storage::kvstore_config(
          8192,
          config::mock_binding(std::chrono::milliseconds(10)),
          _test_dir,
          storage::make_sanitized_file_config());
    }

    storage::storage_resources resources{};
    ss::sstring _test_dir;
    storage::kvstore_config _kv_config;
    ss::sharded<features::feature_table> _feature_table;
};
