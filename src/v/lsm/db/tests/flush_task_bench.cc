// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/flush_task.h"
#include "lsm/db/memtable.h"
#include "lsm/db/table_cache.h"
#include "lsm/db/version_set.h"
#include "lsm/io/disk_persistence.h"
#include "lsm/sst/block_cache.h"
#include "random/generators.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/defer.hh>

#include <filesystem>

using namespace lsm;

namespace {

// 16MiB of data with 1KiB values = 16384 entries
static constexpr size_t num_entries = 16384;
static constexpr size_t value_size = 1024;

std::filesystem::path make_temp_dir() {
    auto temp_path
      = std::filesystem::temp_directory_path()
        / fmt::format(
          "flush_bench_{}",
          std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(temp_path);
    return temp_path;
}

ss::lw_shared_ptr<db::memtable> create_memtable() {
    auto memtable = ss::make_lw_shared<db::memtable>();
    for (size_t i = 0; i < num_entries; ++i) {
        auto key = ss::sstring(fmt::format("key{:016d}", i));
        auto value_str = random_generators::gen_alphanum_string(value_size);
        auto value = iobuf::from(std::move(value_str));

        auto key_encoded = lsm::internal::key::encode(
          {.key = user_key_view(key),
           .seqno = lsm::internal::sequence_number(i),
           .type = lsm::internal::value_type::value});

        memtable->put(std::move(key_encoded), std::move(value));
    }
    return memtable;
}

class flush_task_fixture {
public:
    flush_task_fixture() {
        // Create memtable with 16MiB of data during setup
        _memtable = create_memtable();
    }

    ss::lw_shared_ptr<db::memtable> _memtable;
};

} // namespace

PERF_TEST_C(flush_task_fixture, flush_16mb) {
    auto temp_dir = make_temp_dir();
    auto cleanup = ss::defer(
      [&temp_dir] { std::filesystem::remove_all(temp_dir); });

    // Setup persistence and version_set
    auto data = co_await io::open_disk_data_persistence(temp_dir);
    auto metadata = co_await io::open_disk_metadata_persistence(temp_dir);

    auto opts = ss::make_lw_shared<lsm::internal::options>();
    auto p = ss::make_lw_shared<lsm::probe>();
    auto block_cache = ss::make_lw_shared<sst::block_cache>(10_MiB, p);
    auto table_cache = ss::make_lw_shared<db::table_cache>(
      data.get(), 100, p, block_cache);
    auto versions = ss::make_lw_shared<db::version_set>(
      metadata.get(), table_cache.get(), opts);
    co_await versions->recover();

    // Measure the flush operation
    ss::abort_source as;
    perf_tests::start_measuring_time();
    auto result = co_await db::run_flush_task(
      opts, data.get(), versions.get(), _memtable, &as);
    perf_tests::stop_measuring_time();

    if (result) {
        co_await versions->log_and_apply(*result);
        perf_tests::do_not_optimize(result);
    }

    co_await table_cache->close();
    co_await metadata->close();
    co_await data->close();
}
