/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "config/configuration.h"
#include "coproc/ntp_context.h"
#include "coproc/offset_storage_utils.h"
#include "model/namespace.h"
#include "storage/snapshot.h"
#include "test_utils/fixture.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <chrono>
#include <filesystem>

class offset_keeper_fixture {
public:
    offset_keeper_fixture()
      : _base_dir(make_base_dir())
      , _api(make_api())
      , _snap(
          _base_dir,
          storage::snapshot_manager::default_snapshot_filename,
          ss::default_priority_class()) {
        _api.start().get();
    }

    ~offset_keeper_fixture() {
        _api.stop().get();
        std::filesystem::remove_all(_base_dir);
    }

    /// Create some logs to properly initialize a valid ntp_context_cache
    coproc::ntp_context_cache create_test_cache(std::size_t n) {
        coproc::ntp_context_cache ntpcc;
        for (auto i = 0; i < n; ++i) {
            model::ntp ntp(
              model::kafka_namespace,
              model::topic(fmt::format("test_topic_{}", i)),
              model::partition_id(0));
            storage::log log = _api.log_mgr()
                                 .manage(
                                   storage::ntp_config(ntp, _base_dir.string()))
                                 .get();
            auto [itr, _] = ntpcc.emplace(
              ntp, ss::make_lw_shared<coproc::ntp_context>(log));
            /// Insert some test data
            itr->second->offsets.emplace(
              coproc::script_id(4444),
              coproc::ntp_context::offset_pair{
                .last_read = model::offset(5), .last_acked = model::offset(3)});
            itr->second->offsets.emplace(
              coproc::script_id(3333),
              coproc::ntp_context::offset_pair{
                .last_read = model::offset(10),
                .last_acked = model::offset(10)});
        }
        return ntpcc;
    }

    storage::log_manager& log_mgr() { return _api.log_mgr(); }
    storage::snapshot_manager& snapshot_mgr() { return _snap; }

private:
    static std::filesystem::path make_base_dir() {
        return std::filesystem::current_path()
               / fmt::format("coproc_test.dir_{}", time(0)) / "data_dir";
    }

    storage::api make_api() const {
        storage::log_config conf{
          storage::log_config::storage_type::disk,
          ss::sstring(_base_dir.string()),
          1024,
          storage::debug_sanitize_files::yes};
        return storage::api(
          storage::kvstore_config(
            1_MiB, 10ms, conf.base_dir, storage::debug_sanitize_files::yes),
          conf);
    }

    std::filesystem::path _base_dir;
    storage::api _api;
    storage::snapshot_manager _snap;
};

namespace coproc {
bool operator==(
  const ntp_context::offset_pair& a, const ntp_context::offset_pair& b) {
    return a.last_acked == b.last_acked && a.last_read == b.last_read;
}
} // namespace coproc

FIXTURE_TEST(offset_keeper_saved_offsets, offset_keeper_fixture) {
    coproc::ntp_context_cache cache, retrieved_cache;
    cache = create_test_cache(50);

    /// Write these offsets to disk
    coproc::save_offsets(snapshot_mgr(), cache).get();

    /// Attempt to retrieve all stored offsets
    retrieved_cache = coproc::recover_offsets(snapshot_mgr(), log_mgr()).get0();

    const size_t total_offsets = std::accumulate(
      retrieved_cache.cbegin(),
      retrieved_cache.cend(),
      size_t(0),
      [](size_t acc, const coproc::ntp_context_cache::value_type& p) {
          return acc + p.second->offsets.size();
      });
    /// Created test cache with 50 static topics...
    BOOST_CHECK_EQUAL(retrieved_cache.size(), 50);
    /// and for every topic 2 scripts are tracking offsets for 100 total
    BOOST_CHECK_EQUAL(total_offsets, 100);
}
