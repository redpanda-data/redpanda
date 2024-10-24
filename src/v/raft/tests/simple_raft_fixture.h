/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "config/property.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/consensus.h"
#include "raft/group_manager.h"
#include "raft/types.h"
#include "random/generators.h"
#include "rpc/connection_cache.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "test_utils/async.h"
#include "utils/unresolved_address.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>

#include <chrono>
#include <memory>

using namespace std::chrono_literals; // NOLINT
struct simple_raft_fixture {
    simple_raft_fixture()
      : _self{0}
      , _data_dir("test_dir_" + random_generators::gen_alphanum_string(6)) {}

    void create_raft(storage::ntp_config::default_overrides overrides = {}) {
        // configure and start kvstore
        storage::kvstore_config kv_conf(
          8192,
          config::mock_binding(std::chrono::milliseconds(10)),
          _data_dir,
          storage::make_sanitized_file_config());

        _storage
          .start(
            [kv_conf]() { return kv_conf; },
            [this]() { return default_log_cfg(); },
            std::ref(_feature_table))
          .get();
        _storage.invoke_on_all(&storage::api::start).get();
        _as.start().get();
        _connections.start(std::ref(_as)).get();
        _recovery_throttle
          .start(
            ss::sharded_parameter([] { return config::mock_binding(100_MiB); }),
            ss::sharded_parameter([] { return config::mock_binding(false); }))
          .get();

        _feature_table.start().get();
        _feature_table
          .invoke_on_all(
            [](features::feature_table& f) { f.testing_activate_all(); })
          .get();

        _group_mgr
          .start(
            _self,
            ss::default_scheduling_group(),
            [] {
                return raft::group_manager::configuration{
                  .heartbeat_interval
                  = config::mock_binding<std::chrono::milliseconds>(100ms),
                  .heartbeat_timeout
                  = config::mock_binding<std::chrono::milliseconds>(2000ms),
                  .raft_io_timeout_ms
                  = config::mock_binding<std::chrono::milliseconds>(30s),
                  .enable_lw_heartbeat = config::mock_binding<bool>(true),
                  .recovery_concurrency_per_shard
                  = config::mock_binding<size_t>(64),
                  .election_timeout_ms = config::mock_binding(10ms),
                  .write_caching = config::mock_binding(
                    model::write_caching_mode::default_false),
                  .write_caching_flush_ms = config::mock_binding(100ms),
                  .write_caching_flush_bytes
                  = config::mock_binding<std::optional<size_t>>(std::nullopt),
                  .enable_longest_log_detection = config::mock_binding<bool>(
                    true)};
            },
            [] {
                return raft::recovery_memory_quota::configuration{
                  .max_recovery_memory
                  = config::mock_binding<std::optional<size_t>>(std::nullopt),
                  .default_read_buffer_size = config::mock_binding(512_KiB),
                };
            },
            std::ref(_connections),
            std::ref(_storage),
            std::ref(_recovery_throttle),
            std::ref(_feature_table))
          .get();

        _group_mgr.invoke_on_all(&raft::group_manager::start).get();

        _raft = _storage.local()
                  .log_mgr()
                  .manage(storage::ntp_config(
                    _ntp,
                    _data_dir,
                    std::make_unique<storage::ntp_config::default_overrides>(
                      overrides)))
                  .then([this](ss::shared_ptr<storage::log> log) mutable {
                      auto group = raft::group_id(0);
                      return _group_mgr.local().create_group(
                        group,
                        {self_broker()},
                        log,
                        raft::with_learner_recovery_throttle::yes);
                  })
                  .get();
    }

    void start_raft(storage::ntp_config::default_overrides overrides = {}) {
        create_raft(overrides);
        _raft->start().get();
        _started = true;
    }

    ~simple_raft_fixture() { stop_all(); }

    void stop_all() {
        if (_started) {
            _as
              .invoke_on_all(
                [](auto& local) noexcept { local.request_abort(); })
              .get();
            _recovery_throttle.stop().get();
            _group_mgr.stop().get();
            if (_raft) {
                _raft.release();
            }

            _connections.stop().get();
            _storage.stop().get();
            _feature_table.stop().get();
            _as.stop().get();
        }
        _started = false;
    }

    storage::log_config default_log_cfg() {
        return storage::log_config(
          _data_dir,
          100_MiB,
          ss::default_priority_class(),
          storage::with_cache::yes,
          storage::make_sanitized_file_config());
    }

    model::broker self_broker() {
        return model::broker(
          _self,
          net::unresolved_address("localhost", 9092),
          net::unresolved_address("localhost", 35543),
          std::nullopt,
          model::broker_properties{});
    }

    void wait_for_becoming_leader() {
        using namespace std::chrono_literals;
        tests::cooperative_spin_wait_with_timeout(10s, [this] {
            return _raft->is_elected_leader();
        }).get();
    }

    void wait_for_confirmed_leader() {
        using namespace std::chrono_literals;
        tests::cooperative_spin_wait_with_timeout(10s, [this] {
            return _raft->is_leader();
        }).get();
    }

    void wait_for_meta_initialized() {
        using namespace std::chrono_literals;
        tests::cooperative_spin_wait_with_timeout(10s, [this] {
            return _raft->meta().commit_index >= model::offset(0);
        }).get();
    }

    model::node_id _self;
    model::ntp _ntp = model::ntp(
      model::ns("default"), model::topic("test"), model::partition_id(0));

    ss::logger _test_logger{"mux-test-logger"};
    ss::sstring _data_dir;
    ss::lw_shared_ptr<raft::consensus> _raft;
    ss::sharded<ss::abort_source> _as;
    ss::sharded<rpc::connection_cache> _connections;
    ss::sharded<storage::api> _storage;
    ss::sharded<features::feature_table> _feature_table;
    ss::sharded<raft::group_manager> _group_mgr;
    ss::sharded<raft::coordinated_recovery_throttle> _recovery_throttle;
    bool _started = false;
};
