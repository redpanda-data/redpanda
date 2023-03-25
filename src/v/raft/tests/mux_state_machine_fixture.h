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
#include "model/timeout_clock.h"
#include "net/unresolved_address.h"
#include "raft/group_manager.h"
#include "raft/mux_state_machine.h"
#include "raft/types.h"
#include "random/generators.h"
#include "rpc/connection_cache.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/types.h"
#include "test_utils/async.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>

#include <chrono>
#include <memory>

using namespace std::chrono_literals; // NOLINT
struct mux_state_machine_fixture {
    mux_state_machine_fixture()
      : _self{0}
      , _data_dir("test_dir_" + random_generators::gen_alphanum_string(6)) {}

    void start_raft(storage::ntp_config::default_overrides overrides = {}) {
        ss::smp::invoke_on_all([]() {
            // We want immediate elections, to avoid a sleep at the start of
            // every instantiation of a test setup.
            config::shard_local_cfg().raft_election_timeout_ms.set_value(10ms);
        }).get();

        // configure and start kvstore
        storage::kvstore_config kv_conf(
          8192,
          config::mock_binding(std::chrono::milliseconds(10)),
          _data_dir,
          storage::debug_sanitize_files::yes);

        _storage
          .start(
            [kv_conf]() { return kv_conf; },
            [this]() { return default_log_cfg(); },
            std::ref(_feature_table))
          .get0();
        _storage.invoke_on_all(&storage::api::start).get0();
        _connections.start().get0();
        _recovery_throttle
          .start(
            ss::sharded_parameter([] { return config::mock_binding(100_MiB); }))
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
                };
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
          .get0();

        _group_mgr.invoke_on_all(&raft::group_manager::start).get0();

        _raft = _storage.local()
                  .log_mgr()
                  .manage(storage::ntp_config(
                    _ntp,
                    _data_dir,
                    std::make_unique<storage::ntp_config::default_overrides>(
                      overrides)))
                  .then([this](storage::log&& log) mutable {
                      auto group = raft::group_id(0);
                      return _group_mgr.local()
                        .create_group(
                          group,
                          {self_broker()},
                          log,
                          raft::with_learner_recovery_throttle::yes)
                        .then([log](ss::lw_shared_ptr<raft::consensus> c) {
                            return c->start().then([c] { return c; });
                        });
                  })
                  .get0();

        _started = true;
    }
    ~mux_state_machine_fixture() { stop_all(); }

    void stop_all() {
        if (_started) {
            _recovery_throttle.stop().get();
            _group_mgr.stop().get0();
            if (_raft) {
                _raft.release();
            }
            _connections.stop().get0();
            _feature_table.stop().get0();
            _storage.stop().get0();
        }
    }

    storage::log_config default_log_cfg() {
        return storage::log_config(
          _data_dir,
          100_MiB,
          storage::debug_sanitize_files::yes,
          ss::default_priority_class(),
          storage::with_cache::yes);
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
        }).get0();
    }

    void wait_for_confirmed_leader() {
        using namespace std::chrono_literals;
        tests::cooperative_spin_wait_with_timeout(10s, [this] {
            return _raft->is_leader();
        }).get0();
    }

    void wait_for_meta_initialized() {
        using namespace std::chrono_literals;
        tests::cooperative_spin_wait_with_timeout(10s, [this] {
            return _raft->meta().commit_index >= model::offset(0);
        }).get0();
    }

    model::node_id _self;
    model::ntp _ntp = model::ntp(
      model::ns("default"), model::topic("test"), model::partition_id(0));

    ss::sstring _data_dir;
    cluster::consensus_ptr _raft;
    ss::sharded<rpc::connection_cache> _connections;
    ss::sharded<storage::api> _storage;
    ss::sharded<features::feature_table> _feature_table;
    ss::sharded<raft::group_manager> _group_mgr;
    ss::sharded<raft::recovery_throttle> _recovery_throttle;
    bool _started = false;
};
