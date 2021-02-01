/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "raft/group_manager.h"
#include "raft/mux_state_machine.h"
#include "random/generators.h"
#include "rpc/connection_cache.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/types.h"
#include "test_utils/async.h"
#include "utils/unresolved_address.h"

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

    void start_raft() {
        // configure and start kvstore
        storage::kvstore_config kv_conf(
          8192,
          std::chrono::milliseconds(10),
          _data_dir,
          storage::debug_sanitize_files::yes);

        _storage.start(kv_conf, default_log_cfg()).get0();
        _storage.invoke_on_all(&storage::api::start).get0();
        _connections.start().get0();

        _group_mgr
          .start(
            _self,
            30s,
            std::chrono::milliseconds(100),
            std::ref(_connections),
            std::ref(_storage))
          .get0();

        _group_mgr.invoke_on_all(&raft::group_manager::start).get0();

        _raft = _storage.local()
                  .log_mgr()
                  .manage(storage::ntp_config(_ntp, _data_dir))
                  .then([this](storage::log&& log) mutable {
                      auto group = raft::group_id(0);
                      return _group_mgr.local()
                        .create_group(group, {self_broker()}, log)
                        .then([this, log, group](
                                ss::lw_shared_ptr<raft::consensus> c) {
                            return c->start().then([c] { return c; });
                        });
                  })
                  .get0();

        _started = true;
    }
    ~mux_state_machine_fixture() {
        if (_started) {
            _group_mgr.stop().get0();
            _connections.stop().get0();
            _storage.stop().get0();
        }
    }

    storage::log_config default_log_cfg() {
        return storage::log_config(
          storage::log_config::storage_type::disk,
          _data_dir,
          100_MiB,
          storage::debug_sanitize_files::yes,
          storage::with_cache::yes);
    }

    model::broker self_broker() {
        return model::broker(
          _self,
          unresolved_address("localhost", 9092),
          unresolved_address("localhost", 35543),
          std::nullopt,
          model::broker_properties{});
    }

    void wait_for_leader() {
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
    ss::sharded<raft::group_manager> _group_mgr;
    bool _started = false;
};
