// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once
#include "cluster/producer_state_manager.h"
#include "cluster/rm_stm.h"
#include "config/mock_property.h"
#include "raft/tests/simple_raft_fixture.h"

#include <seastar/core/sharded.hh>

#include <chrono>

static ss::logger logger{"rm_stm-test"};
static prefix_logger ctx_logger{logger, ""};

static constexpr auto large_timeout = std::chrono::minutes(30);

struct rm_stm_test_fixture : simple_raft_fixture {
    void create_stm_and_start_raft(
      storage::ntp_config::default_overrides overrides = {}) {
        max_concurent_producers.start(std::numeric_limits<size_t>::max()).get();
        producer_expiration_ms.start(large_timeout).get();
        producer_state_manager
          .start(
            ss::sharded_parameter(
              [this] { return max_concurent_producers.local().bind(); }),
            ss::sharded_parameter(
              [this] { return producer_expiration_ms.local().bind(); }),
            config::mock_binding(std::numeric_limits<uint64_t>::max()))
          .get();
        producer_state_manager
          .invoke_on_all([](cluster::tx::producer_state_manager& mgr) {
              return mgr.start();
          })
          .get();
        create_raft(overrides);
        raft::state_machine_manager_builder stm_m_builder;

        _stm = stm_m_builder.create_stm<cluster::rm_stm>(
          logger,
          _raft.get(),
          tx_gateway_frontend,
          _feature_table,
          producer_state_manager,
          std::nullopt);

        _raft->start(std::move(stm_m_builder)).get();
        _started = true;
    }

    cluster::rm_stm& start_and_disable_auto_abort() {
        create_stm_and_start_raft();
        _stm->testing_only_disable_auto_abort();
        _stm->start().get();
        wait_for_confirmed_leader();
        wait_for_meta_initialized();
        return *_stm;
    }

    ~rm_stm_test_fixture() {
        if (_started) {
            stop_all();
            producer_state_manager.stop().get();
            producer_expiration_ms.stop().get();
            max_concurent_producers.stop().get();
        }
    }

    const cluster::rm_stm::producers_t& producers() const {
        return _stm->_producers;
    }

    auto local_snapshot(uint8_t version) {
        return _stm->do_take_local_snapshot(version, {});
    }

    void update_producer_expiration(std::chrono::milliseconds value) {
        producer_expiration_ms
          .invoke_on_all(
            [value](auto& local) mutable { local.update(std::move(value)); })
          .get();
    }

    auto take_raft_snapshot(model::offset last_included_offset) {
        return _stm->take_raft_snapshot(last_included_offset);
    }

    auto apply_raft_snapshot(const iobuf& buf) {
        return _stm->apply_raft_snapshot(buf);
    }

    auto apply_snapshot(raft::stm_snapshot_header hdr, iobuf buf) {
        return _stm->apply_local_snapshot(hdr, std::move(buf));
    }

    // Tears down all infrastructure and recreates it from the same data
    // directory. Invalidates all existing references to _stm and _raft.
    void restart_stm_and_raft(
      storage::ntp_config::default_overrides overrides = {}) {
        stop_all();
        producer_state_manager.stop().get();
        producer_expiration_ms.stop().get();
        max_concurent_producers.stop().get();
        create_stm_and_start_raft(overrides);
        _stm->testing_only_disable_auto_abort();
        _stm->start().get();
        wait_for_confirmed_leader();
        wait_for_meta_initialized();
    }

    auto wait_for_kafka_offset_apply(kafka::offset offset) {
        auto raft_offset = _stm->to_log_offset(offset);
        return _stm->wait(raft_offset, model::timeout_clock::now() + 10ms);
    }

    auto get_expired_producers() const { return _stm->get_expired_producers(); }

    auto stm_read_lock() { return _stm->_state_lock.hold_read_lock(); }

    auto maybe_create_producer(model::producer_identity pid) {
        return stm_read_lock().then([pid, this](auto /*units*/) {
            return _stm->maybe_create_producer(pid);
        });
    }

    auto reset_producers() {
        return _stm->_state_lock.hold_write_lock().then([this](auto units) {
            return _stm->reset_producers().then([units = std::move(units)] {});
        });
    }

    auto rearm_eviction_timer(std::chrono::milliseconds period) {
        return producer_state_manager
          .invoke_on_all([period](auto& mgr) {
              return mgr.rearm_eviction_timer_for_testing(period);
          })
          .get();
    }

    auto get_aborted_txs() {
        return _stm->aborted_transactions(
          model::offset::min(), _stm->last_applied_offset());
    }

    ss::sharded<config::mock_property<size_t>> max_concurent_producers;
    ss::sharded<config::mock_property<std::chrono::milliseconds>>
      producer_expiration_ms;
    ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;
    ss::sharded<cluster::tx::producer_state_manager> producer_state_manager;
    ss::shared_ptr<cluster::rm_stm> _stm;
};
