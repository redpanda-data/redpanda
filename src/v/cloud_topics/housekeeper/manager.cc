/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/housekeeper/manager.h"

#include "cloud_topics/frontend/frontend.h"
#include "cloud_topics/housekeeper/housekeeper.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/state_accessors.h"
#include "config/configuration.h"
#include "model/timeout_clock.h"
#include "utils/retry_chain_node.h"

#include <exception>
#include <memory>
#include <stdexcept>

namespace cloud_topics {

namespace {

static constexpr auto stm_timeout = std::chrono::seconds(10);

class l0_metastore_impl : public housekeeper::l0_metadata_storage {
public:
    explicit l0_metastore_impl(
      chunked_hash_map<model::topic_id_partition, housekeeper_manager::state>*
        state)
      : _state(state) {}

    kafka::offset
    get_start_offset(const model::topic_id_partition& tidp) override {
        auto api = get_api(tidp);
        return api.get_start_offset();
    }

    kafka::offset
    get_last_reconciled_offset(const model::topic_id_partition& tidp) override {
        auto api = get_api(tidp);
        return api.get_last_reconciled_offset();
    }

    ss::future<> set_start_offset(
      const model::topic_id_partition& tidp,
      kafka::offset offset,
      ss::abort_source* as) override {
        auto api = get_api(tidp);
        co_await api.set_start_offset(
          offset, model::timeout_clock::now() + stm_timeout, *as);
    }

    kafka::offset get_max_allowed_start_offset(
      const model::topic_id_partition& tidp) override {
        // If the partition has a pinned offset (e.g. it has not translated data
        // to Iceberg), that bounds our potential start offset.
        auto& state = _state->at(tidp);
        auto lowest_pinned = state.partition->raft()
                               ->log()
                               ->stm_hookset()
                               ->lowest_pinned_data_offset();
        if (!lowest_pinned.has_value()) {
            return kafka::offset::max();
        }
        return lowest_pinned.value();
    }

    std::optional<cloud_topics::cluster_epoch> estimate_inactive_epoch(
      const model::topic_id_partition& tidp) noexcept override {
        try {
            return get_api(tidp).estimate_inactive_epoch();
        } catch (...) {
            auto ex = std::current_exception();
            vlog(cd_log.warn, "Error collecting inactive epoch... {}", ex);
            return std::nullopt;
        }
    }
    ss::future<std::optional<cloud_topics::cluster_epoch>>
    get_current_cluster_epoch(
      const model::topic_id_partition& tidp,
      ss::abort_source* as) noexcept override {
        auto res = co_await get_frontend(tidp).get_current_epoch(*as);
        if (!res.has_value()) {
            co_return std::nullopt;
        }
        co_return res.value();
    }

    ss::future<> advance_epoch(
      const model::topic_id_partition& tidp,
      cloud_topics::cluster_epoch epoch,
      ss::abort_source* as) noexcept override {
        try {
            auto res = co_await get_api(tidp).advance_epoch(
              epoch, model::timeout_clock::now() + 5s, *as);
            if (!res.has_value()) {
                throw std::runtime_error(fmt::format("{}", res.error()));
            }
        } catch (...) {
            auto ex = std::current_exception();
            vlog(cd_log.warn, "Error advancing epoch: {}", ex);
        }
    }

    ss::future<> sync_to_next_placeholder(
      const model::topic_id_partition& tidp,
      ss::abort_source* as) noexcept override {
        try {
            auto res = co_await get_api(tidp).sync_to_next_placeholder(
              model::timeout_clock::now() + 5s, *as);
            if (!res.has_value()) {
                throw std::runtime_error(fmt::format("{}", res.error()));
            }
        } catch (...) {
            auto ex = std::current_exception();
            vlog(cd_log.warn, "Error advancing LRLO to epoch window: {}", ex);
        }
        co_return;
    }

private:
    ctp_stm_api get_api(const model::topic_id_partition& tidp) {
        auto& state = _state->at(tidp);
        auto stm = state.partition->raft()->stm_manager()->get<ctp_stm>();
        if (!stm) {
            throw std::runtime_error(fmt::format("no ctp_stm for {}", tidp));
        }
        return ctp_stm_api(stm);
    }

    cloud_topics::frontend get_frontend(const model::topic_id_partition& tidp) {
        auto& state = _state->at(tidp);
        auto ct_state = state.partition->get_cloud_topics_state();
        if (ct_state == nullptr || !ct_state->local_is_initialized()) {
            throw std::runtime_error(
              fmt::format("no cloud topics state for {}", tidp));
        }
        return cloud_topics::frontend{
          state.partition, ct_state->local().get_data_plane()};
    }

    chunked_hash_map<model::topic_id_partition, housekeeper_manager::state>*
      _state;
};

class topic_configuration : public housekeeper::retention_configuration {
public:
    explicit topic_configuration(
      chunked_hash_map<model::topic_id_partition, housekeeper_manager::state>*
        state)
      : _state(state) {}

    std::optional<size_t>
    retention_bytes(const model::topic_id_partition& tidp) override {
        auto& state = _state->at(tidp);
        return state.partition->get_ntp_config().retention_bytes();
    }

    std::optional<std::chrono::milliseconds>
    retention_duration(const model::topic_id_partition& tidp) override {
        auto& state = _state->at(tidp);
        return state.partition->get_ntp_config().retention_duration();
    }

private:
    chunked_hash_map<model::topic_id_partition, housekeeper_manager::state>*
      _state;
};

} // namespace

housekeeper_manager::housekeeper_manager(l1::metastore* metastore)
  : _l1_metastore(metastore)
  , _l0_metastore(std::make_unique<l0_metastore_impl>(&_state))
  , _retention_configuration(std::make_unique<topic_configuration>(&_state))
  , _queue([](const std::exception_ptr& ex) {
      vlog(
        cd_log.error,
        "unexpected error in housekeeper_manager work queue: {}",
        ex);
  }) {}

void housekeeper_manager::start_housekeeper(
  model::topic_id_partition tidp, ss::lw_shared_ptr<cluster::partition> p) {
    _queue.submit([this, tidp, p = std::move(p)]() -> ss::future<> {
        if (_state.contains(tidp)) {
            return ss::now();
        }
        auto hk = std::make_unique<housekeeper>(
          tidp,
          _l0_metastore.get(),
          _l1_metastore,
          _retention_configuration.get(),
          config::shard_local_cfg()
            .cloud_storage_housekeeping_interval_ms.bind());
        vlog(cd_log.debug, "starting housekeeper for: {}", tidp);
        auto [it, _] = _state.emplace(
          tidp,
          state{
            .partition = p,
            .housekeeper = std::move(hk),
          });
        return it->second.housekeeper->start();
    });
}

void housekeeper_manager::stop_housekeeper(model::topic_id_partition tidp) {
    _queue.submit([this, tidp]() -> ss::future<> {
        auto it = _state.find(tidp);
        if (it == _state.end()) {
            return ss::now();
        }
        vlog(cd_log.debug, "stopping housekeeper for: {}", tidp);
        return it->second.housekeeper->stop().then(
          [this, it] { _state.erase(it); });
    });
}

ss::future<> housekeeper_manager::start() { co_return; }

ss::future<> housekeeper_manager::stop() {
    vlog(cd_log.info, "stopping cloud_topics::housekeeper_manager");
    co_await _queue.shutdown();
    vlog(cd_log.info, "cloud_topics::housekeeper_manager queue stopped");
    for (auto& [tidp, state] : _state) {
        vlog(cd_log.info, "stopping cloud_topics::housekeeper {}", tidp);
        co_await state.housekeeper->stop();
    }
    vlog(cd_log.info, "successfully stopped cloud_topics::housekeeper");
    _state.clear();
}

} // namespace cloud_topics
