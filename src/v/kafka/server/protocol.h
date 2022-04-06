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

#include "cluster/fwd.h"
#include "config/configuration.h"
#include "coproc/fwd.h"
#include "kafka/latency_probe.h"
#include "kafka/server/fetch_metadata_cache.hh"
#include "kafka/server/fwd.h"
#include "kafka/server/queue_depth_monitor.h"
#include "net/server.h"
#include "security/authorizer.h"
#include "security/credential_store.h"
#include "utils/ema.h"
#include "v8_engine/data_policy_table.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

namespace kafka {

class protocol final : public net::server::protocol {
public:
    protocol(
      ss::smp_service_group,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<cluster::topics_frontend>&,
      ss::sharded<cluster::config_frontend>&,
      ss::sharded<cluster::feature_table>&,
      ss::sharded<quota_manager>&,
      ss::sharded<kafka::group_router>&,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<fetch_session_cache>&,
      ss::sharded<cluster::id_allocator_frontend>&,
      ss::sharded<security::credential_store>&,
      ss::sharded<security::authorizer>&,
      ss::sharded<cluster::security_frontend>&,
      ss::sharded<cluster::controller_api>&,
      ss::sharded<cluster::tx_gateway_frontend>&,
      ss::sharded<coproc::partition_manager>&,
      ss::sharded<v8_engine::data_policy_table>&,
      std::optional<qdc_monitor::config>) noexcept;

    ~protocol() noexcept override = default;
    protocol(const protocol&) = delete;
    protocol& operator=(const protocol&) = delete;
    protocol(protocol&&) noexcept = default;
    protocol& operator=(protocol&&) noexcept = delete;

    const char* name() const final { return "kafka rpc protocol"; }
    // the lifetime of all references here are guaranteed to live
    // until the end of the server (container/parent)
    ss::future<> apply(net::server::resources) final;

    ss::smp_service_group smp_group() const { return _smp_group; }
    cluster::topics_frontend& topics_frontend() {
        return _topics_frontend.local();
    }
    ss::sharded<cluster::config_frontend>& config_frontend() {
        return _config_frontend;
    }
    ss::sharded<cluster::feature_table>& feature_table() {
        return _feature_table;
    }
    cluster::metadata_cache& metadata_cache() {
        return _metadata_cache.local();
    }
    cluster::id_allocator_frontend& id_allocator_frontend() {
        return _id_allocator_frontend.local();
    }
    cluster::tx_gateway_frontend& tx_gateway_frontend() {
        return _tx_gateway_frontend.local();
    }
    kafka::group_router& group_router() { return _group_router.local(); }
    cluster::shard_table& shard_table() { return _shard_table.local(); }
    ss::sharded<coproc::partition_manager>& coproc_partition_manager() {
        return _coproc_partition_manager;
    }
    ss::sharded<cluster::partition_manager>& partition_manager() {
        return _partition_manager;
    }
    coordinator_ntp_mapper& coordinator_mapper();

    fetch_session_cache& fetch_sessions_cache() {
        return _fetch_session_cache.local();
    }
    quota_manager& quota_mgr() { return _quota_mgr.local(); }
    bool is_idempotence_enabled() const { return _is_idempotence_enabled; }
    bool are_transactions_enabled() const { return _are_transactions_enabled; }

    security::credential_store& credentials() { return _credentials.local(); }

    security::authorizer& authorizer() { return _authorizer.local(); }

    cluster::security_frontend& security_frontend() {
        return _security_frontend.local();
    }

    v8_engine::data_policy_table& data_policy_table() {
        return _data_policy_table.local();
    }

    void update_produce_latency(std::chrono::steady_clock::duration x) {
        if (_qdc_mon) {
            _qdc_mon->ema.update(x);
        }
    }

    ss::future<ss::semaphore_units<>> get_request_unit() {
        if (_qdc_mon) {
            return _qdc_mon->qdc.get_unit();
        }
        return ss::make_ready_future<ss::semaphore_units<>>(
          ss::semaphore_units<>());
    }

    cluster::controller_api& controller_api() {
        return _controller_api.local();
    }

    kafka::fetch_metadata_cache& get_fetch_metadata_cache() {
        return _fetch_metadata_cache;
    }

    latency_probe& probe() { return _probe; }

private:
    ss::smp_service_group _smp_group;
    ss::sharded<cluster::topics_frontend>& _topics_frontend;
    ss::sharded<cluster::config_frontend>& _config_frontend;
    ss::sharded<cluster::feature_table>& _feature_table;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    ss::sharded<quota_manager>& _quota_mgr;
    ss::sharded<kafka::group_router>& _group_router;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<kafka::fetch_session_cache>& _fetch_session_cache;
    ss::sharded<cluster::id_allocator_frontend>& _id_allocator_frontend;
    bool _is_idempotence_enabled{false};
    bool _are_transactions_enabled{false};
    ss::sharded<security::credential_store>& _credentials;
    ss::sharded<security::authorizer>& _authorizer;
    ss::sharded<cluster::security_frontend>& _security_frontend;
    ss::sharded<cluster::controller_api>& _controller_api;
    ss::sharded<cluster::tx_gateway_frontend>& _tx_gateway_frontend;
    ss::sharded<coproc::partition_manager>& _coproc_partition_manager;
    ss::sharded<v8_engine::data_policy_table>& _data_policy_table;
    std::optional<qdc_monitor> _qdc_mon;
    kafka::fetch_metadata_cache _fetch_metadata_cache;

    latency_probe _probe;
};

} // namespace kafka
