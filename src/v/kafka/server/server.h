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
#include "features/feature_table.h"
#include "kafka/latency_probe.h"
#include "kafka/server/fetch_metadata_cache.hh"
#include "kafka/server/fwd.h"
#include "kafka/server/queue_depth_monitor.h"
#include "net/server.h"
#include "security/fwd.h"
#include "security/gssapi_principal_mapper.h"
#include "security/krb5_configurator.h"
#include "security/mtls.h"
#include "ssx/fwd.h"
#include "utils/ema.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

namespace kafka {

class server final : public net::server {
public:
    server(
      ss::sharded<net::server_configuration>*,
      ss::smp_service_group,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<cluster::topics_frontend>&,
      ss::sharded<cluster::config_frontend>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<quota_manager>&,
      ss::sharded<snc_quota_manager>&,
      ss::sharded<kafka::group_router>&,
      ss::sharded<kafka::usage_manager>&,
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
      std::optional<qdc_monitor::config>,
      ssx::thread_worker&) noexcept;

    ~server() noexcept override = default;
    server(const server&) = delete;
    server& operator=(const server&) = delete;
    server(server&&) noexcept = default;
    server& operator=(server&&) noexcept = delete;

    std::string_view name() const final { return "kafka rpc protocol"; }
    // the lifetime of all references here are guaranteed to live
    // until the end of the server (container/parent)
    ss::future<> apply(ss::lw_shared_ptr<net::connection>) final;

    ss::smp_service_group smp_group() const { return _smp_group; }
    cluster::topics_frontend& topics_frontend() {
        return _topics_frontend.local();
    }
    ss::sharded<cluster::config_frontend>& config_frontend() {
        return _config_frontend;
    }
    ss::sharded<features::feature_table>& feature_table() {
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
    usage_manager& usage_mgr() { return _usage_manager.local(); }
    snc_quota_manager& snc_quota_mgr() { return _snc_quota_mgr.local(); }
    bool is_idempotence_enabled() const { return _is_idempotence_enabled; }
    bool are_transactions_enabled() const { return _are_transactions_enabled; }

    security::credential_store& credentials() { return _credentials.local(); }

    security::authorizer& authorizer() { return _authorizer.local(); }

    cluster::security_frontend& security_frontend() {
        return _security_frontend.local();
    }

    void update_produce_latency(std::chrono::steady_clock::duration x) {
        if (_qdc_mon) {
            _qdc_mon->ema.update(x);
        }
    }

    ss::future<ssx::semaphore_units> get_request_unit() {
        if (_qdc_mon) {
            return _qdc_mon->qdc.get_unit();
        }
        return ss::make_ready_future<ssx::semaphore_units>(
          ssx::semaphore_units());
    }

    cluster::controller_api& controller_api() {
        return _controller_api.local();
    }

    kafka::fetch_metadata_cache& get_fetch_metadata_cache() {
        return _fetch_metadata_cache;
    }

    security::gssapi_principal_mapper& gssapi_principal_mapper() {
        return _gssapi_principal_mapper;
    }

    latency_probe& latency_probe() { return _probe; }

    ssx::thread_worker& thread_worker() { return _thread_worker; }

private:
    ss::smp_service_group _smp_group;
    ss::sharded<cluster::topics_frontend>& _topics_frontend;
    ss::sharded<cluster::config_frontend>& _config_frontend;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    ss::sharded<quota_manager>& _quota_mgr;
    ss::sharded<snc_quota_manager>& _snc_quota_mgr;
    ss::sharded<kafka::group_router>& _group_router;
    ss::sharded<kafka::usage_manager>& _usage_manager;
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
    std::optional<qdc_monitor> _qdc_mon;
    kafka::fetch_metadata_cache _fetch_metadata_cache;
    security::tls::principal_mapper _mtls_principal_mapper;
    security::gssapi_principal_mapper _gssapi_principal_mapper;
    security::krb5::configurator _krb_configurator;

    class latency_probe _probe;
    ssx::thread_worker& _thread_worker;
};

} // namespace kafka
