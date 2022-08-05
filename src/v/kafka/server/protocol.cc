// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "protocol.h"

#include "cluster/topics_frontend.h"
#include "config/broker_authn_endpoint.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "kafka/server/connection_context.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/group_router.h"
#include "kafka/server/logger.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "net/connection.h"
#include "security/errc.h"
#include "security/exceptions.h"
#include "security/mtls.h"
#include "security/scram_algorithm.h"
#include "utils/utf8.h"
#include "vlog.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/metrics.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/log.hh>

#include <fmt/format.h>

#include <chrono>
#include <exception>
#include <limits>
#include <memory>

namespace kafka {

protocol::protocol(
  ss::smp_service_group smp,
  ss::sharded<cluster::metadata_cache>& meta,
  ss::sharded<cluster::topics_frontend>& tf,
  ss::sharded<cluster::config_frontend>& cf,
  ss::sharded<cluster::feature_table>& ft,
  ss::sharded<quota_manager>& quota,
  ss::sharded<kafka::group_router>& router,
  ss::sharded<cluster::shard_table>& tbl,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<fetch_session_cache>& session_cache,
  ss::sharded<cluster::id_allocator_frontend>& id_allocator_frontend,
  ss::sharded<security::credential_store>& credentials,
  ss::sharded<security::authorizer>& authorizer,
  ss::sharded<cluster::security_frontend>& sec_fe,
  ss::sharded<cluster::controller_api>& controller_api,
  ss::sharded<cluster::tx_gateway_frontend>& tx_gateway_frontend,
  ss::sharded<coproc::partition_manager>& coproc_partition_manager,
  ss::sharded<v8_engine::data_policy_table>& data_policy_table,
  std::optional<qdc_monitor::config> qdc_config) noexcept
  : _smp_group(smp)
  , _topics_frontend(tf)
  , _config_frontend(cf)
  , _feature_table(ft)
  , _metadata_cache(meta)
  , _quota_mgr(quota)
  , _group_router(router)
  , _shard_table(tbl)
  , _partition_manager(pm)
  , _fetch_session_cache(session_cache)
  , _id_allocator_frontend(id_allocator_frontend)
  , _is_idempotence_enabled(
      config::shard_local_cfg().enable_idempotence.value())
  , _are_transactions_enabled(
      config::shard_local_cfg().enable_transactions.value())
  , _credentials(credentials)
  , _authorizer(authorizer)
  , _security_frontend(sec_fe)
  , _controller_api(controller_api)
  , _tx_gateway_frontend(tx_gateway_frontend)
  , _coproc_partition_manager(coproc_partition_manager)
  , _data_policy_table(data_policy_table)
  , _mtls_principal_mapper(
      config::shard_local_cfg().kafka_mtls_principal_mapping_rules.bind()) {
    if (qdc_config) {
        _qdc_mon.emplace(*qdc_config);
    }
    _probe.setup_metrics();
    _probe.setup_public_metrics();
}

coordinator_ntp_mapper& protocol::coordinator_mapper() {
    return _group_router.local().coordinator_mapper().local();
}

config::broker_authn_method get_authn_method(const net::connection& conn) {
    // If authn_method is set on the endpoint
    //    Use it
    // Else if kafka_enable_authorization is not set
    //    Use sasl if enable_sasl
    // Else if has mtls mapping rules
    //    Use mtls_identity
    // Else
    //    Disable AuthN

    std::optional<config::broker_authn_method> authn_method;
    auto n = conn.name();
    const auto& kafka_api = config::node().kafka_api.value();
    auto ep_it = std::find_if(
      kafka_api.begin(),
      kafka_api.end(),
      [&n](const config::broker_authn_endpoint& ep) { return ep.name == n; });
    if (ep_it != kafka_api.end()) {
        authn_method = ep_it->authn_method;
    }
    if (authn_method.has_value()) {
        return *authn_method;
    }
    const auto& config = config::shard_local_cfg();
    // if kafka_enable_authorization is not set, use sasl iff enable_sasl
    if (
      !config.kafka_enable_authorization().has_value()
      && config.enable_sasl()) {
        return config::broker_authn_method::sasl;
    }
    return config::broker_authn_method::none;
}

ss::future<security::tls::mtls_state> get_mtls_principal_state(
  const security::tls::principal_mapper& pm, net::connection& conn) {
    using namespace std::chrono_literals;
    return ss::with_timeout(
             model::timeout_clock::now() + 5s, conn.get_distinguished_name())
      .then([&pm](std::optional<ss::session_dn> dn) {
          ss::sstring anonymous_principal;
          if (!dn.has_value()) {
              vlog(klog.info, "failed to fetch distinguished name");
              return security::tls::mtls_state{anonymous_principal};
          }
          auto principal = pm.apply(dn->subject);
          if (!principal) {
              vlog(
                klog.info,
                "failed to extract principal from distinguished name: {}",
                dn->subject);
              return security::tls::mtls_state{anonymous_principal};
          }

          vlog(
            klog.debug,
            "got principal: {}, from distinguished name: {}",
            *principal,
            dn->subject);
          return security::tls::mtls_state{*principal};
      });
}

ss::future<> protocol::apply(net::server::resources rs) {
    const bool authz_enabled
      = config::shard_local_cfg().kafka_enable_authorization().value_or(
        config::shard_local_cfg().enable_sasl());
    const auto authn_method = get_authn_method(*rs.conn);

    /*
     * if sasl authentication is not enabled then initialize the sasl state to
     * complete. this will cause auth to be skipped during request processing.
     */
    security::sasl_server sasl(
      authn_method == config::broker_authn_method::sasl
        ? security::sasl_server::sasl_state::initial
        : security::sasl_server::sasl_state::complete);

    std::optional<security::tls::mtls_state> mtls_state;
    if (authn_method == config::broker_authn_method::mtls_identity) {
        mtls_state = co_await get_mtls_principal_state(
          _mtls_principal_mapper, *rs.conn);
    }

    auto ctx = ss::make_lw_shared<connection_context>(
      *this, std::move(rs), std::move(sasl), authz_enabled, mtls_state);

    co_return co_await ss::do_until(
      [ctx] { return ctx->is_finished_parsing(); },
      [ctx] { return ctx->process_one_request(); })
      .handle_exception([ctx](std::exception_ptr eptr) {
          auto disconnected = net::is_disconnect_exception(eptr);
          if (config::shard_local_cfg().enable_sasl()) {
              /*
               * This block is a 2x2 matrix of:
               * - sasl enabled or disabled
               * - message looks like a disconnect or internal error
               *
               * Disconnects are logged at DEBUG level, because they are
               * already recorded at INFO level by the outer RPC layer,
               * so we don't want to log two INFO logs for each client
               * disconnect.
               */
              if (disconnected) {
                  vlog(
                    klog.debug,
                    "Disconnected {} {}:{} ({}, sasl state: {})",
                    ctx->server().name(),
                    ctx->client_host(),
                    ctx->client_port(),
                    disconnected.value(),
                    security::sasl_state_to_str(ctx->sasl().state()));

              } else {
                  vlog(
                    klog.warn,
                    "Error {} {}:{}: {} (sasl state: {})",
                    ctx->server().name(),
                    ctx->client_host(),
                    ctx->client_port(),
                    eptr,
                    security::sasl_state_to_str(ctx->sasl().state()));
              }
          } else {
              if (disconnected) {
                  vlog(
                    klog.debug,
                    "Disconnected {} {}:{} ({})",
                    ctx->server().name(),
                    ctx->client_host(),
                    ctx->client_port(),
                    disconnected.value());

              } else {
                  vlog(
                    klog.warn,
                    "Error {} {}:{}: {}",
                    ctx->server().name(),
                    ctx->client_host(),
                    ctx->client_port(),
                    eptr);
              }
          }
          return ss::make_exception_future(eptr);
      })
      .finally([ctx] {});
}

} // namespace kafka
