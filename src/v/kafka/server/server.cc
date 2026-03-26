// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "server.h"

#include "absl/container/flat_hash_map.h"
#include "base/vlog.h"
#include "cluster/cluster_link/frontend.h"
#include "cluster/cluster_link/types.h"
#include "cluster/id_allocator_frontend.h"
#include "cluster/security_frontend.h"
#include "cluster/topics_frontend.h"
#include "cluster/tx_gateway_frontend.h"
#include "cluster_link/model/filter_utils.h"
#include "config/broker_authn_endpoint.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "config/sasl_mechanisms.h"
#include "container/chunked_hash_map.h"
#include "features/enterprise_feature_messages.h"
#include "features/feature_table.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/produce.h"
#include "kafka/protocol/schemata/list_groups_response.h"
#include "kafka/server/connection_context.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/datalake_throttle_manager.h"
#include "kafka/server/errors.h"
#include "kafka/server/group.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/handlers/add_offsets_to_txn.h"
#include "kafka/server/handlers/add_partitions_to_txn.h"
#include "kafka/server/handlers/create_acls.h"
#include "kafka/server/handlers/delete_groups.h"
#include "kafka/server/handlers/delete_topics.h"
#include "kafka/server/handlers/describe_groups.h"
#include "kafka/server/handlers/details/security.h"
#include "kafka/server/handlers/end_txn.h"
#include "kafka/server/handlers/fetch/replica_selector.h"
#include "kafka/server/handlers/handler_interface.h"
#include "kafka/server/handlers/heartbeat.h"
#include "kafka/server/handlers/init_producer_id.h"
#include "kafka/server/handlers/join_group.h"
#include "kafka/server/handlers/leave_group.h"
#include "kafka/server/handlers/list_groups.h"
#include "kafka/server/handlers/list_transactions.h"
#include "kafka/server/handlers/offset_commit.h"
#include "kafka/server/handlers/offset_delete.h"
#include "kafka/server/handlers/offset_fetch.h"
#include "kafka/server/handlers/sasl_authenticate.h"
#include "kafka/server/handlers/sasl_handshake.h"
#include "kafka/server/handlers/sync_group.h"
#include "kafka/server/logger.h"
#include "kafka/server/quota_manager.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "kafka/server/usage_manager.h"
#include "model/record.h"
#include "net/connection.h"
#include "random/generators.h"
#include "security/acl.h"
#include "security/audit/schemas/iam.h"
#include "security/audit/schemas/utils.h"
#include "security/audit/types.h"
#include "security/errc.h"
#include "security/exceptions.h"
#include "security/gssapi_authenticator.h"
#include "security/mtls.h"
#include "security/oidc_authenticator.h"
#include "security/plain_authenticator.h"
#include "security/scram_algorithm.h"
#include "security/scram_authenticator.h"
#include "ssx/future-util.h"
#include "ssx/thread_worker.h"
#include "ssx/when_all.h"
#include "strings/string_switch.h"
#include "strings/utf8.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/log.hh>

#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <exception>
#include <iterator>
#include <limits>
#include <memory>
#include <ranges>
#include <vector>

namespace kafka {

namespace {
security::audit::authentication_event_options make_auth_event_options(
  const security::tls::mtls_state& mtls_state,
  const ss::lw_shared_ptr<connection_context>& ctx) {
    return {
    .auth_protocol = "mtls",
    .server_addr = {fmt::format("{}", ctx->local_address().addr()), ctx->local_address().port(), ctx->local_address().addr().in_family()},
    .svc_name = ctx->server().name(),
    .client_addr = {fmt::format("{}", ctx->client_host()), ctx->client_port()},
    .is_cleartext = security::audit::authentication::used_cleartext::no,
    .user = {
      .name = mtls_state.principal().name(),
      .type_id = security::audit::user::type::user,
      .uid = mtls_state.subject().value_or("")
    }
  };
}

} // namespace

server::server(
  ss::sharded<net::server_configuration>* cfg,
  ss::smp_service_group smp,
  ss::scheduling_group fetch_sg,
  ss::scheduling_group produce_sg,
  ss::scheduling_group handler_sg,
  ss::sharded<cluster::metadata_cache>& meta,
  ss::sharded<cluster::topics_frontend>& tf,
  ss::sharded<cluster::config_frontend>& cf,
  ss::sharded<features::feature_table>& ft,
  ss::sharded<cluster::client_quota::frontend>& quota_frontend,
  ss::sharded<cluster::client_quota::store>& quota_store,
  ss::sharded<quota_manager>& quota,
  ss::sharded<snc_quota_manager>& snc_quota_mgr,
  ss::sharded<kafka::group_router>& router,
  ss::sharded<kafka::usage_manager>& usage_manager,
  ss::sharded<cluster::shard_table>& tbl,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<cluster::id_allocator_frontend>& id_allocator_frontend,
  ss::sharded<security::credential_store>& credentials,
  ss::sharded<security::authorizer>& authorizer,
  ss::sharded<security::audit::audit_log_manager>& audit_mgr,
  ss::sharded<security::oidc::service>& oidc_service,
  ss::sharded<cluster::security_frontend>& sec_fe,
  ss::sharded<cluster::controller_api>& controller_api,
  ss::sharded<cluster::tx_gateway_frontend>& tx_gateway_frontend,
  ss::sharded<kafka::datalake_throttle_manager>& datalake_throttle_manager,
  ss::sharded<cluster::cluster_link::frontend>& clfe,
  std::optional<qdc_monitor_config> qdc_config,
  ssx::singleton_thread_worker& tw,
  const std::unique_ptr<pandaproxy::schema_registry::api>& sr) noexcept
  : net::server(cfg, klog)
  , _smp_group(smp)
  , _fetch_scheduling_group(fetch_sg)
  , _produce_scheduling_group(produce_sg)
  , _request_handler_scheduling_group(handler_sg)
  , _topics_frontend(tf)
  , _config_frontend(cf)
  , _feature_table(ft)
  , _metadata_cache(meta)
  , _quota_frontend(quota_frontend)
  , _quota_store(quota_store)
  , _quota_mgr(quota)
  , _snc_quota_mgr(snc_quota_mgr)
  , _group_router(router)
  , _usage_manager(usage_manager)
  , _shard_table(tbl)
  , _partition_manager(pm)
  , _fetch_pid_controller(fetch_sg)
  , _fetch_session_cache(
      config::shard_local_cfg().fetch_session_eviction_timeout_ms())
  , _id_allocator_frontend(id_allocator_frontend)
  , _is_idempotence_enabled(
      config::shard_local_cfg().enable_idempotence.value())
  , _are_transactions_enabled(
      config::shard_local_cfg().enable_transactions.value())
  , _recovery_mode_enabled(config::node().recovery_mode_enabled.value())
  , _credentials(credentials)
  , _authorizer(authorizer)
  , _audit_mgr(audit_mgr)
  , _oidc_service(oidc_service)
  , _security_frontend(sec_fe)
  , _controller_api(controller_api)
  , _tx_gateway_frontend(tx_gateway_frontend)
  , _datalake_throttle_manager(datalake_throttle_manager)
  , _cluster_link_frontend(clfe)
  , _mtls_principal_mapper(
      config::shard_local_cfg().kafka_mtls_principal_mapping_rules.bind())
  , _gssapi_principal_mapper(
      config::shard_local_cfg().sasl_kerberos_principal_mapping.bind())
  , _krb_configurator(config::shard_local_cfg().sasl_kerberos_config.bind())
  , _memory_fetch_sem(
      static_cast<size_t>(
        cfg->local().max_service_memory_per_core
        * config::shard_local_cfg().kafka_memory_share_for_fetch()),
      "kafka/server-mem-fetch")
  , _fetch_units_manager(
      memory(),
      memory_fetch_sem(),
      [this] -> fetch_memory_units_manager& {
          return container().local().fetch_units_manager();
      })
  , _probe(std::make_unique<class kafka_probe>())
  , _sasl_probe(std::make_unique<class sasl_probe>())
  , _read_dist_probe(std::make_unique<read_distribution_probe>())
  , _thread_worker(tw)
  , _replica_selector(
      std::make_unique<rack_aware_replica_selector>(_metadata_cache.local()))
  , _schema_registry(sr) {
    vlog(
      klog.debug,
      "Starting kafka server with {} byte limit on fetch requests",
      _memory_fetch_sem.available_units());
    if (qdc_config) {
        _qdc_mon.emplace(*qdc_config);
    }
    setup_metrics();
    _probe->setup_metrics();
    _probe->setup_public_metrics();

    _sasl_probe->setup_metrics(cfg->local().name);
    _read_dist_probe->setup_metrics();
    _fetch_metadata_cache.setup_metrics();
}
ss::future<> server::stop() {
    co_await net::server::stop();
    co_await _fetch_units_manager.stop();
}

bool server::is_cluster_link_active() const {
    const auto& clfe = _cluster_link_frontend.local();
    return clfe.cluster_linking_enabled() && clfe.cluster_link_active();
}

chunked_vector<ss::lw_shared_ptr<const connection_context>>
server::list_connections() const {
    using ret_t = chunked_vector<ss::lw_shared_ptr<const connection_context>>;
    return _connections | std::views::transform([](const auto& conn) {
               return conn.shared_from_this();
           })
           | std::ranges::to<ret_t>();
}

closed_connections_t server::list_closed_connections() const {
    return _closed_connections;
}

void server::setup_metrics() {
    namespace sm = ss::metrics;
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    _metrics.add_group(
      prometheus_sanitize::metrics_name(cfg.name),
      {
        sm::make_total_bytes(
          "fetch_avail_mem_bytes",
          [this] { return _memory_fetch_sem.current(); },
          sm::description(
            ssx::sformat(
              "{}: Memory available for fetch request processing", cfg.name))),
      });
}

ss::scheduling_group server::fetch_scheduling_group() const {
    return config::shard_local_cfg().use_fetch_scheduler_group()
             ? _fetch_scheduling_group
             : ss::default_scheduling_group();
}

ss::scheduling_group server::produce_scheduling_group() const {
    return config::shard_local_cfg().use_produce_scheduler_group()
             ? _produce_scheduling_group
             : ss::default_scheduling_group();
}

ss::scheduling_group server::get_request_handler_sg() const {
    return config::shard_local_cfg().use_kafka_handler_scheduler_group()
             ? _request_handler_scheduling_group
             : ss::default_scheduling_group();
}

coordinator_ntp_mapper& server::coordinator_mapper() {
    return _group_router.local().coordinator_mapper().local();
}

ss::future<security::tls::mtls_state> get_mtls_principal_state(
  const security::tls::principal_mapper& pm, net::connection& conn) {
    using namespace std::chrono_literals;
    auto format = [] {
        auto fmt = config::shard_local_cfg().tls_certificate_name_format();
        switch (fmt) {
        case config::tls_name_format::legacy:
            return ss::tls::dn_format::legacy;
        case config::tls_name_format::rfc2253:
            return ss::tls::dn_format::rfc2253;
        }
    }();
    return ss::with_timeout(
             model::timeout_clock::now() + 5s,
             conn.get_distinguished_name(format))
      .then([&pm](std::optional<ss::session_dn> dn) {
          ss::sstring anonymous_principal;
          if (!dn.has_value()) {
              vlog(klog.info, "failed to fetch distinguished name");
              return security::tls::mtls_state{
                anonymous_principal, std::nullopt};
          }
          auto principal = pm.apply(dn->subject);
          if (!principal) {
              vlog(
                klog.info,
                "failed to extract principal from distinguished name: {}",
                dn->subject);
              return security::tls::mtls_state{
                anonymous_principal, dn->subject};
          }

          vlog(
            klog.debug,
            "got principal: {}, from distinguished name: {}",
            *principal,
            dn->subject);
          return security::tls::mtls_state{*principal, dn->subject};
      });
}

/*static*/ std::vector<bool> server::convert_api_names_to_key_bitmap(
  const std::vector<ss::sstring>& api_names) {
    std::vector<bool> res;
    res.resize(max_api_key() + 1);
    for (const ss::sstring& api_name : api_names) {
        if (const auto api_key = api_name_to_key(api_name); api_key) {
            res.at(*api_key) = true;
            continue;
        }
        vlog(klog.warn, "Unrecognized Kafka API name: {}", api_name);
    }
    return res;
}

ss::future<> server::apply(ss::lw_shared_ptr<net::connection> conn) {
    const bool authz_enabled = config::kafka_authz_enabled();
    const auto authn_method = config::get_authn_method(conn->name());

    const auto sasl_max_reauth
      = config::shard_local_cfg().kafka_sasl_max_reauth_ms();

    vlog(
      klog.debug,
      "max_reauth_ms: {}",
      sasl_max_reauth.value_or(std::chrono::milliseconds{0}));

    // Only initialise sasl state if sasl is enabled
    auto sasl = authn_method == config::broker_authn_method::sasl
                  ? std::make_optional<security::sasl_server>(
                      security::sasl_server::sasl_state::initial,
                      sasl_max_reauth)
                  : std::nullopt;

    // Only initialise mtls state if mtls_identity is enabled
    std::optional<security::tls::mtls_state> mtls_state;
    if (authn_method == config::broker_authn_method::mtls_identity) {
        mtls_state = co_await get_mtls_principal_state(
          _mtls_principal_mapper, *conn);
    }

    auto ctx = ss::make_lw_shared<connection_context>(
      _connections,
      _closed_connections,
      *this,
      conn,
      std::move(sasl),
      authz_enabled,
      mtls_state,
      config::shard_local_cfg().kafka_request_max_bytes.bind(),
      config::shard_local_cfg()
        .kafka_throughput_controlled_api_keys.bind<std::vector<bool>>(
          &convert_api_names_to_key_bitmap));

    std::exception_ptr eptr;
    try {
        co_await ctx->start();
        // Must call start() to ensure `ctx` is inserted into the `_connections`
        // list.  Otherwise if enqueing the audit message fails and `stop()` is
        // called, this will result in a segfault.
        if (authn_method == config::broker_authn_method::mtls_identity) {
            auto authn_event = make_auth_event_options(mtls_state.value(), ctx);
            if (!ctx->server().audit_mgr().enqueue_authn_event(
                  std::move(authn_event))) {
                throw std::runtime_error(
                  "Failed to enqueue mTLS authentication event - audit log "
                  "system error");
            }
        }
        co_await ctx->process();
    } catch (...) {
        eptr = std::current_exception();
    }
    if (!eptr) {
        co_return co_await ctx->stop();
    } else {
        co_await ctx->abort_source().request_abort_ex(eptr);
        co_await ctx->stop();
        auto disconnected = net::is_disconnect_exception(eptr);
        if (authn_method == config::broker_authn_method::sasl) {
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
                  security::sasl_state_to_str(ctx->sasl()->state()));

            } else {
                vlog(
                  klog.warn,
                  "Error {} {}:{}: {} (sasl state: {})",
                  ctx->server().name(),
                  ctx->client_host(),
                  ctx->client_port(),
                  eptr,
                  security::sasl_state_to_str(ctx->sasl()->state()));
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
        std::rethrow_exception(eptr);
    }
}

void server::mark_datalake_producer(
  const std::optional<std::string_view>& client_id) {
    if (
      !config::shard_local_cfg().iceberg_enabled()
      || !_datalake_throttle_manager.local_is_initialized()) {
        return;
    }
    _datalake_throttle_manager.local().mark_datalake_producer(client_id);
}

ss::future<std::chrono::milliseconds> server::get_datalake_producer_throttle(
  std::optional<std::string_view> client_id) {
    if (
      !config::shard_local_cfg().iceberg_enabled()
      || !_datalake_throttle_manager.local_is_initialized()) {
        return ssx::now<std::chrono::milliseconds>(0ms);
    }

    return _datalake_throttle_manager.local().maybe_throttle_producer(
      client_id);
}

ss::future<> server::revoke_credentials(std::string_view name) {
    constexpr size_t max_concurrency{100};
    return ss::max_concurrent_for_each(
      _connections, max_concurrency, [name = ss::sstring{name}](auto& ctx) {
          return ctx.revoke_credentials(name);
      });
}

} // namespace kafka
