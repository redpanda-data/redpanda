// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "server.h"

#include "cluster/id_allocator_frontend.h"
#include "cluster/topics_frontend.h"
#include "config/broker_authn_endpoint.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "features/feature_table.h"
#include "kafka/server/connection_context.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/errors.h"
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
#include "net/connection.h"
#include "security/errc.h"
#include "security/exceptions.h"
#include "security/gssapi_authenticator.h"
#include "security/mtls.h"
#include "security/scram_algorithm.h"
#include "security/scram_authenticator.h"
#include "ssx/future-util.h"
#include "ssx/thread_worker.h"
#include "utils/utf8.h"
#include "vlog.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/log.hh>

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_map.h>
#include <fmt/format.h>

#include <chrono>
#include <exception>
#include <limits>
#include <memory>
#include <vector>

namespace kafka {

server::server(
  ss::sharded<net::server_configuration>* cfg,
  ss::smp_service_group smp,
  ss::scheduling_group fetch_sg,
  ss::sharded<cluster::metadata_cache>& meta,
  ss::sharded<cluster::topics_frontend>& tf,
  ss::sharded<cluster::config_frontend>& cf,
  ss::sharded<features::feature_table>& ft,
  ss::sharded<quota_manager>& quota,
  ss::sharded<snc_quota_manager>& snc_quota_mgr,
  ss::sharded<kafka::group_router>& router,
  ss::sharded<kafka::usage_manager>& usage_manager,
  ss::sharded<cluster::shard_table>& tbl,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<cluster::id_allocator_frontend>& id_allocator_frontend,
  ss::sharded<security::credential_store>& credentials,
  ss::sharded<security::authorizer>& authorizer,
  ss::sharded<cluster::security_frontend>& sec_fe,
  ss::sharded<cluster::controller_api>& controller_api,
  ss::sharded<cluster::tx_gateway_frontend>& tx_gateway_frontend,
  ss::sharded<cluster::tx_registry_frontend>& tx_registry_frontend,
  std::optional<qdc_monitor::config> qdc_config,
  ssx::thread_worker& tw,
  const std::unique_ptr<pandaproxy::schema_registry::api>& sr) noexcept
  : net::server(cfg, klog)
  , _smp_group(smp)
  , _fetch_scheduling_group(fetch_sg)
  , _topics_frontend(tf)
  , _config_frontend(cf)
  , _feature_table(ft)
  , _metadata_cache(meta)
  , _quota_mgr(quota)
  , _snc_quota_mgr(snc_quota_mgr)
  , _group_router(router)
  , _usage_manager(usage_manager)
  , _shard_table(tbl)
  , _partition_manager(pm)
  , _fetch_session_cache(
      config::shard_local_cfg().fetch_session_eviction_timeout_ms())
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
  , _tx_registry_frontend(tx_registry_frontend)
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
    _probe.setup_metrics();
    _probe.setup_public_metrics();
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
          sm::description(ssx::sformat(
            "{}: Memory available for fetch request processing", cfg.name))),
      });
}

ss::scheduling_group server::fetch_scheduling_group() const {
    return config::shard_local_cfg().use_fetch_scheduler_group()
             ? _fetch_scheduling_group
             : ss::default_scheduling_group();
}

coordinator_ntp_mapper& server::coordinator_mapper() {
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
    const bool authz_enabled
      = config::shard_local_cfg().kafka_enable_authorization().value_or(
        config::shard_local_cfg().enable_sasl());
    const auto authn_method = get_authn_method(*conn);

    // Only initialise sasl state if sasl is enabled
    auto sasl = authn_method == config::broker_authn_method::sasl
                  ? std::make_optional<security::sasl_server>(
                    security::sasl_server::sasl_state::initial)
                  : std::nullopt;

    // Only initialise mtls state if mtls_identity is enabled
    std::optional<security::tls::mtls_state> mtls_state;
    if (authn_method == config::broker_authn_method::mtls_identity) {
        mtls_state = co_await get_mtls_principal_state(
          _mtls_principal_mapper, *conn);
    }

    auto ctx = ss::make_lw_shared<connection_context>(
      *this,
      conn,
      std::move(sasl),
      authz_enabled,
      mtls_state,
      config::shard_local_cfg().kafka_request_max_bytes.bind(),
      config::shard_local_cfg()
        .kafka_throughput_controlled_api_keys.bind<std::vector<bool>>(
          &convert_api_names_to_key_bitmap));

    try {
        co_await ctx->process();
    } catch (...) {
        auto eptr = std::current_exception();
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

template<>
ss::future<response_ptr> heartbeat_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    heartbeat_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    if (!ctx.authorized(security::acl_operation::read, request.data.group_id)) {
        co_return co_await ctx.respond(
          heartbeat_response(error_code::group_authorization_failed));
    }

    auto resp = co_await ctx.groups().heartbeat(std::move(request));
    co_return co_await ctx.respond(resp);
}

template<>
ss::future<response_ptr> sasl_authenticate_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    sasl_authenticate_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);
    vlog(klog.debug, "Received SASL_AUTHENTICATE {}", request);

    auto result = co_await ctx.sasl()->authenticate(
      std::move(request.data.auth_bytes));
    if (likely(result)) {
        sasl_authenticate_response_data data{
          .error_code = error_code::none,
          .error_message = std::nullopt,
          .auth_bytes = std::move(result.value()),
        };
        co_return co_await ctx.respond(
          sasl_authenticate_response(std::move(data)));
    }

    sasl_authenticate_response_data data{
      .error_code = error_code::sasl_authentication_failed,
      .error_message = ssx::sformat(
        "SASL authentication failed: {}", result.error().message()),
    };
    co_return co_await ctx.respond(sasl_authenticate_response(std::move(data)));
}

template<>
process_result_stages sync_group_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    sync_group_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    if (!ctx.authorized(security::acl_operation::read, request.data.group_id)) {
        return process_result_stages::single_stage(ctx.respond(
          sync_group_response(error_code::group_authorization_failed)));
    }

    auto stages = ctx.groups().sync_group(std::move(request));
    auto res = ss::do_with(
      std::move(ctx),
      [f = std::move(stages.result)](request_context& ctx) mutable {
          return f.then([&ctx](sync_group_response response) {
              return ctx.respond(std::move(response));
          });
      });

    return process_result_stages(std::move(stages.dispatched), std::move(res));
}

template<>
ss::future<response_ptr> leave_group_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    leave_group_request request;
    request.decode(ctx.reader(), ctx.header().version);
    request.version = ctx.header().version;
    log_request(ctx.header(), request);

    if (!ctx.authorized(security::acl_operation::read, request.data.group_id)) {
        co_return co_await ctx.respond(
          leave_group_response(error_code::group_authorization_failed));
    }

    auto resp = co_await ctx.groups().leave_group(std::move(request));
    co_return co_await ctx.respond(std::move(resp));
}

template<>
ss::future<response_ptr> list_groups_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    list_groups_request request{};
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);
    auto&& [error, groups] = co_await ctx.groups().list_groups();

    list_groups_response resp;
    resp.data.error_code = error;
    resp.data.groups = std::move(groups);

    if (ctx.authorized(
          security::acl_operation::describe, security::default_cluster_name)) {
        co_return co_await ctx.respond(std::move(resp));
    }

    // remove groups from response that should not be visible
    auto non_visible_it = std::partition(
      resp.data.groups.begin(),
      resp.data.groups.end(),
      [&ctx](const listed_group& group) {
          return ctx.authorized(
            security::acl_operation::describe, group.group_id);
      });

    resp.data.groups.erase(non_visible_it, resp.data.groups.end());

    co_return co_await ctx.respond(std::move(resp));
}

template<>
ss::future<response_ptr> sasl_handshake_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    sasl_handshake_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);
    vlog(klog.debug, "Received SASL_HANDSHAKE {}", request);

    const auto& configured = config::shard_local_cfg().sasl_mechanisms();
    auto supports = [&configured](std::string_view value) {
        return absl::c_find(configured, value) != configured.end();
    };

    /*
     * configure sasl for the current connection context. see the sasl
     * authenticate request for the next phase of the auth process.
     */
    auto error = error_code::none;

    std::vector<ss::sstring> supported_sasl_mechanisms;
    if (supports("SCRAM")) {
        supported_sasl_mechanisms.insert(
          supported_sasl_mechanisms.end(),
          {security::scram_sha256_authenticator::name,
           security::scram_sha512_authenticator::name});

        if (
          request.data.mechanism
          == security::scram_sha256_authenticator::name) {
            ctx.sasl()->set_mechanism(
              std::make_unique<security::scram_sha256_authenticator::auth>(
                ctx.credentials()));

        } else if (
          request.data.mechanism
          == security::scram_sha512_authenticator::name) {
            ctx.sasl()->set_mechanism(
              std::make_unique<security::scram_sha512_authenticator::auth>(
                ctx.credentials()));
        }
    }

    const bool has_kafka_gssapi = ctx.feature_table().local().is_active(
      features::feature::kafka_gssapi);
    if (has_kafka_gssapi && supports("GSSAPI")) {
        supported_sasl_mechanisms.emplace_back(
          security::gssapi_authenticator::name);

        if (request.data.mechanism == security::gssapi_authenticator::name) {
            ctx.sasl()->set_mechanism(
              std::make_unique<security::gssapi_authenticator>(
                ctx.connection()->server().thread_worker(),
                ctx.connection()->server().gssapi_principal_mapper().rules(),
                config::shard_local_cfg().sasl_kerberos_principal(),
                config::shard_local_cfg().sasl_kerberos_keytab()));
        }
    }

    if (!ctx.sasl()->has_mechanism()) {
        error = error_code::unsupported_sasl_mechanism;
    }

    return ctx.respond(
      sasl_handshake_response(error, std::move(supported_sasl_mechanisms)));
}

template<>
process_result_stages join_group_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    join_group_request request;
    request.decode(ctx.reader(), ctx.header().version);
    request.version = ctx.header().version;
    if (ctx.header().client_id) {
        request.client_id = kafka::client_id(
          ss::sstring(*ctx.header().client_id));
    }
    request.client_host = kafka::client_host(
      fmt::format("{}", ctx.connection()->client_host()));
    log_request(ctx.header(), request);

    if (!ctx.authorized(security::acl_operation::read, request.data.group_id)) {
        return process_result_stages::single_stage(ctx.respond(
          join_group_response(error_code::group_authorization_failed)));
    }

    auto stages = ctx.groups().join_group(std::move(request));
    auto res = ss::do_with(
      std::move(ctx),
      [f = std::move(stages.result)](request_context& ctx) mutable {
          return f.then([&ctx](join_group_response response) {
              return ctx.respond(std::move(response));
          });
      });

    return process_result_stages(std::move(stages.dispatched), std::move(res));
}

template<>
ss::future<response_ptr> delete_groups_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    delete_groups_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    auto unauthorized_it = std::partition(
      request.data.groups_names.begin(),
      request.data.groups_names.end(),
      [&ctx](const kafka::group_id& group) {
          return ctx.authorized(security::acl_operation::remove, group);
      });

    std::vector<kafka::group_id> unauthorized(
      std::make_move_iterator(unauthorized_it),
      std::make_move_iterator(request.data.groups_names.end()));

    request.data.groups_names.erase(
      unauthorized_it, request.data.groups_names.end());

    std::vector<deletable_group_result> results;

    if (!request.data.groups_names.empty()) {
        results = co_await ctx.groups().delete_groups(
          std::move(request.data.groups_names));
    }

    for (auto& group : unauthorized) {
        results.push_back(deletable_group_result{
          .group_id = std::move(group),
          .error_code = error_code::group_authorization_failed,
        });
    }

    co_return co_await ctx.respond(delete_groups_response(std::move(results)));
}

template<>
ss::future<response_ptr> end_txn_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    return ss::do_with(std::move(ctx), [](request_context& ctx) {
        end_txn_request request;
        request.decode(ctx.reader(), ctx.header().version);
        log_request(ctx.header(), request);
        cluster::end_tx_request tx_request{
          .transactional_id = request.data.transactional_id,
          .producer_id = request.data.producer_id,
          .producer_epoch = request.data.producer_epoch,
          .committed = request.data.committed};
        return ctx.tx_gateway_frontend()
          .end_txn(
            tx_request, config::shard_local_cfg().create_topic_timeout_ms())
          .then([&ctx](cluster::end_tx_reply tx_response) {
              end_txn_response_data data;
              switch (tx_response.error_code) {
              case cluster::tx_errc::none:
                  data.error_code = error_code::none;
                  break;
              case cluster::tx_errc::not_coordinator:
                  data.error_code = error_code::not_coordinator;
                  break;
              case cluster::tx_errc::coordinator_not_available:
                  data.error_code = error_code::coordinator_not_available;
                  break;
              case cluster::tx_errc::fenced:
                  data.error_code = error_code::invalid_producer_epoch;
                  break;
              case cluster::tx_errc::invalid_producer_id_mapping:
                  data.error_code = error_code::invalid_producer_id_mapping;
                  break;
              case cluster::tx_errc::invalid_txn_state:
                  data.error_code = error_code::invalid_txn_state;
                  break;
              case cluster::tx_errc::timeout:
                  data.error_code = error_code::request_timed_out;
                  break;
              default:
                  data.error_code = error_code::unknown_server_error;
                  break;
              }
              end_txn_response response;
              response.data = data;
              return ctx.respond(response);
          });
    });
}

template<>
ss::future<response_ptr>
add_offsets_to_txn_handler::handle(request_context ctx, ss::smp_service_group) {
    return ss::do_with(std::move(ctx), [](request_context& ctx) {
        add_offsets_to_txn_request request;
        request.decode(ctx.reader(), ctx.header().version);
        log_request(ctx.header(), request);

        cluster::add_offsets_tx_request tx_request{
          .transactional_id = request.data.transactional_id,
          .producer_id = request.data.producer_id,
          .producer_epoch = request.data.producer_epoch,
          .group_id = request.data.group_id};

        auto f = ctx.tx_gateway_frontend().add_offsets_to_tx(
          tx_request, config::shard_local_cfg().create_topic_timeout_ms());

        return f.then([&ctx](cluster::add_offsets_tx_reply tx_response) {
            add_offsets_to_txn_response_data data;
            switch (tx_response.error_code) {
            case cluster::tx_errc::none:
                data.error_code = error_code::none;
                break;
            case cluster::tx_errc::not_coordinator:
                data.error_code = error_code::not_coordinator;
                break;
            case cluster::tx_errc::coordinator_not_available:
                data.error_code = error_code::coordinator_not_available;
                break;
            case cluster::tx_errc::coordinator_load_in_progress:
                data.error_code = error_code::coordinator_load_in_progress;
                break;
            case cluster::tx_errc::invalid_producer_id_mapping:
                data.error_code = error_code::invalid_producer_id_mapping;
                break;
            case cluster::tx_errc::fenced:
                data.error_code = error_code::invalid_producer_epoch;
                break;
            case cluster::tx_errc::invalid_txn_state:
                data.error_code = error_code::invalid_txn_state;
                break;
            default:
                data.error_code = error_code::unknown_server_error;
                break;
            }
            add_offsets_to_txn_response res;
            res.data = data;
            return ctx.respond(res);
        });
    });
}

template<>
ss::future<response_ptr> add_partitions_to_txn_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    return ss::do_with(std::move(ctx), [](request_context& ctx) {
        add_partitions_to_txn_request request;
        request.decode(ctx.reader(), ctx.header().version);
        log_request(ctx.header(), request);

        cluster::add_paritions_tx_request tx_request{
          .transactional_id = request.data.transactional_id,
          .producer_id = request.data.producer_id,
          .producer_epoch = request.data.producer_epoch};
        tx_request.topics.reserve(request.data.topics.size());
        for (auto& topic : request.data.topics) {
            cluster::add_paritions_tx_request::topic tx_topic{
              .name = std::move(topic.name),
              .partitions = std::move(topic.partitions),
            };
            tx_request.topics.push_back(tx_topic);
        }

        return ctx.tx_gateway_frontend()
          .add_partition_to_tx(
            tx_request, config::shard_local_cfg().create_topic_timeout_ms())
          .then([&ctx](cluster::add_paritions_tx_reply tx_response) {
              add_partitions_to_txn_response_data data;
              for (auto& tx_topic : tx_response.results) {
                  add_partitions_to_txn_topic_result topic{
                    .name = std::move(tx_topic.name),
                  };
                  for (const auto& tx_partition : tx_topic.results) {
                      add_partitions_to_txn_partition_result partition{
                        .partition_index = tx_partition.partition_index};
                      switch (tx_partition.error_code) {
                      case cluster::tx_errc::none:
                          partition.error_code = error_code::none;
                          break;
                      case cluster::tx_errc::not_coordinator:
                          partition.error_code = error_code::not_coordinator;
                          break;
                      case cluster::tx_errc::coordinator_not_available:
                          partition.error_code
                            = error_code::coordinator_not_available;
                          break;
                      case cluster::tx_errc::invalid_producer_id_mapping:
                          partition.error_code
                            = error_code::invalid_producer_id_mapping;
                          break;
                      case cluster::tx_errc::fenced:
                          partition.error_code
                            = error_code::invalid_producer_epoch;
                          break;
                      case cluster::tx_errc::invalid_txn_state:
                          partition.error_code = error_code::invalid_txn_state;
                          break;
                      case cluster::tx_errc::timeout:
                          partition.error_code = error_code::request_timed_out;
                          break;
                      default:
                          partition.error_code
                            = error_code::unknown_server_error;
                          break;
                      }
                      topic.results.push_back(partition);
                  }
                  data.results.push_back(std::move(topic));
              }

              add_partitions_to_txn_response response;
              response.data = data;
              return ctx.respond(std::move(response));
          });
    });
}

template<>
ss::future<response_ptr>
offset_fetch_handler::handle(request_context ctx, ss::smp_service_group) {
    offset_fetch_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);
    if (!ctx.authorized(
          security::acl_operation::describe, request.data.group_id)) {
        co_return co_await ctx.respond(
          offset_fetch_response(error_code::group_authorization_failed));
    }

    /*
     * request is for all group offsets
     */
    if (!request.data.topics) {
        auto resp = co_await ctx.groups().offset_fetch(std::move(request));
        if (resp.data.error_code != error_code::none) {
            co_return co_await ctx.respond(std::move(resp));
        }

        // remove unauthorized topics from response
        auto unauthorized = std::partition(
          resp.data.topics.begin(),
          resp.data.topics.end(),
          [&ctx](const offset_fetch_response_topic& topic) {
              /*
               * quiet authz failures. this is checking for visibility across
               * all topics not specifically requested topics.
               */
              return ctx.authorized(
                security::acl_operation::describe,
                topic.name,
                authz_quiet{true});
          });
        resp.data.topics.erase(unauthorized, resp.data.topics.end());

        co_return co_await ctx.respond(std::move(resp));
    }

    /*
     * pre-filter authorized topics in request
     */
    auto unauthorized_it = std::partition(
      request.data.topics->begin(),
      request.data.topics->end(),
      [&ctx](const offset_fetch_request_topic& topic) {
          return ctx.authorized(security::acl_operation::describe, topic.name);
      });

    std::vector<offset_fetch_request_topic> unauthorized(
      std::make_move_iterator(unauthorized_it),
      std::make_move_iterator(request.data.topics->end()));

    // remove unauthorized topics from request
    request.data.topics->erase(unauthorized_it, request.data.topics->end());
    auto resp = co_await ctx.groups().offset_fetch(std::move(request));

    // add requested (but unauthorized) topics into response
    for (auto& req_topic : unauthorized) {
        auto& topic = resp.data.topics.emplace_back();
        topic.name = std::move(req_topic.name);
        topic.partitions.reserve(req_topic.partition_indexes.size());
        for (auto partition_index : req_topic.partition_indexes) {
            auto& partition = topic.partitions.emplace_back();
            partition.partition_index = partition_index;
            partition.error_code = error_code::group_authorization_failed;
        }
    }

    co_return co_await ctx.respond(std::move(resp));
}

template<>
ss::future<response_ptr>
offset_delete_handler::handle(request_context ctx, ss::smp_service_group) {
    offset_delete_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);
    if (!ctx.authorized(
          security::acl_operation::remove, request.data.group_id)) {
        co_return co_await ctx.respond(
          offset_fetch_response(error_code::group_authorization_failed));
    }

    /// Remove unauthorized topics from request
    auto unauthorized_it = std::partition(
      request.data.topics.begin(),
      request.data.topics.end(),
      [&ctx](const offset_delete_request_topic& topic) {
          return ctx.authorized(security::acl_operation::read, topic.name);
      });

    std::vector<offset_delete_request_topic> unauthorized(
      std::make_move_iterator(unauthorized_it),
      std::make_move_iterator(request.data.topics.end()));
    request.data.topics.erase(unauthorized_it, request.data.topics.end());

    /// Remove unknown topic_partitions from request
    std::vector<offset_delete_request_topic> unknowns;
    for (auto& topic : request.data.topics) {
        auto unknowns_it = std::partition(
          topic.partitions.begin(),
          topic.partitions.end(),
          [&topic, &ctx](const offset_delete_request_partition& partition) {
              return ctx.metadata_cache().contains(
                model::topic_namespace_view(model::kafka_namespace, topic.name),
                partition.partition_index);
          });
        if (std::distance(unknowns_it, topic.partitions.end()) > 0) {
            unknowns.push_back(offset_delete_request_topic{
              .name = topic.name,
              .partitions = std::vector<offset_delete_request_partition>(
                std::make_move_iterator(unknowns_it),
                std::make_move_iterator(topic.partitions.end()))});
            topic.partitions.erase(unknowns_it, topic.partitions.end());
        }
    }

    auto resp = co_await ctx.groups().offset_delete(std::move(request));

    /// Re-add unauthorized requests back into the response as errors
    for (auto& req_topic : unauthorized) {
        auto& topic = resp.data.topics.emplace_back();
        topic.name = std::move(req_topic.name);
        topic.partitions.reserve(req_topic.partitions.size());
        for (auto& req_part : req_topic.partitions) {
            auto& partition = topic.partitions.emplace_back();
            partition.partition_index = req_part.partition_index;
            partition.error_code = error_code::topic_authorization_failed;
        }
    }

    /// Re-add unknown requests back into the response as errors
    for (auto& req_topic : unknowns) {
        auto found = std::find_if(
          resp.data.topics.begin(),
          resp.data.topics.end(),
          [&req_topic](const offset_delete_response_topic& resp_topic) {
              return resp_topic.name == req_topic.name;
          });
        auto& topic = (found == resp.data.topics.end())
                        ? resp.data.topics.emplace_back()
                        : *found;
        topic.name = std::move(req_topic.name);
        for (auto& req_part : req_topic.partitions) {
            auto& partition = topic.partitions.emplace_back();
            partition.partition_index = req_part.partition_index;
            partition.error_code = error_code::unknown_topic_or_partition;
        }
    }

    co_return co_await ctx.respond(std::move(resp));
}

template<>
ss::future<response_ptr>
delete_topics_handler::handle(request_context ctx, ss::smp_service_group) {
    delete_topics_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    auto unauthorized_it = std::partition(
      request.data.topic_names.begin(),
      request.data.topic_names.end(),
      [&ctx](const model::topic& topic) {
          return ctx.authorized(security::acl_operation::remove, topic);
      });

    std::vector<model::topic> unauthorized(
      std::make_move_iterator(unauthorized_it),
      std::make_move_iterator(request.data.topic_names.end()));

    request.data.topic_names.erase(
      unauthorized_it, request.data.topic_names.end());

    const auto& kafka_nodelete_topics
      = config::shard_local_cfg().kafka_nodelete_topics();
    auto nodelete_it = std::partition(
      request.data.topic_names.begin(),
      request.data.topic_names.end(),
      [&kafka_nodelete_topics](const model::topic& topic) {
          return std::find(
                   kafka_nodelete_topics.begin(),
                   kafka_nodelete_topics.end(),
                   topic)
                 == kafka_nodelete_topics.end();
      });
    std::vector<model::topic> nodelete_topics(
      std::make_move_iterator(nodelete_it),
      std::make_move_iterator(request.data.topic_names.end()));

    request.data.topic_names.erase(nodelete_it, request.data.topic_names.end());

    std::vector<cluster::topic_result> res;

    // Measure the partition mutation rate
    auto resp_delay = 0ms;
    auto quota_exceeded_it = co_await ssx::partition(
      request.data.topic_names.begin(),
      request.data.topic_names.end(),
      [&ctx, &resp_delay](const model::topic& t) {
          const auto cfg = ctx.metadata_cache().get_topic_cfg(
            model::topic_namespace_view(model::kafka_namespace, t));
          const auto mutations = cfg ? cfg->partition_count : 0;
          /// Capture before next scheduling point below
          auto& resp_delay_ref = resp_delay;
          return ctx.quota_mgr()
            .record_partition_mutations(ctx.header().client_id, mutations)
            .then([&resp_delay_ref](std::chrono::milliseconds delay) {
                resp_delay_ref = std::max(delay, resp_delay_ref);
                return delay == 0ms;
            });
      });

    std::vector<model::topic> quota_exceeded(
      std::make_move_iterator(quota_exceeded_it),
      std::make_move_iterator(request.data.topic_names.end()));

    request.data.topic_names.erase(
      quota_exceeded_it, request.data.topic_names.end());

    if (!request.data.topic_names.empty()) {
        // construct namespaced topic set from request
        std::vector<model::topic_namespace> topics;
        topics.reserve(request.data.topic_names.size());
        std::transform(
          std::begin(request.data.topic_names),
          std::end(request.data.topic_names),
          std::back_inserter(topics),
          [](model::topic& tp) {
              return model::topic_namespace(
                model::kafka_namespace, std::move(tp));
          });
        auto tout = request.data.timeout_ms + model::timeout_clock::now();
        res = co_await ctx.topics_frontend().delete_topics(
          std::move(topics), tout);
    }

    // initialize response with topic placeholders
    delete_topics_response resp;
    resp.data.responses.reserve(res.size());
    std::transform(
      res.begin(),
      res.end(),
      std::back_inserter(resp.data.responses),
      [](cluster::topic_result tr) {
          return deletable_topic_result{
            .name = std::move(tr.tp_ns.tp),
            .error_code = map_topic_error_code(tr.ec)};
      });

    resp.data.throttle_time_ms = resp_delay;
    for (auto& topic : unauthorized) {
        resp.data.responses.push_back(deletable_topic_result{
          .name = std::move(topic),
          .error_code = error_code::topic_authorization_failed,
        });
    }

    for (auto& topic : quota_exceeded) {
        /// The throttling_quota_exceeded code is only respected by newer
        /// clients, older clients will observe a different failure and be
        /// throttled at the connection layer
        const auto ec = (ctx.header().version >= api_version(5))
                          ? error_code::throttling_quota_exceeded
                          : error_code::unknown_server_error;
        resp.data.responses.push_back(deletable_topic_result{
          .name = std::move(topic),
          .error_code = ec,
          .error_message = "Too many partition mutations requested"});
    }

    co_return co_await ctx.respond(std::move(resp));
}

template<>
ss::future<response_ptr> init_producer_id_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    return ss::do_with(std::move(ctx), [](request_context& ctx) {
        init_producer_id_request request;
        request.decode(ctx.reader(), ctx.header().version);
        log_request(ctx.header(), request);

        if (request.data.transactional_id) {
            if (!ctx.authorized(
                  security::acl_operation::write,
                  transactional_id(*request.data.transactional_id))) {
                init_producer_id_response reply;
                reply.data.error_code
                  = error_code::transactional_id_authorization_failed;
                return ctx.respond(reply);
            }

            model::producer_identity expected_pid = model::producer_identity{
              request.data.producer_id, request.data.producer_epoch};

            // Provided pid in init_producer_id request can not be {x >= 0, -1}
            // or {-1, x >= 0}.
            const bool is_invalid_pid =
              [](model::producer_identity expected_pid) {
                  if (expected_pid == model::unknown_pid) {
                      return false;
                  }

                  if (expected_pid.id < 0 || expected_pid.epoch < 0) {
                      return true;
                  }
                  return false;
              }(expected_pid);

            if (is_invalid_pid) {
                init_producer_id_response reply;
                reply.data.error_code = error_code::invalid_request;
                return ctx.respond(reply);
            }

            return ctx.tx_gateway_frontend()
              .init_tm_tx(
                request.data.transactional_id.value(),
                request.data.transaction_timeout_ms,
                config::shard_local_cfg().create_topic_timeout_ms(),
                expected_pid)
              .then([&ctx](cluster::init_tm_tx_reply r) {
                  init_producer_id_response reply;

                  switch (r.ec) {
                  case cluster::tx_errc::none:
                      reply.data.producer_id = kafka::producer_id(r.pid.id);
                      reply.data.producer_epoch = r.pid.epoch;
                      vlog(
                        klog.trace,
                        "allocated pid {} with epoch {} via tx_gateway",
                        reply.data.producer_id,
                        reply.data.producer_epoch);
                      break;
                  case cluster::tx_errc::invalid_txn_state:
                      reply.data.error_code = error_code::invalid_txn_state;
                      break;
                  case cluster::tx_errc::not_coordinator:
                      reply.data.error_code = error_code::not_coordinator;
                      break;
                  case cluster::tx_errc::invalid_producer_epoch:
                      reply.data.error_code
                        = error_code::invalid_producer_epoch;
                      break;
                  case cluster::tx_errc::timeout:
                      reply.data.error_code = error_code::request_timed_out;
                      break;
                  case cluster::tx_errc::shard_not_found:
                      reply.data.error_code = error_code::not_coordinator;
                      break;
                  default:
                      vlog(klog.warn, "failed to allocate pid, ec: {}", r.ec);
                      reply.data.error_code = error_code::broker_not_available;
                      break;
                  }

                  return ctx.respond(reply);
              });
        }

        bool permitted = false;
        auto topics = ctx.metadata_cache().all_topics();
        for (auto& tp_ns : topics) {
            permitted = ctx.authorized(
              security::acl_operation::write, tp_ns.tp, authz_quiet{true});
            if (permitted) {
                break;
            }
        }

        if (!permitted) {
            if (!ctx.authorized(
                  security::acl_operation::idempotent_write,
                  security::default_cluster_name)) {
                init_producer_id_response reply;
                reply.data.error_code
                  = error_code::cluster_authorization_failed;
                return ctx.respond(reply);
            }
        }

        return ctx.id_allocator_frontend()
          .allocate_id(config::shard_local_cfg().create_topic_timeout_ms())
          .then([&ctx](cluster::allocate_id_reply r) {
              init_producer_id_response reply;

              if (r.ec == cluster::errc::success) {
                  reply.data.producer_id = kafka::producer_id(r.id);
                  reply.data.producer_epoch = 0;
                  vlog(
                    klog.trace,
                    "allocated pid {} with epoch {}",
                    reply.data.producer_id,
                    reply.data.producer_epoch);
              } else {
                  vlog(klog.warn, "failed to allocate pid, ec: {}", r.ec);
                  reply.data.error_code = error_code::not_coordinator;
              }

              return ctx.respond(reply);
          });
    });
}

template<>
ss::future<response_ptr> create_acls_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    create_acls_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    if (!ctx.authorized(
          security::acl_operation::alter, security::default_cluster_name)) {
        creatable_acl_result result;
        result.error_code = error_code::cluster_authorization_failed;
        create_acls_response resp;
        resp.data.results.assign(request.data.creations.size(), result);
        co_return co_await ctx.respond(std::move(resp));
    }

    // <bindings index> | error
    std::vector<std::variant<size_t, creatable_acl_result>> result_index;
    result_index.reserve(request.data.creations.size());

    // bindings to create. optimized for common case
    std::vector<security::acl_binding> bindings;
    bindings.reserve(request.data.creations.size());

    for (const auto& acl : request.data.creations) {
        try {
            auto binding = details::to_acl_binding(acl);
            result_index.emplace_back(bindings.size());
            bindings.push_back(std::move(binding));

        } catch (const details::acl_conversion_error& e) {
            result_index.emplace_back(creatable_acl_result{
              .error_code = error_code::invalid_request,
              .error_message = e.what(),
            });
        }
    }

    const auto num_bindings = bindings.size();

    auto results = co_await ctx.security_frontend().create_acls(
      std::move(bindings), 5s);

    // kafka: put things back in the same order :(
    create_acls_response response;
    response.data.results.reserve(result_index.size());

    // this is an important step because the loop below that constructs the
    // response unconditionally indexes into the result set.
    if (unlikely(results.size() != num_bindings)) {
        vlog(
          klog.error,
          "Unexpected result size creating ACLs: {} (expected {})",
          results.size(),
          num_bindings);

        response.data.results.assign(
          result_index.size(),
          creatable_acl_result{.error_code = error_code::unknown_server_error});

        co_return co_await ctx.respond(std::move(response));
    }

    for (auto& result : result_index) {
        ss::visit(
          result,
          [&response, &results](size_t i) {
              auto ec = map_topic_error_code(results[i]);
              response.data.results.push_back(
                creatable_acl_result{.error_code = ec});
          },
          [&response](creatable_acl_result r) {
              response.data.results.push_back(std::move(r));
          });
    }

    co_return co_await ctx.respond(std::move(response));
}

template<>
process_result_stages
offset_commit_handler::handle(request_context ctx, ss::smp_service_group ssg) {
    offset_commit_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    // check authorization for this group
    const auto group_authorized = ctx.authorized(
      security::acl_operation::read, request.data.group_id);

    struct offset_commit_ctx {
        request_context rctx;
        offset_commit_request request;
        ss::smp_service_group ssg;

        // topic partitions found not to existent prior to processing. responses
        // for these are patched back into the final response after processing.
        absl::flat_hash_map<
          model::topic,
          std::vector<offset_commit_response_partition>>
          nonexistent_tps;

        // topic partitions that principal is not authorized to read
        absl::flat_hash_map<
          model::topic,
          std::vector<offset_commit_response_partition>>
          unauthorized_tps;

        offset_commit_ctx(
          request_context&& rctx,
          offset_commit_request&& request,
          ss::smp_service_group ssg)
          : rctx(std::move(rctx))
          , request(std::move(request))
          , ssg(ssg) {}
    };

    offset_commit_ctx octx(std::move(ctx), std::move(request), ssg);

    /*
     * offset commit will operate normally on topic-partitions in the request
     * that exist, while returning partial errors for those that do not exist.
     * in order to deal with this we filter out nonexistent topic-partitions,
     * and pass those that exist on to the group membership layer.
     *
     * TODO: the filtering is expensive for large requests. there are two things
     * that can be done to speed this up. first, the metadata cache should
     * provide an interface for efficiently searching for topic-partitions.
     * second, the code generator should be extended to allow the generated
     * structures to contain extra fields. in this case, we could introduce a
     * flag to mark topic-partitions to be ignored by the group membership
     * subsystem.
     */
    for (auto it = octx.request.data.topics.begin();
         it != octx.request.data.topics.end();) {
        /*
         * not authorized for this group. all topics in the request are
         * responded to with a group authorization failed error code.
         */
        if (!group_authorized) {
            auto& parts = octx.unauthorized_tps[it->name];
            parts.reserve(it->partitions.size());
            for (const auto& part : it->partitions) {
                parts.push_back(offset_commit_response_partition{
                  .partition_index = part.partition_index,
                  .error_code = error_code::group_authorization_failed,
                });
            }
            it = octx.request.data.topics.erase(it);
            continue;
        }

        /*
         * check topic authorization
         */
        if (!octx.rctx.authorized(security::acl_operation::read, it->name)) {
            auto& parts = octx.unauthorized_tps[it->name];
            parts.reserve(it->partitions.size());
            for (const auto& part : it->partitions) {
                parts.push_back(offset_commit_response_partition{
                  .partition_index = part.partition_index,
                  .error_code = error_code::topic_authorization_failed,
                });
            }
            it = octx.request.data.topics.erase(it);
            continue;
        }

        /*
         * check if topic exists
         */
        const auto topic_name = model::topic(it->name);
        model::topic_namespace_view tn(model::kafka_namespace, topic_name);

        if (octx.rctx.metadata_cache().contains(tn)) {
            /*
             * check if each partition exists
             */
            auto split = std::partition(
              it->partitions.begin(),
              it->partitions.end(),
              [&octx, &tn](const offset_commit_request_partition& p) {
                  return octx.rctx.metadata_cache().contains(
                    tn, p.partition_index);
              });
            /*
             * build responses for nonexistent topic partitions
             */
            if (split != it->partitions.end()) {
                auto& parts = octx.nonexistent_tps[it->name];
                for (auto part = split; part != it->partitions.end(); part++) {
                    parts.push_back(offset_commit_response_partition{
                      .partition_index = part->partition_index,
                      .error_code = error_code::unknown_topic_or_partition,
                    });
                }
                it->partitions.erase(split, it->partitions.end());
            }
            ++it;
        } else {
            /*
             * the topic doesn't exist. build all partition responses.
             */
            auto& parts = octx.nonexistent_tps[it->name];
            for (const auto& part : it->partitions) {
                parts.push_back(offset_commit_response_partition{
                  .partition_index = part.partition_index,
                  .error_code = error_code::unknown_topic_or_partition,
                });
            }
            it = octx.request.data.topics.erase(it);
        }
    }

    // all of the topics either don't exist or failed authorization
    if (unlikely(octx.request.data.topics.empty())) {
        offset_commit_response resp;
        for (auto& topic : octx.nonexistent_tps) {
            resp.data.topics.push_back(offset_commit_response_topic{
              .name = topic.first,
              .partitions = std::move(topic.second),
            });
        }
        for (auto& topic : octx.unauthorized_tps) {
            resp.data.topics.push_back(offset_commit_response_topic{
              .name = topic.first,
              .partitions = std::move(topic.second),
            });
        }
        return process_result_stages::single_stage(
          octx.rctx.respond(std::move(resp)));
    }
    ss::promise<> dispatch;
    auto dispatch_f = dispatch.get_future();
    auto f = ss::do_with(
      std::move(octx),
      [dispatch = std::move(dispatch)](offset_commit_ctx& octx) mutable {
          auto stages = octx.rctx.groups().offset_commit(
            std::move(octx.request));
          stages.dispatched.forward_to(std::move(dispatch));
          return stages.result.then([&octx](offset_commit_response resp) {
              if (unlikely(!octx.nonexistent_tps.empty())) {
                  /*
                   * copy over partitions for topics that had some partitions
                   * that were processed normally.
                   */
                  for (auto& topic : resp.data.topics) {
                      auto it = octx.nonexistent_tps.find(topic.name);
                      if (it != octx.nonexistent_tps.end()) {
                          topic.partitions.insert(
                            topic.partitions.end(),
                            it->second.begin(),
                            it->second.end());
                          octx.nonexistent_tps.erase(it);
                      }
                  }
                  /*
                   * the remaining nonexistent topics are moved into the
                   * response directly.
                   */
                  for (auto& topic : octx.nonexistent_tps) {
                      resp.data.topics.push_back(offset_commit_response_topic{
                        .name = topic.first,
                        .partitions = std::move(topic.second),
                      });
                  }
              }
              // no need to handle partial sets of partitions here because
              // authorization occurs all or nothing on a level
              for (auto& topic : octx.unauthorized_tps) {
                  resp.data.topics.push_back(offset_commit_response_topic{
                    .name = topic.first,
                    .partitions = std::move(topic.second),
                  });
              }
              return octx.rctx.respond(std::move(resp));
          });
      });

    return process_result_stages(std::move(dispatch_f), std::move(f));
}

template<>
ss::future<response_ptr>
describe_groups_handler::handle(request_context ctx, ss::smp_service_group) {
    describe_groups_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    auto unauthorized_it = std::partition(
      request.data.groups.begin(),
      request.data.groups.end(),
      [&ctx](const group_id& id) {
          return ctx.authorized(security::acl_operation::describe, id);
      });

    std::vector<group_id> unauthorized(
      std::make_move_iterator(unauthorized_it),
      std::make_move_iterator(request.data.groups.end()));

    request.data.groups.erase(unauthorized_it, request.data.groups.end());

    describe_groups_response response;

    if (likely(!request.data.groups.empty())) {
        std::vector<ss::future<described_group>> described;
        described.reserve(request.data.groups.size());
        for (auto& group_id : request.data.groups) {
            described.push_back(ctx.groups().describe_group(group_id).then(
              [&ctx, &request, group_id](auto res) {
                  if (request.data.include_authorized_operations) {
                      res.authorized_operations = details::to_bit_field(
                        details::authorized_operations(ctx, group_id));
                  }
                  return res;
              }));
        }
        response.data.groups = co_await ss::when_all_succeed(
          described.begin(), described.end());
    }

    for (auto& group : unauthorized) {
        response.data.groups.push_back(described_group{
          .error_code = error_code::group_authorization_failed,
          .group_id = std::move(group),
        });
    }

    co_return co_await ctx.respond(std::move(response));
}

template<>
ss::future<response_ptr>
list_transactions_handler::handle(request_context ctx, ss::smp_service_group) {
    list_transactions_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    list_transactions_response response;

    auto filter_tx = [](
                       const list_transactions_request& req,
                       const cluster::tm_transaction& tx) -> bool {
        if (!req.data.producer_id_filters.empty()) {
            if (std::none_of(
                  req.data.producer_id_filters.begin(),
                  req.data.producer_id_filters.end(),
                  [pid = tx.pid.get_id()](const auto& provided_pid) {
                      return pid == provided_pid;
                  })) {
                return false;
            }
        }

        if (!req.data.state_filters.empty()) {
            if (std::none_of(
                  req.data.state_filters.begin(),
                  req.data.state_filters.end(),
                  [status = tx.get_kafka_status()](
                    const auto& provided_status) {
                      return status == provided_status;
                  })) {
                return false;
            }
        }
        return true;
    };

    auto& tx_frontend = ctx.tx_gateway_frontend();
    auto txs = co_await tx_frontend.get_all_transactions();
    if (txs.has_value()) {
        for (const auto& tx : txs.value()) {
            if (!ctx.authorized(security::acl_operation::describe, tx.id)) {
                // We should skip this transactional id
                continue;
            }

            if (filter_tx(request, tx)) {
                list_transaction_state tx_state;
                tx_state.transactional_id = tx.id;
                tx_state.producer_id = kafka::producer_id(tx.pid.id);
                tx_state.transaction_state = ss::sstring(tx.get_status());
                response.data.transaction_states.push_back(std::move(tx_state));
            }
        }
    } else {
        // In this 2 errors not coordinator got request and we just return empty
        // array
        if (
          txs.error() != cluster::tx_errc::shard_not_found
          && txs.error() != cluster::tx_errc::not_coordinator) {
            vlog(
              klog.error,
              "Can not return list of transactions. Error: {}",
              txs.error());
            response.data.error_code = kafka::error_code::unknown_server_error;
        }
    }

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
