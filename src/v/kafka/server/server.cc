// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "server.h"

#include "cluster/topics_frontend.h"
#include "config/broker_authn_endpoint.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "kafka/server/connection_context.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/group_router.h"
#include "kafka/server/handlers/add_offsets_to_txn.h"
#include "kafka/server/handlers/add_partitions_to_txn.h"
#include "kafka/server/handlers/delete_groups.h"
#include "kafka/server/handlers/end_txn.h"
#include "kafka/server/handlers/heartbeat.h"
#include "kafka/server/handlers/join_group.h"
#include "kafka/server/handlers/leave_group.h"
#include "kafka/server/handlers/list_groups.h"
#include "kafka/server/handlers/offset_fetch.h"
#include "kafka/server/handlers/sasl_authenticate.h"
#include "kafka/server/handlers/sasl_handshake.h"
#include "kafka/server/handlers/sync_group.h"
#include "kafka/server/logger.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "net/connection.h"
#include "security/errc.h"
#include "security/exceptions.h"
#include "security/mtls.h"
#include "security/scram_algorithm.h"
#include "security/scram_authenticator.h"
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

static const std::vector<ss::sstring> supported_sasl_mechanisms = {
  security::scram_sha256_authenticator::name,
  security::scram_sha512_authenticator::name};

namespace kafka {

server::server(
  ss::sharded<net::server_configuration>* cfg,
  ss::smp_service_group smp,
  ss::sharded<cluster::metadata_cache>& meta,
  ss::sharded<cluster::topics_frontend>& tf,
  ss::sharded<cluster::config_frontend>& cf,
  ss::sharded<features::feature_table>& ft,
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
  std::optional<qdc_monitor::config> qdc_config) noexcept
  : net::server(cfg, klog)
  , _smp_group(smp)
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
  , _mtls_principal_mapper(
      config::shard_local_cfg().kafka_mtls_principal_mapping_rules.bind()) {
    if (qdc_config) {
        _qdc_mon.emplace(*qdc_config);
    }
    _probe.setup_metrics();
    _probe.setup_public_metrics();
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
      config::shard_local_cfg().kafka_request_max_bytes.bind());

    co_return co_await ss::do_until(
      [ctx] { return ctx->is_finished_parsing(); },
      [ctx] { return ctx->process_one_request(); })
      .handle_exception([ctx, authn_method](std::exception_ptr eptr) {
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
          return ss::make_exception_future(eptr);
      })
      .finally([ctx] {});
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

    auto result = ctx.sasl()->authenticate(std::move(request.data.auth_bytes));
    if (likely(result)) {
        sasl_authenticate_response_data data{
          .error_code = error_code::none,
          .error_message = std::nullopt,
          .auth_bytes = std::move(result.value()),
        };
        return ctx.respond(sasl_authenticate_response(std::move(data)));
    }

    sasl_authenticate_response_data data{
      .error_code = error_code::sasl_authentication_failed,
      .error_message = ssx::sformat(
        "SASL authentication failed: {}", result.error().message()),
    };
    return ctx.respond(sasl_authenticate_response(std::move(data)));
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

    /*
     * configure sasl for the current connection context. see the sasl
     * authenticate request for the next phase of the auth process.
     */
    auto error = error_code::none;

    if (request.data.mechanism == security::scram_sha256_authenticator::name) {
        ctx.sasl()->set_mechanism(
          std::make_unique<security::scram_sha256_authenticator::auth>(
            ctx.credentials()));

    } else if (
      request.data.mechanism == security::scram_sha512_authenticator::name) {
        ctx.sasl()->set_mechanism(
          std::make_unique<security::scram_sha512_authenticator::auth>(
            ctx.credentials()));

    } else {
        error = error_code::unsupported_sasl_mechanism;
    }

    return ctx.respond(
      sasl_handshake_response(error, supported_sasl_mechanisms));
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
            topic.partitions.push_back(std::move(partition));
        }
    }

    co_return co_await ctx.respond(std::move(resp));
}

} // namespace kafka
