// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/service.h"

#include "cluster/controller.h"
#include "cluster/ephemeral_credential_frontend.h"
#include "cluster/members_table.h"
#include "cluster/security_frontend.h"
#include "config/configuration.h"
#include "kafka/client/brokers.h"
#include "kafka/client/client_fetch_batch_reader.h"
#include "kafka/client/config_utils.h"
#include "kafka/client/exceptions.h"
#include "kafka/protocol/create_topics.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/list_offset.h"
#include "kafka/protocol/topic_properties.h"
#include "model/fundamental.h"
#include "pandaproxy/api/api-doc/schema_registry.json.hh"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "pandaproxy/schema_registry/handlers.h"
#include "pandaproxy/schema_registry/storage.h"
#include "pandaproxy/util.h"
#include "security/acl.h"
#include "security/audit/audit_log_manager.h"
#include "security/audit/types.h"
#include "security/ephemeral_credential_store.h"
#include "security/request_auth.h"
#include "ssx/semaphore.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/memory.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/http/api_docs.hh>
#include <seastar/http/exception.hh>
#include <seastar/util/noncopyable_function.hh>

#include <boost/algorithm/string/predicate.hpp>

namespace pandaproxy::schema_registry {

static constexpr auto audit_svc_name = "Redpanda Schema Registry Service";

using server = ctx_server<service>;
const security::acl_principal principal{
  security::principal_type::ephemeral_user, "__schema_registry"};

class wrap {
public:
    enum class auth_level {
        // Unauthenticated endpoint (not a typo, 'public' is a keyword)
        publik,
        // Requires authentication (if enabled) but not superuser status
        user,
        // Requires authentication (if enabled) and superuser status
        superuser
    };

    wrap(ss::gate& g, one_shot& os, auth_level lvl, server::function_handler h)
      : _g{g}
      , _os{os}
      , _auth_level(lvl)
      , _h{std::move(h)} {}

    ss::future<server::reply_t>
    operator()(server::request_t rq, server::reply_t rp) const {
        handle_auth(rq);

        co_await _os();
        auto guard = _g.hold();
        try {
            co_return co_await _h(std::move(rq), std::move(rp));
        } catch (const kafka::client::partition_error& ex) {
            if (
              ex.error == kafka::error_code::unknown_topic_or_partition
              && ex.tp.topic == model::schema_registry_internal_tp.topic) {
                throw exception(
                  kafka::error_code::unknown_server_error,
                  "_schemas topic does not exist");
            }
            throw;
        }
    }

private:
    // Authenticates and authorizes the request when HTTP Basic Auth is enabled
    // and handles audit logging. It throws on failure.
    void handle_auth(server::request_t& rq) const {
        rq.authn_method = config::get_authn_method(
          rq.service().config().schema_registry_api.value(),
          rq.req->get_listener_idx());

        if (rq.authn_method != config::rest_authn_method::none) {
            // Will throw 400 & 401 if auth fails
            auto auth_result = [this, &rq]() {
                try {
                    return rq.service().authenticator().authenticate(*rq.req);
                } catch (const unauthorized_user_exception& e) {
                    audit_authn_failure(rq, e.get_username(), e.what());
                    throw;
                } catch (const ss::httpd::base_exception& e) {
                    audit_authn_failure(rq, "", e.what());
                    throw;
                }
            }();

            rq.user = credential_t{
              auth_result.get_username(),
              auth_result.get_password(),
              auth_result.get_sasl_mechanism()};
            audit_authn_success(rq);

            // Will throw 403 if user enabled HTTP Basic Auth but
            // did not give the authorization header.
            [this, &rq, &auth_result]() {
                try {
                    switch (_auth_level) {
                    case auth_level::superuser:
                        auth_result.require_superuser();
                        break;
                    case auth_level::user:
                        auth_result.require_authenticated();
                        break;
                    case auth_level::publik:
                        auth_result.pass();
                        break;
                    }
                } catch (const ss::httpd::base_exception& e) {
                    audit_authz_failure(rq, auth_result, e.what());
                    throw;
                }
            }();

            audit_authz_success(rq);
        } else {
            rq.user = credential_t{};
            audit_authn_success(rq);
            audit_authz_success(rq);
        }
    }

    inline net::unresolved_address
    from_ss_sa(const ss::socket_address& sa) const {
        return {fmt::format("{}", sa.addr()), sa.port(), sa.addr().in_family()};
    }

    security::audit::authentication::used_cleartext
    is_cleartext(const ss::sstring& protocol) const {
        return boost::iequals(protocol, "https")
                 ? security::audit::authentication::used_cleartext::no
                 : security::audit::authentication::used_cleartext::yes;
    }
    security::audit::authentication_event_options
    make_authn_event_options(const server::request_t& rq) const {
        return {
          .auth_protocol = rq.user.sasl_mechanism,
          .server_addr = from_ss_sa(rq.req->get_server_address()),
          .svc_name = audit_svc_name,
          .client_addr = from_ss_sa(rq.req->get_client_address()),
          .is_cleartext = is_cleartext(rq.req->get_protocol_name()),
          .user = {
            .name = rq.user.name.empty() ? "{{anonymous}}" : rq.user.name,
            .type_id = rq.user.name.empty()
                         ? security::audit::user::type::unknown
                         : security::audit::user::type::user}};
    }
    security::audit::authentication_event_options make_authn_event_error(
      const server::request_t& rq,
      const ss::sstring& username,
      ss::sstring reason) const {
        return {
          .server_addr = from_ss_sa(rq.req->get_server_address()),
          .svc_name = audit_svc_name,
          .client_addr = from_ss_sa(rq.req->get_client_address()),
          .is_cleartext = is_cleartext(rq.req->get_protocol_name()),
          .user
          = {.name = username, .type_id = security::audit::user::type::unknown},
          .error_reason = reason};
    }

    void audit_authn_failure(
      const server::request_t& rq,
      const ss::sstring& username,
      ss::sstring reason) const {
        do_audit_authn(
          rq, make_authn_event_error(rq, username, std::move(reason)));
    }

    void audit_authn_success(const server::request_t& rq) const {
        do_audit_authn(rq, make_authn_event_options(rq));
    }

    void audit_authz_success(const server::request_t& rq) const {
        do_audit_authz(rq);
    }

    void audit_authz_failure(
      const server::request_t& rq,
      const request_auth_result auth_result,
      ss::sstring reason) const {
        vlog(
          plog.trace, "Attempting to audit authz for {}", rq.req->format_url());
        auto success = rq.service().audit_mgr().enqueue_api_activity_event(
          security::audit::event_type::schema_registry,
          *rq.req,
          auth_result,
          audit_svc_name,
          false,
          std::move(reason));

        if (!success) {
            vlog(
              plog.error,
              "Failed to audit authorization request for endpoint: {}",
              rq.req->format_url());
            throw ss::httpd::base_exception(
              "Failed to audit authorization request",
              ss::http::reply::status_type::service_unavailable);
        }
    }

    void do_audit_authn(
      const server::request_t& rq,
      security::audit::authentication_event_options options) const {
        vlog(
          plog.trace, "Attempting to audit authn for {}", rq.req->format_url());
        auto success = rq.service().audit_mgr().enqueue_authn_event(
          std::move(options));
        if (!success) {
            vlog(
              plog.error,
              "Failed to audit authentication request for endpoint: {}",
              rq.req->format_url());
            throw ss::httpd::base_exception(
              "Failed to audit authentication request",
              ss::http::reply::status_type::service_unavailable);
        }
    }

    void do_audit_authz(const server::request_t& rq) const {
        vlog(
          plog.trace, "Attempting to audit authz for {}", rq.req->format_url());
        auto success = rq.service().audit_mgr().enqueue_api_activity_event(
          security::audit::event_type::schema_registry,
          *rq.req,
          rq.user.name,
          audit_svc_name);

        if (!success) {
            vlog(
              plog.error,
              "Failed to audit authorization request for endpoint: {}",
              rq.req->format_url());
            throw ss::httpd::base_exception(
              "Failed to audit authorization request",
              ss::http::reply::status_type::service_unavailable);
        }
    }

private:
    ss::gate& _g;
    one_shot& _os;
    auth_level _auth_level;
    server::function_handler _h;
};

server::routes_t get_schema_registry_routes(ss::gate& gate, one_shot& es) {
    using auth_level = wrap::auth_level;
    server::routes_t routes;
    routes.api = ss::httpd::schema_registry_json::name;

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_config,
      wrap(gate, es, auth_level::user, get_config)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::put_config,
      wrap(gate, es, auth_level::user, put_config)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_config_subject,
      wrap(gate, es, auth_level::user, get_config_subject)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::put_config_subject,
      wrap(gate, es, auth_level::user, put_config_subject)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::delete_config_subject,
      wrap(gate, es, auth_level::user, delete_config_subject)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_mode,
      wrap(gate, es, auth_level::user, get_mode)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::put_mode,
      wrap(gate, es, auth_level::superuser, put_mode)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_mode_subject,
      wrap(gate, es, auth_level::user, get_mode_subject)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::put_mode_subject,
      wrap(gate, es, auth_level::superuser, put_mode_subject)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::delete_mode_subject,
      wrap(gate, es, auth_level::superuser, delete_mode_subject)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_schemas_types,
      wrap(gate, es, auth_level::publik, get_schemas_types)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_schemas_ids_id,
      wrap(gate, es, auth_level::user, get_schemas_ids_id)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_schemas_ids_id_versions,
      wrap(gate, es, auth_level::user, get_schemas_ids_id_versions)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_schemas_ids_id_subjects,
      wrap(gate, es, auth_level::user, get_schemas_ids_id_subjects)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_subjects,
      wrap(gate, es, auth_level::user, get_subjects)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_subject_versions,
      wrap(gate, es, auth_level::user, get_subject_versions)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::post_subject,
      wrap(gate, es, auth_level::user, post_subject)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::post_subject_versions,
      wrap(gate, es, auth_level::user, post_subject_versions)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_subject_versions_version,
      wrap(gate, es, auth_level::user, get_subject_versions_version)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_subject_versions_version_schema,
      wrap(gate, es, auth_level::user, get_subject_versions_version_schema)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::
        get_subject_versions_version_referenced_by,
      wrap(
        gate,
        es,
        auth_level::user,
        get_subject_versions_version_referenced_by)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::
        get_subject_versions_version_referenced_by_deprecated,
      wrap(
        gate,
        es,
        auth_level::user,
        get_subject_versions_version_referenced_by)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::delete_subject,
      wrap(gate, es, auth_level::user, delete_subject)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::delete_subject_version,
      wrap(gate, es, auth_level::user, delete_subject_version)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::compatibility_subject_version,
      wrap(gate, es, auth_level::user, compatibility_subject_version)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::schema_registry_status_ready,
      wrap(gate, es, auth_level::publik, status_ready)});

    return routes;
}

ss::future<> service::do_start() {
    if (_is_started) {
        co_return;
    }
    auto guard = _gate.hold();
    try {
        co_await create_internal_topic();
        vlog(plog.info, "Schema registry successfully initialized");
    } catch (...) {
        vlog(
          plog.error,
          "Schema registry failed to initialize: {}",
          std::current_exception());
        throw;
    }
    co_await container().invoke_on_all(_ctx.smp_sg, [](service& s) {
        s._is_started = true;
        return s.fetch_internal_topic();
    });
}

ss::future<> create_acls(cluster::security_frontend& security_fe) {
    std::vector<security::acl_binding> princpal_acl_binding{
      security::acl_binding{
        security::resource_pattern{
          security::resource_type::topic,
          model::schema_registry_internal_tp.topic,
          security::pattern_type::literal},
        security::acl_entry{
          principal,
          security::acl_host::wildcard_host(),
          security::acl_operation::all,
          security::acl_permission::allow}}};

    auto err_vec = co_await security_fe.create_acls(princpal_acl_binding, 5s);
    auto it = std::find_if(err_vec.begin(), err_vec.end(), [](const auto& err) {
        return err != cluster::errc::success;
    });

    if (it != err_vec.end()) {
        vlog(
          plog.warn,
          "Failed to create ACLs for {}, err {} - {}",
          principal,
          *it,
          cluster::make_error_code(*it).message());
    } else {
        vlog(plog.debug, "Successfully created ACLs for {}", principal);
    }
}

ss::future<> service::configure() {
    auto config = co_await kafka::client::create_client_credentials(
      *_controller,
      config::shard_local_cfg(),
      _client.local().config(),
      principal);
    co_await kafka::client::set_client_credentials(*config, _client);

    const auto& store = _controller->get_ephemeral_credential_store().local();
    bool has_ephemeral_credentials = store.has(store.find(principal));
    co_await container().invoke_on_all(
      _ctx.smp_sg, [has_ephemeral_credentials](service& s) {
          s._has_ephemeral_credentials = has_ephemeral_credentials;
      });
}

ss::future<> service::mitigate_error(std::exception_ptr eptr) {
    if (_gate.is_closed()) {
        // Return so that the client doesn't try to mitigate.
        return ss::now();
    }
    vlog(plog.warn, "mitigate_error: {}", eptr);
    return ss::make_exception_future<>(eptr)
      .handle_exception_type(
        [this, eptr](const kafka::client::broker_error& ex) {
            if (
              ex.error == kafka::error_code::sasl_authentication_failed
              && _has_ephemeral_credentials) {
                return inform(ex.node_id).then([this]() {
                    // This fully mitigates, don't rethrow.
                    return _client.local().connect();
                });
            }

            // Rethrow unhandled exceptions
            return ss::make_exception_future<>(eptr);
        })
      .handle_exception_type([this,
                              eptr](const kafka::client::topic_error& ex) {
          if (
            ex.error == kafka::error_code::topic_authorization_failed
            && _has_ephemeral_credentials) {
              return create_acls(_controller->get_security_frontend().local());
          }

          return ss::make_exception_future<>(eptr);
      });
}

ss::future<> service::inform(model::node_id id) {
    vlog(plog.trace, "inform: {}", id);

    // Inform a particular node
    if (id != kafka::client::unknown_node_id) {
        return do_inform(id);
    }

    // Inform all nodes
    return seastar::parallel_for_each(
      _controller->get_members_table().local().node_ids(),
      [this](model::node_id id) { return do_inform(id); });
}

ss::future<> service::do_inform(model::node_id id) {
    auto& fe = _controller->get_ephemeral_credential_frontend().local();
    auto ec = co_await fe.inform(id, principal);
    vlog(plog.info, "Informed: broker: {}, ec: {}", id, ec);
}

ss::future<> service::create_internal_topic() {
    // Use the default topic replica count, unless our specific setting
    // for the schema registry chooses to override it.
    int16_t replication_factor
      = _config.schema_registry_replication_factor().value_or(
        _controller->internal_topic_replication());

    vlog(
      plog.debug,
      "Schema registry: attempting to create internal topic (replication={})",
      replication_factor);

    auto make_internal_topic = [replication_factor]() {
        constexpr std::string_view retain_forever = "-1";
        return kafka::creatable_topic{
          .name{model::schema_registry_internal_tp.topic},
          .num_partitions = 1,
          .replication_factor = replication_factor,
          .assignments{},
          .configs{
            {.name{ss::sstring{kafka::topic_property_cleanup_policy}},
             .value{"compact"}},
            {.name{ss::sstring{kafka::topic_property_compression}},
             .value{ssx::sformat("{}", model::compression::none)}},
            {.name{ss::sstring{kafka::topic_property_retention_bytes}},
             .value{retain_forever}},
            {.name{ss::sstring{kafka::topic_property_retention_duration}},
             .value{retain_forever}},
            {.name{
               ss::sstring{kafka::topic_property_retention_local_target_bytes}},
             .value{retain_forever}},
            {.name{
               ss::sstring{kafka::topic_property_retention_local_target_ms}},
             .value{retain_forever}},
            {.name{ss::sstring{
               kafka::topic_property_initial_retention_local_target_bytes}},
             .value{retain_forever}},
            {.name{ss::sstring{
               kafka::topic_property_initial_retention_local_target_ms}},
             .value{retain_forever}}}};
    };
    auto res = co_await _client.local().create_topic(make_internal_topic());
    if (res.data.topics.size() != 1) {
        throw std::runtime_error("Unexpected topic count");
    }

    const auto& topic = res.data.topics[0];
    if (topic.error_code == kafka::error_code::none) {
        vlog(plog.debug, "Schema registry: created internal topic");
    } else if (topic.error_code == kafka::error_code::topic_already_exists) {
        vlog(plog.debug, "Schema registry: found internal topic");
    } else if (topic.error_code == kafka::error_code::not_controller) {
        vlog(plog.debug, "Schema registry: not controller");
    } else {
        throw kafka::exception(
          topic.error_code,
          topic.error_message.value_or(
            kafka::make_error_code(topic.error_code).message()));
    }

    // TODO(Ben): Validate the _schemas topic
}

ss::future<> service::fetch_internal_topic() {
    vlog(plog.debug, "Schema registry: loading internal topic");

    // TODO: should check the replication_factor of the topic is
    // what our config calls for

    auto offset_res = co_await _client.local().list_offsets(
      model::schema_registry_internal_tp);
    auto max_offset = offset_res.data.topics[0].partitions[0].offset;
    vlog(plog.debug, "Schema registry: _schemas max_offset: {}", max_offset);

    co_await kafka::client::make_client_fetch_batch_reader(
      _client.local(),
      model::schema_registry_internal_tp,
      model::offset{0},
      max_offset)
      .consume(consume_to_store{_store, writer()}, model::no_timeout);
}

service::service(
  const YAML::Node& config,
  ss::smp_service_group smp_sg,
  size_t max_memory,
  ss::sharded<kafka::client::client>& client,
  sharded_store& store,
  ss::sharded<seq_writer>& sequencer,
  std::unique_ptr<cluster::controller>& controller,
  ss::sharded<security::audit::audit_log_manager>& audit_mgr)
  : _config(config)
  , _mem_sem(max_memory, "pproxy/schema-svc")
  , _inflight_sem(config::shard_local_cfg()
                    .max_in_flight_schema_registry_requests_per_shard())
  , _inflight_config_binding(
      config::shard_local_cfg()
        .max_in_flight_schema_registry_requests_per_shard.bind())
  , _client(client)
  , _ctx{{{}, _mem_sem, _inflight_sem, {}, smp_sg}, *this}
  , _server(
      "schema_registry", // server_name
      "schema_registry", // public_metric_group_name
      ss::httpd::api_registry_builder20(_config.api_doc_dir(), "/v1"),
      "schema_registry_header",
      "/schema_registry_definitions",
      _ctx,
      json::serialization_format::schema_registry_v1_json)
  , _store(store)
  , _writer(sequencer)
  , _controller(controller)
  , _audit_mgr(audit_mgr)
  , _ensure_started{[this]() { return do_start(); }}
  , _auth{
      config::always_true(),
      config::shard_local_cfg().superusers.bind(),
      controller.get()} {
    _inflight_config_binding.watch(
      [this]() { _inflight_sem.set_capacity(_inflight_config_binding()); });
}

ss::future<> service::start() {
    co_await configure();
    static std::vector<model::broker_endpoint> not_advertised{};
    _server.routes(get_schema_registry_routes(_gate, _ensure_started));
    co_return co_await _server.start(
      _config.schema_registry_api(),
      _config.schema_registry_api_tls(),
      not_advertised);
}

ss::future<> service::stop() {
    co_await _gate.close();
    co_await _server.stop();
}

configuration& service::config() { return _config; }

kafka::client::configuration& service::client_config() {
    return _client.local().config();
}

} // namespace pandaproxy::schema_registry
