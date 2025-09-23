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
#include "pandaproxy/schema_registry/auth.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "pandaproxy/schema_registry/handlers.h"
#include "pandaproxy/schema_registry/storage.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/util.h"
#include "security/acl.h"
#include "security/audit/audit_log_manager.h"
#include "security/ephemeral_credential_store.h"
#include "security/request_auth.h"
#include "ssx/semaphore.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/memory.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/http/api_docs.hh>
#include <seastar/http/exception.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <boost/algorithm/string/predicate.hpp>

#include <variant>

namespace pandaproxy::schema_registry {

using server = ctx_server<service>;

class wrap {
public:
    wrap(
      ss::gate& g,
      one_shot& os,
      auth auth,
      auth::function_handler h,
      std::string_view operation_name)
      : _g{g}
      , _os{os}
      , _auth{std::move(auth)}
      , _h{std::move(h)}
      , _operation_name(operation_name) {
        const auto is_h_deferred
          = std::holds_alternative<auth::deferred_function_handler>(_h);
        vassert(
          _auth.is_deferred() == is_h_deferred,
          "Deferred auth endpoints must use a deferred handler");
    }
    ss::future<server::reply_t>
    operator()(server::request_t rq, server::reply_t rp) const {
        auto auth_result = _auth.handle_auth(rq, _operation_name);

        co_await _os();
        auto guard = _g.hold();
        try {
            co_return co_await ss::visit(
              _h,
              [&](const auth::regular_function_handler& h) {
                  vassert(
                    !auth_result.has_value(),
                    "Authorization must not be deferred for non-deferred "
                    "endpoints");
                  return h(std::move(rq), std::move(rp));
              },
              [&](const auth::deferred_function_handler& h) {
                  return h(
                    std::move(rq), std::move(rp), std::move(auth_result));
              });
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
    ss::gate& _g;
    one_shot& _os;
    auth _auth;
    auth::function_handler _h;
    std::string_view _operation_name;
};

server::routes_t get_schema_registry_routes(ss::gate& gate, one_shot& es) {
    using security::acl_operation;

    server::routes_t routes;
    routes.api = ss::httpd::schema_registry_json::name;

    auto wrap = [&gate, &es](
                  const ss::httpd::path_description& path,
                  auth::level lvl,
                  std::optional<auth::op> op,
                  auth::resource res,
                  auth::function_handler h) {
        return server::route_t{
          path,
          schema_registry::wrap(
            gate,
            es,
            auth{lvl, op, std::move(res)},
            std::move(h),
            path.operations.nickname)};
    };

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::get_config,
      auth::level::user,
      acl_operation::describe_configs,
      registry_resource{},
      get_config));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::put_config,
      auth::level::user,
      acl_operation::alter_configs,
      registry_resource{},
      put_config));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::get_config_subject,
      auth::level::user,
      acl_operation::describe_configs,
      subject{},
      get_config_subject));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::put_config_subject,
      auth::level::user,
      acl_operation::alter_configs,
      subject{},
      put_config_subject));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::delete_config_subject,
      auth::level::user,
      acl_operation::alter_configs,
      subject{},
      delete_config_subject));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::get_mode,
      auth::level::user,
      acl_operation::describe_configs,
      registry_resource{},
      get_mode));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::put_mode,
      auth::level::superuser,
      acl_operation::alter_configs,
      registry_resource{},
      put_mode));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::get_mode_subject,
      auth::level::user,
      acl_operation::describe_configs,
      subject{},
      get_mode_subject));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::put_mode_subject,
      auth::level::superuser,
      acl_operation::alter_configs,
      subject{},
      put_mode_subject));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::delete_mode_subject,
      auth::level::superuser,
      acl_operation::alter_configs,
      subject{},
      delete_mode_subject));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::get_schemas_types,
      auth::level::publik,
      acl_operation::read,
      auth::none{},
      get_schemas_types));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::get_schemas_ids_id,
      auth::level::user,
      std::nullopt,
      auth::deferred{},
      get_schemas_ids_id));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::get_schemas_ids_id_versions,
      auth::level::user,
      acl_operation::describe,
      registry_resource{},
      get_schemas_ids_id_versions));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::get_schemas_ids_id_subjects,
      auth::level::user,
      acl_operation::describe,
      registry_resource{},
      get_schemas_ids_id_subjects));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::get_subjects,
      auth::level::user,
      std::nullopt,
      auth::deferred{},
      get_subjects));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::get_subject_versions,
      auth::level::user,
      acl_operation::describe,
      subject{},
      get_subject_versions));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::post_subject,
      auth::level::user,
      acl_operation::read,
      subject{},
      post_subject));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::post_subject_versions,
      auth::level::user,
      acl_operation::write,
      subject{},
      post_subject_versions));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::get_subject_versions_version,
      auth::level::user,
      acl_operation::read,
      subject{},
      get_subject_versions_version));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::get_subject_versions_version_schema,
      auth::level::user,
      acl_operation::read,
      subject{},
      get_subject_versions_version_schema));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::
        get_subject_versions_version_referenced_by,
      auth::level::user,
      acl_operation::describe,
      registry_resource{},
      get_subject_versions_version_referenced_by));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::
        get_subject_versions_version_referenced_by_deprecated,
      auth::level::user,
      acl_operation::describe,
      registry_resource{},
      get_subject_versions_version_referenced_by));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::delete_subject,
      auth::level::user,
      acl_operation::remove,
      subject{},
      delete_subject));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::delete_subject_version,
      auth::level::user,
      acl_operation::remove,
      subject{},
      delete_subject_version));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::compatibility_subject_version,
      auth::level::user,
      acl_operation::read,
      subject{},
      compatibility_subject_version));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::schema_registry_status_ready,
      auth::level::publik,
      acl_operation::read,
      auth::none{},
      status_ready));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::get_security_acls,
      auth::level::superuser,
      acl_operation::describe,
      security::default_cluster_name,
      get_security_acls));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::post_security_acls,
      auth::level::superuser,
      acl_operation::alter,
      security::default_cluster_name,
      post_security_acls));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::delete_security_acls,
      auth::level::superuser,
      acl_operation::alter,
      security::default_cluster_name,
      delete_security_acls));

    return routes;
}

ss::future<> service::do_start() {
    if (_is_started) {
        co_return;
    }
    auto guard = _gate.hold();
    try {
        co_await create_internal_topic();
        vlog(srlog.info, "Schema registry successfully initialized");
    } catch (...) {
        vlogl(
          srlog,
          ssx::is_shutdown_exception(std::current_exception())
            ? ss::log_level::debug
            : ss::log_level::error,
          "Schema registry failed to initialize: {}",
          std::current_exception());
        throw;
    }
    co_await container().invoke_on_all(_ctx.smp_sg, [](service& s) {
        s._is_started = true;
        return ss::this_shard_id() == seq_writer::reader_shard
                 ? s.fetch_internal_topic()
                 : ss::now();
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
          security::schema_registry_principal,
          security::acl_host::wildcard_host(),
          security::acl_operation::all,
          security::acl_permission::allow}}};

    auto err_vec = co_await security_fe.create_acls(princpal_acl_binding, 5s);
    auto it = std::find_if(err_vec.begin(), err_vec.end(), [](const auto& err) {
        return err != cluster::errc::success;
    });

    if (it != err_vec.end()) {
        vlog(
          srlog.warn,
          "Failed to create ACLs for {}, err {} - {}",
          security::schema_registry_principal,
          *it,
          cluster::make_error_code(*it).message());
    } else {
        vlog(
          srlog.debug,
          "Successfully created ACLs for {}",
          security::schema_registry_principal);
    }
}

ss::future<> service::configure() {
    auto sasl_config = co_await kafka::client::create_client_credentials(
      *_controller, _client_config, security::schema_registry_principal);
    co_await _client.invoke_on_all(
      [sasl_config = std::move(sasl_config)](kafka::client::client& c) {
          c.set_credentials(sasl_config);
      });

    const auto& store = _controller->get_ephemeral_credential_store().local();
    bool has_ephemeral_credentials = store.has(
      store.find(security::schema_registry_principal));
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
    vlog(srlog.warn, "mitigate_error: {}", eptr);
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
    vlog(srlog.trace, "inform: {}", id);

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
    auto ec = co_await fe.inform(id, security::schema_registry_principal);
    vlog(srlog.info, "Informed: broker: {}, ec: {}", id, ec);
}

ss::future<> service::create_internal_topic() {
    // Use the default topic replica count, unless our specific setting
    // for the schema registry chooses to override it.
    int16_t replication_factor
      = _config.schema_registry_replication_factor().value_or(
        _controller->internal_topic_replication());

    vlog(
      srlog.debug,
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
        vlog(srlog.debug, "Schema registry: created internal topic");
    } else if (topic.error_code == kafka::error_code::topic_already_exists) {
        vlog(srlog.debug, "Schema registry: found internal topic");
    } else if (topic.error_code == kafka::error_code::not_controller) {
        vlog(srlog.debug, "Schema registry: not controller");
    } else {
        throw kafka::exception(
          topic.error_code,
          topic.error_message.value_or(
            kafka::make_error_code(topic.error_code).message()));
    }

    // TODO(Ben): Validate the _schemas topic
}

ss::future<> service::fetch_internal_topic() {
    vlog(srlog.debug, "Schema registry: loading internal topic");

    // TODO: should check the replication_factor of the topic is
    // what our config calls for

    auto offset_res = co_await _client.local().list_offsets(
      model::schema_registry_internal_tp);
    if (
      offset_res.data.topics.size() != 1
      || offset_res.data.topics[0].partitions.size() != 1) {
        throw kafka::exception(
          kafka::error_code::unknown_server_error,
          "Malformed ListOffsets Kafka response for internal topic");
    }

    auto max_offset = offset_res.data.topics[0].partitions[0].offset;
    vlog(srlog.debug, "Schema registry: _schemas max_offset: {}", max_offset);

    co_await kafka::client::make_client_fetch_batch_reader(
      _client.local(),
      model::schema_registry_internal_tp,
      model::offset{0},
      max_offset)
      .consume(consume_to_store{_store, writer()}, model::no_timeout);

    // If a schema failed to be compiled, it will be marked. We attempt to
    // reprocess them once now that the whole topic has been read,  in case they
    // have a reference to a schema declared later in the topic.
    co_await _store.process_marked_schemas();
}

service::service(
  const YAML::Node& config,
  const YAML::Node& client_config,
  ss::smp_service_group smp_sg,
  size_t max_memory,
  ss::sharded<kafka::client::client>& client,
  sharded_store& store,
  ss::sharded<seq_writer>& sequencer,
  std::unique_ptr<cluster::controller>& controller,
  ss::sharded<security::audit::audit_log_manager>& audit_mgr)
  : _config(config)
  , _client_config(client_config)
  , _mem_sem(max_memory, "pproxy/schema-svc")
  , _inflight_sem(
      config::shard_local_cfg()
        .max_in_flight_schema_registry_requests_per_shard())
  , _inflight_config_binding(
      config::shard_local_cfg()
        .max_in_flight_schema_registry_requests_per_shard.bind())
  , _client(client)
  , _ctx{{{}, max_memory, _mem_sem, _inflight_config_binding(), _inflight_sem, {}, smp_sg}, *this}
  , _server(
      "schema_registry", // server_name
      "schema_registry", // public_metric_group_name
      ss::httpd::api_registry_builder20(_config.api_doc_dir(), "/v1"),
      "schema_registry_header",
      "/schema_registry_definitions",
      _ctx,
      json::serialization_format::schema_registry_v1_json,
      srlog,
      srreqs)
  , _store(store)
  , _writer(sequencer)
  , _controller(controller)
  , _audit_mgr(audit_mgr)
  , _ensure_started{[this]() { return do_start(); }}
  , _auth{
      config::always_true(),
      config::shard_local_cfg().superusers.bind(),
      controller.get()} {
    _inflight_config_binding.watch([this]() {
        const size_t capacity = _inflight_config_binding();
        _inflight_sem.set_capacity(capacity);
        _ctx.max_inflight = capacity;
    });
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

security::authorizer& service::authorizor() {
    return _controller->get_authorizer().local();
}

} // namespace pandaproxy::schema_registry
