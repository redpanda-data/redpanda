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
#include "kafka/client/brokers.h"
#include "kafka/client/client_fetch_batch_reader.h"
#include "kafka/client/exceptions.h"
#include "kafka/protocol/create_topics.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/list_offsets.h"
#include "kafka/server/handlers/topics/types.h"
#include "model/fundamental.h"
#include "pandaproxy/api/api-doc/schema_registry.json.h"
#include "pandaproxy/auth_utils.h"
#include "pandaproxy/config_utils.h"
#include "pandaproxy/error.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "pandaproxy/schema_registry/handlers.h"
#include "pandaproxy/schema_registry/storage.h"
#include "pandaproxy/util.h"
#include "security/acl.h"
#include "security/ephemeral_credential_store.h"
#include "ssx/semaphore.h"
#include "utils/gate_guard.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/memory.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/http/api_docs.hh>
#include <seastar/http/exception.hh>
#include <seastar/util/noncopyable_function.hh>

namespace pandaproxy::schema_registry {

using server = ctx_server<service>;
const security::acl_principal principal{
  security::principal_type::ephemeral_user, "__schema_registry"};

class wrap {
public:
    wrap(ss::gate& g, one_shot& os, server::function_handler h)
      : _g{g}
      , _os{os}
      , _h{std::move(h)} {}

    ss::future<server::reply_t>
    operator()(server::request_t rq, server::reply_t rp) const {
        rq.authn_method = config::get_authn_method(
          rq.service().config().schema_registry_api.value(),
          rq.req->get_listener_idx());
        rq.user = maybe_authenticate_request(
          rq.authn_method, rq.service().authenticator(), *rq.req);

        auto units = co_await _os();
        auto guard = gate_guard(_g);
        try {
            co_return co_await _h(std::move(rq), std::move(rp));
        } catch (kafka::client::partition_error const& ex) {
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
    server::function_handler _h;
};

server::routes_t get_schema_registry_routes(ss::gate& gate, one_shot& es) {
    server::routes_t routes;
    routes.api = ss::httpd::schema_registry_json::name;

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_config, wrap(gate, es, get_config)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::put_config, wrap(gate, es, put_config)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_config_subject,
      wrap(gate, es, get_config_subject)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::put_config_subject,
      wrap(gate, es, put_config_subject)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_mode, wrap(gate, es, get_mode)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_schemas_types,
      wrap(gate, es, get_schemas_types)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_schemas_ids_id,
      wrap(gate, es, get_schemas_ids_id)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_schemas_ids_id_versions,
      wrap(gate, es, get_schemas_ids_id_versions)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_subjects,
      wrap(gate, es, get_subjects)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_subject_versions,
      wrap(gate, es, get_subject_versions)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::post_subject,
      wrap(gate, es, post_subject)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::post_subject_versions,
      wrap(gate, es, post_subject_versions)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_subject_versions_version,
      wrap(gate, es, get_subject_versions_version)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_subject_versions_version_schema,
      wrap(gate, es, get_subject_versions_version_schema)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::
        get_subject_versions_version_referenced_by,
      wrap(gate, es, get_subject_versions_version_referenced_by)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::delete_subject,
      wrap(gate, es, delete_subject)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::delete_subject_version,
      wrap(gate, es, delete_subject_version)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::compatibility_subject_version,
      wrap(gate, es, compatibility_subject_version)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::schema_registry_status_ready,
      wrap(gate, es, status_ready)});

    return routes;
}

ss::future<> service::do_start() {
    if (_is_started) {
        co_return;
    }
    auto guard = gate_guard(_gate);
    try {
        co_await configure();
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

ss::future<> service::configure() {
    auto config = co_await pandaproxy::create_client_credentials(
      *_controller,
      config::shard_local_cfg(),
      _client.local().config(),
      principal);
    co_await set_client_credentials(*config, _client);

    auto const& store = _controller->get_ephemeral_credential_store().local();
    bool has_ephemeral_credentials = store.has(store.find(principal));
    co_await container().invoke_on_all(
      _ctx.smp_sg, [has_ephemeral_credentials](service& s) {
          s._has_ephemeral_credentials = has_ephemeral_credentials;
      });
    co_await _controller->get_security_frontend().local().create_acls(
      {security::acl_binding{
        security::resource_pattern{
          security::resource_type::topic,
          model::schema_registry_internal_tp.topic,
          security::pattern_type::literal},
        security::acl_entry{
          principal,
          security::acl_host::wildcard_host(),
          security::acl_operation::all,
          security::acl_permission::allow}}},
      5s);
}

ss::future<> service::mitigate_error(std::exception_ptr eptr) {
    if (_gate.is_closed()) {
        // Return so that the client doesn't try to mitigate.
        return ss::now();
    }
    vlog(plog.warn, "mitigate_error: {}", eptr);
    return ss::make_exception_future<>(eptr).handle_exception_type(
      [this, eptr](kafka::client::broker_error const& ex) {
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
  std::unique_ptr<cluster::controller>& controller)
  : _config(config)
  , _mem_sem(max_memory, "pproxy/schema-svc")
  , _client(client)
  , _ctx{{{}, _mem_sem, {}, smp_sg}, *this}
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
  , _ensure_started{[this]() { return do_start(); }}
  , _auth{
      config::always_true(),
      config::shard_local_cfg().superusers.bind(),
      controller.get()} {}

ss::future<> service::start() {
    static std::vector<model::broker_endpoint> not_advertised{};
    _server.routes(get_schema_registry_routes(_gate, _ensure_started));
    return _server.start(
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
