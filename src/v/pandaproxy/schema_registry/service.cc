// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/service.h"

#include "cluster/cluster_link/frontend.h"
#include "cluster/controller.h"
#include "config/configuration.h"
#include "kafka/data/rpc/deps.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/list_offset.h"
#include "kafka/server/handlers/topics/types.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "pandaproxy/api/api-doc/schema_registry.json.hh"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/auth.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/handlers.h"
#include "pandaproxy/schema_registry/storage.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/util.h"
#include "security/acl.h"
#include "security/audit/audit_log_manager.h"
#include "security/authorizer.h"
#include "security/request_auth.h"
#include "ssx/semaphore.h"
#include "utils/tristate.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/sleep.hh>
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
              return h(std::move(rq), std::move(rp), std::move(auth_result));
          });
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
      std::nullopt,
      auth::deferred{},
      get_config_subject));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::put_config_subject,
      auth::level::user,
      std::nullopt,
      auth::deferred{},
      put_config_subject));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::delete_config_subject,
      auth::level::user,
      std::nullopt,
      auth::deferred{},
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
      std::nullopt,
      auth::deferred{},
      get_mode_subject));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::put_mode_subject,
      auth::level::superuser,
      std::nullopt,
      auth::deferred{},
      put_mode_subject));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::delete_mode_subject,
      auth::level::superuser,
      std::nullopt,
      auth::deferred{},
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
      ss::httpd::schema_registry_json::get_schemas_ids_id_schema,
      auth::level::user,
      std::nullopt,
      auth::deferred{},
      get_schemas_ids_id_schema));

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
      ss::httpd::schema_registry_json::get_contexts,
      auth::level::user,
      std::nullopt,
      auth::deferred{},
      get_contexts));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::delete_context,
      auth::level::superuser,
      acl_operation::remove,
      registry_resource{},
      delete_context));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::get_subject_versions,
      auth::level::user,
      acl_operation::describe,
      context_subject{},
      get_subject_versions));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::post_subject,
      auth::level::user,
      acl_operation::read,
      context_subject{},
      post_subject));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::post_subject_versions,
      auth::level::user,
      acl_operation::write,
      context_subject{},
      post_subject_versions));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::get_subject_versions_version,
      auth::level::user,
      acl_operation::read,
      context_subject{},
      get_subject_versions_version));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::get_subject_versions_version_schema,
      auth::level::user,
      acl_operation::read,
      context_subject{},
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
      context_subject{},
      delete_subject));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::delete_subject_version,
      auth::level::user,
      acl_operation::remove,
      context_subject{},
      delete_subject_version));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::compatibility_subject_version,
      auth::level::user,
      acl_operation::read,
      context_subject{},
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
    co_await container().invoke_on_all(
      _ctx.smp_sg, [](this auto, service& s) -> ss::future<> {
          s._is_started = true;
          if (ss::this_shard_id() != seq_writer::reader_shard) {
              co_return;
          }
          // create_internal_topic returns once the controller commits the
          // topic, but the metadata cache is updated asynchronously. Retry
          // fetch_internal_topic on topic_not_exists to ride out this race.
          for (int attempts = 0;; ++attempts) {
              auto fut = co_await ss::coroutine::as_future(
                s.fetch_internal_topic());
              if (fut.available()) {
                  co_return;
              }
              bool retriable = false;
              try {
                  std::rethrow_exception(fut.get_exception());
              } catch (const exception& e) {
                  retriable = e.code()
                                == kafka::error_code::unknown_topic_or_partition
                              && attempts < 10;
                  if (!retriable) {
                      throw;
                  }
              }
              co_await ss::sleep(std::chrono::milliseconds(100));
          }
      });
}

ss::future<> service::create_internal_topic() {
    auto topic_cfg = _topic_metadata_cache->find_topic_cfg(
      {model::kafka_namespace, model::schema_registry_internal_tp.topic});
    if (topic_cfg.has_value()) {
        vlog(srlog.debug, "Schema registry: found internal topic");
        co_return;
    }

    // Use the default topic replica count, unless our specific setting
    // for the schema registry chooses to override it.
    int16_t replication_factor
      = _config.schema_registry_replication_factor().value_or(
        _controller->internal_topic_replication());

    co_await _transport->validate_topic_creation_authorization(
      replication_factor);

    // If shadow linking is active and a link is actively mirroring the
    // schema registry topic, then we will not create the topic and we will
    // throw an error.  This is so the oneshot doesn't become 'completed'.
    if (active_sr_mirroring()) {
        throw std::runtime_error(
          "Shadow Linking actively mirroring schema "
          "registry topic.  Topic will not be created");
    }

    // Create the base topic configuration to get the cluster defaults
    auto base_topic_config = kafka::to_topic_config(
      model::kafka_namespace,
      model::schema_registry_internal_tp.topic,
      /*partition_count=*/1,
      replication_factor,
      {});
    // Now update the properties
    base_topic_config.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    base_topic_config.properties.compression = model::compression::none;
    base_topic_config.properties.retention_bytes = tristate<size_t>{
      disable_tristate};
    base_topic_config.properties.retention_duration
      = tristate<std::chrono::milliseconds>{disable_tristate};
    base_topic_config.properties.retention_local_target_bytes
      = tristate<size_t>{disable_tristate};
    base_topic_config.properties.retention_local_target_ms
      = tristate<std::chrono::milliseconds>{disable_tristate};
    base_topic_config.properties.initial_retention_local_target_bytes
      = tristate<size_t>{disable_tristate};
    base_topic_config.properties.initial_retention_local_target_ms
      = tristate<std::chrono::milliseconds>{disable_tristate};

    vlog(
      srlog.debug,
      "Schema registry: attempting to create internal topic "
      "(replication={}, "
      "properties={})",
      replication_factor,
      base_topic_config.properties);

    auto res = co_await _topic_creator->create_topic(
      {model::kafka_namespace, model::schema_registry_internal_tp.topic},
      1,
      std::move(base_topic_config.properties),
      replication_factor);

    if (res == cluster::errc::success) {
        vlog(srlog.debug, "Schema registry: created internal topic");
    } else if (res == cluster::errc::topic_already_exists) {
        vlog(srlog.debug, "Schema registry: found internal topic");
    } else {
        throw std::runtime_error(
          fmt::format("Failed to create internal topic: {}", res));
    }

    // TODO(Ben): Validate the _schemas topic
}

ss::future<> service::fetch_internal_topic() {
    vlog(srlog.debug, "Schema registry: loading internal topic");

    // TODO: should check the replication_factor of the topic is
    // what our config calls for

    auto max_offset = co_await _transport->get_high_watermark();
    vlog(srlog.debug, "Schema registry: _schemas max_offset: {}", max_offset);

    co_await _transport->consume_range(
      model::offset{0},
      max_offset,
      [this](this auto, model::record_batch batch) -> ss::future<> {
          consume_to_store c{_store, writer()};
          co_await c(std::move(batch));
      });

    // If a schema failed to be compiled, it will be marked. We attempt to
    // reprocess them once now that the whole topic has been read, in case
    // they have a reference to a schema declared later in the topic.
    co_await _store.process_marked_schemas();
}

bool service::active_sr_mirroring() const {
    return _controller->get_cluster_link_frontend()
      .local()
      .schema_registry_shadowing_active();
}

service::service(
  const YAML::Node& config,
  ss::smp_service_group smp_sg,
  size_t max_memory,
  schema_registry::transport* transport,
  sharded_store& store,
  ss::sharded<seq_writer>& sequencer,
  std::unique_ptr<kafka::data::rpc::topic_metadata_cache> topic_metadata_cache,
  std::unique_ptr<kafka::data::rpc::topic_creator> topic_creator,
  std::unique_ptr<cluster::controller>& controller,
  ss::sharded<security::audit::audit_log_manager>& audit_mgr)
  : _config(config)
  , _mem_sem(max_memory, "pproxy/schema-svc")
  , _inflight_sem(
      config::shard_local_cfg()
        .max_in_flight_schema_registry_requests_per_shard())
  , _inflight_config_binding(
      config::shard_local_cfg()
        .max_in_flight_schema_registry_requests_per_shard.bind())
  , _transport(transport)
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
  , _topic_metadata_cache(std::move(topic_metadata_cache))
  , _topic_creator(std::move(topic_creator))
  , _controller(controller)
  , _audit_mgr(audit_mgr)
  , _ensure_started{[this]() { return do_start(); }}
  , _auth{
      config::always_true(),
      config::shard_local_cfg().superusers.bind(),
      controller.get()} {
    vassert(_transport != nullptr, "Transport unexpectedly null");
    _inflight_config_binding.watch([this]() {
        const size_t capacity = _inflight_config_binding();
        _inflight_sem.set_capacity(capacity);
        _ctx.max_inflight = capacity;
    });
}

ss::future<> service::start() {
    co_await _transport->configure();
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
