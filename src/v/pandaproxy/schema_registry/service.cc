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
#include "kafka/protocol/exceptions.h"
#include "kafka/server/handlers/topics/types.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "pandaproxy/api/api-doc/schema_registry.json.hh"
#include "pandaproxy/logger.h"
#include "pandaproxy/parsing/httpd.h"
#include "pandaproxy/schema_registry/auth.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "pandaproxy/schema_registry/context_router.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/handlers.h"
#include "pandaproxy/schema_registry/seq_writer.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/storage.h"
#include "pandaproxy/schema_registry/transport.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/util.h"
#include "security/acl.h"
#include "security/audit/audit_log_manager.h"
#include "security/authorizer.h"
#include "security/request_auth.h"
#include "ssx/future-util.h"
#include "ssx/semaphore.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/sleep.hh>
#include <seastar/http/api_docs.hh>
#include <seastar/util/log.hh>

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
              return h(
                std::move(rq),
                std::move(rp),
                std::move(auth_result),
                _operation_name);
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
                  auth::route_resource res,
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

    // Context-prefixed route helpers: extract and normalize the {context}
    // path parameter, apply the given URL-rewriting function, then delegate
    // to the handler.
    auto ctx_route = [](auto scope_fn, auto handler) {
        return [=](
                 server::request_t rq,
                 server::reply_t rp) -> ss::future<server::reply_t> {
            auto ctx = parse_normalized_context(*rq.req);
            scope_fn(*rq.req, ctx);
            return handler(std::move(rq), std::move(rp));
        };
    };

    auto ctx_deferred_route = [](auto scope_fn, auto handler) {
        return
          [=](
            server::request_t rq,
            server::reply_t rp,
            std::optional<request_auth_result> auth_result,
            std::string_view operation_name) -> ss::future<server::reply_t> {
              auto ctx = parse_normalized_context(*rq.req);
              scope_fn(*rq.req, ctx);
              return handler(
                std::move(rq),
                std::move(rp),
                std::move(auth_result),
                operation_name);
          };
    };

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_post_subject,
      auth::level::user,
      acl_operation::read,
      auth::context_prefix_subject{},
      ctx_route(scope_subject_param, post_subject)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_delete_subject,
      auth::level::user,
      acl_operation::remove,
      auth::context_prefix_subject{},
      ctx_route(scope_subject_param, delete_subject)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_get_subject_versions,
      auth::level::user,
      acl_operation::describe,
      auth::context_prefix_subject{},
      ctx_route(scope_subject_param, get_subject_versions)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_post_subject_versions,
      auth::level::user,
      acl_operation::write,
      auth::context_prefix_subject{},
      ctx_route(scope_subject_param, post_subject_versions)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_get_subject_versions_version,
      auth::level::user,
      acl_operation::read,
      auth::context_prefix_subject{},
      ctx_route(scope_subject_param, get_subject_versions_version)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_get_subject_versions_version_schema,
      auth::level::user,
      acl_operation::read,
      auth::context_prefix_subject{},
      ctx_route(scope_subject_param, get_subject_versions_version_schema)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::
        ctx_get_subject_versions_version_referenced_by,
      auth::level::user,
      acl_operation::describe,
      registry_resource{},
      ctx_route(
        scope_subject_param, get_subject_versions_version_referenced_by)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::
        ctx_get_subject_versions_version_referenced_by_deprecated,
      auth::level::user,
      acl_operation::describe,
      registry_resource{},
      ctx_route(
        scope_subject_param, get_subject_versions_version_referenced_by)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_delete_subject_version,
      auth::level::user,
      acl_operation::remove,
      auth::context_prefix_subject{},
      ctx_route(scope_subject_param, delete_subject_version)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_compatibility_subject_version,
      auth::level::user,
      acl_operation::read,
      auth::context_prefix_subject{},
      ctx_route(scope_subject_param, compatibility_subject_version)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_get_config_subject,
      auth::level::user,
      std::nullopt,
      auth::deferred{},
      ctx_deferred_route(scope_subject_param, get_config_subject)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_put_config_subject,
      auth::level::user,
      std::nullopt,
      auth::deferred{},
      ctx_deferred_route(scope_subject_param, put_config_subject)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_delete_config_subject,
      auth::level::user,
      std::nullopt,
      auth::deferred{},
      ctx_deferred_route(scope_subject_param, delete_config_subject)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_get_mode_subject,
      auth::level::user,
      std::nullopt,
      auth::deferred{},
      ctx_deferred_route(scope_subject_param, get_mode_subject)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_put_mode_subject,
      auth::level::superuser,
      std::nullopt,
      auth::deferred{},
      ctx_deferred_route(scope_subject_param, put_mode_subject)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_delete_mode_subject,
      auth::level::superuser,
      std::nullopt,
      auth::deferred{},
      ctx_deferred_route(scope_subject_param, delete_mode_subject)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_get_schemas_ids_id,
      auth::level::user,
      std::nullopt,
      auth::deferred{},
      ctx_deferred_route(scope_subject_query, get_schemas_ids_id)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_get_schemas_ids_id_schema,
      auth::level::user,
      std::nullopt,
      auth::deferred{},
      ctx_deferred_route(scope_subject_query, get_schemas_ids_id_schema)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_get_schemas_ids_id_versions,
      auth::level::user,
      acl_operation::describe,
      registry_resource{},
      ctx_route(scope_subject_query, get_schemas_ids_id_versions)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_get_schemas_ids_id_subjects,
      auth::level::user,
      acl_operation::describe,
      registry_resource{},
      ctx_route(scope_subject_query, get_schemas_ids_id_subjects)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_get_subjects,
      auth::level::user,
      std::nullopt,
      auth::deferred{},
      ctx_deferred_route(scope_subject_prefix_query, get_subjects)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_get_config,
      auth::level::user,
      std::nullopt,
      auth::deferred{},
      ctx_deferred_route(inject_context_as_subject, get_config_subject)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_put_config,
      auth::level::user,
      std::nullopt,
      auth::deferred{},
      ctx_deferred_route(inject_context_as_subject, put_config_subject)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_get_mode,
      auth::level::user,
      std::nullopt,
      auth::deferred{},
      ctx_deferred_route(inject_context_as_subject, get_mode_subject)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_put_mode,
      auth::level::superuser,
      std::nullopt,
      auth::deferred{},
      ctx_deferred_route(inject_context_as_subject, put_mode_subject)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_get_schemas_types,
      auth::level::publik,
      acl_operation::read,
      auth::none{},
      // Schema types are global — the handler ignores the context.
      // ctx_route validates the {context} param for consistency with the
      // other /contexts/{context}/... routes.
      ctx_route(
        [](ss::http::request&, std::string_view) {}, get_schemas_types)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_delete_config,
      auth::level::user,
      std::nullopt,
      auth::deferred{},
      ctx_deferred_route(inject_context_as_subject, delete_config_subject)));

    routes.routes.emplace_back(wrap(
      ss::httpd::schema_registry_json::ctx_delete_mode,
      auth::level::superuser,
      std::nullopt,
      auth::deferred{},
      ctx_deferred_route(inject_context_as_subject, delete_mode_subject)));

    return routes;
}

ss::future<> service::ensure_topic_loaded() {
    // `service` is a peering_sharded_service, so each shard has its own
    // `_ensure_started` one_shot. If the first requests after start-up land on
    // several shards at once, each shard's one_shot would independently run
    // do_start() and launch its own full `_schemas` replay on the reader
    // shard. Redundant replays each pay the full recovery cost, so N racing
    // shards make recovery up to N times slower.
    //
    // Funnel all shards through a single one_shot on the reader shard, so the
    // topic is loaded exactly once. The per-shard `_ensure_started` one_shot
    // still caches completion, so once started this costs nothing on later
    // requests; only the first request on each shard pays the cross-shard hop.
    return container().invoke_on(
      seq_writer::reader_shard, _ctx.smp_sg, [](service& s) {
          return s._load_once();
      });
}

ss::future<> service::do_start() {
    // Invoked only on the reader shard, via _load_once, so it runs exactly
    // once and can drive the reader-shard-only fetch directly.
    vassert(
      ss::this_shard_id() == seq_writer::reader_shard,
      "do_start must run on the reader shard, not {}",
      ss::this_shard_id());
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
    using namespace std::chrono_literals;

    // create_internal_topic returns once the controller commits the topic,
    // but the metadata cache and partition leadership are established
    // asynchronously. Retry transient errors in that window with exponential
    // backoff (100ms..5s, ~26s total).
    constexpr int max_attempts = 10;
    constexpr auto max_backoff = 5000ms;
    auto backoff = 100ms;
    for (int attempts = 0;; ++attempts) {
        auto fut = co_await ss::coroutine::as_future(fetch_internal_topic());
        if (!fut.failed()) {
            co_return;
        }
        auto eptr = fut.get_exception();
        if (attempts >= max_attempts) {
            std::rethrow_exception(eptr);
        }
        try {
            std::rethrow_exception(eptr);
        } catch (const kafka::exception_base& e) {
            if (!kafka::is_retriable(e.error)) {
                throw;
            }
        } catch (const exception& e) {
            // kafka_client_transport wraps "topic missing" as
            // unknown_server_error via schema_registry::exception.
            // treat this as retriable and rethrow anything else.
            if (e.code() != kafka::error_code::unknown_server_error) {
                throw;
            }
        }
        vlog(
          srlog.info,
          "Retriable error encountered while initializing the schemas "
          "topic: {}. Retrying in {}ms",
          eptr,
          backoff.count());
        try {
            co_await ss::sleep_abortable(backoff, _as);
        } catch (const ss::sleep_aborted&) {
            std::rethrow_exception(eptr);
        }
        backoff = std::min(backoff * 2, max_backoff);
    }
}

ss::future<> service::ensure_internal_topic() {
    auto guard = _gate.hold();
    co_await create_internal_topic();
}

ss::future<> service::create_internal_topic() {
    auto topic_cfg = _topic_metadata_cache->find_topic_cfg(
      {model::kafka_namespace, model::schema_registry_internal_tp.topic});
    if (topic_cfg.has_value()) {
        vlog(srlog.debug, "Schema registry: found internal topic");
        co_return;
    }

    // If shadow linking is active and a link is actively mirroring the
    // schema registry topic, then we will not create the topic and we will
    // throw an error.  This is so the oneshot doesn't become 'completed'.
    // API sync needs a local _schemas topic; topic mirroring does not.
    if (
      _controller->get_cluster_link_frontend()
        .local()
        .schema_registry_local_topic_writes_disabled()) {
        throw std::runtime_error(
          "Shadow Linking actively mirroring schema "
          "registry topic.  Topic will not be created");
    }

    // Use the default topic replica count, unless our specific setting
    // for the schema registry chooses to override it.
    int16_t replication_factor
      = _config.schema_registry_replication_factor().value_or(
        _controller->internal_topic_replication());

    auto base_topic_config = kafka::schema_registry_topic_configuration(
      replication_factor);

    vlog(
      srlog.debug,
      "Schema registry: attempting to create internal topic "
      "(replication={}, "
      "properties={})",
      replication_factor,
      base_topic_config.properties);

    auto res = co_await _transport->create_topic(
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

    using namespace std::chrono_literals;
    const auto defer = defer_processing{
      config::shard_local_cfg().schema_registry_deferred_recovery()};
    auto replay_start = ss::lowres_clock::now();
    co_await _transport->consume_range(
      model::offset{0}, max_offset, consume_to_store{_store, writer(), defer});
    auto replay_done = ss::lowres_clock::now();

    co_await _store.process_marked_schemas();

    vlog(
      srlog.info,
      "Schema registry recovery complete: replayed to offset {} in {}ms "
      "(deferred={}), canonicalised marked schemas in {}ms",
      max_offset,
      (replay_done - replay_start) / 1ms,
      defer == defer_processing::yes,
      (ss::lowres_clock::now() - replay_done) / 1ms);
}

service::service(
  const YAML::Node& config,
  ss::smp_service_group smp_sg,
  size_t max_memory,
  schema_registry::transport& transport,
  sharded_store& store,
  ss::sharded<seq_writer>& sequencer,
  std::unique_ptr<kafka::data::rpc::topic_metadata_cache> topic_metadata_cache,
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
  , _transport(&transport)
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
  , _controller(controller)
  , _audit_mgr(audit_mgr)
  , _ensure_started{[this]() { return ensure_topic_loaded(); }}
  , _load_once{[this]() { return do_start(); }}
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
    static std::vector<model::broker_endpoint> not_advertised{};
    _server.routes(get_schema_registry_routes(_gate, _ensure_started));
    co_await _server.start(
      _config.schema_registry_api(),
      _config.schema_registry_api_tls(),
      not_advertised);

    if (
      ss::this_shard_id() == seq_writer::reader_shard
      && config::shard_local_cfg().schema_registry_replay_on_startup()) {
        // Proactively hydrate the store rather than wait for the first request.
        // Fire-and-forget under the gate: a large topic can take a while to
        // replay and must not block broker start-up. This goes through the
        // single-flight ensure_started(), so a request that races it still
        // triggers exactly one replay.
        ssx::spawn_with_gate(_gate, [this] {
            return ensure_started().handle_exception(
              [](const std::exception_ptr& e) {
                  vlog(
                    srlog.warn,
                    "eager _schemas replay failed at start-up; will retry on "
                    "the first request: {}",
                    e);
              });
        });
    }
}

ss::future<> service::stop() {
    _as.request_abort();
    co_await _gate.close();
    co_await _server.stop();
}

configuration& service::config() { return _config; }

security::authorizer& service::authorizor() {
    return _controller->get_authorizer().local();
}

} // namespace pandaproxy::schema_registry
