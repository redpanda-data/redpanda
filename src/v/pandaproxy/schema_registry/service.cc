// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/service.h"

#include "kafka/client/client_fetch_batch_reader.h"
#include "kafka/protocol/create_topics.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/list_offsets.h"
#include "kafka/server/handlers/topics/types.h"
#include "model/fundamental.h"
#include "pandaproxy/api/api-doc/schema_registry.json.h"
#include "pandaproxy/error.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "pandaproxy/schema_registry/handlers.h"
#include "pandaproxy/schema_registry/storage.h"
#include "utils/gate_guard.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/std-coroutine.hh>
#include <seastar/http/api_docs.hh>

namespace pandaproxy::schema_registry {

using server = ctx_server<service>;

template<typename Handler>
auto wrap(ss::gate& g, one_shot& os, Handler h) {
    return [&g, &os, _h{std::move(h)}](
             server::request_t rq,
             server::reply_t rp) -> ss::future<server::reply_t> {
        auto h{_h};
        auto units = co_await os();
        auto guard = gate_guard(g);
        co_return co_await h(std::move(rq), std::move(rp));
    };
}

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

    return routes;
}

ss::future<> service::do_start() {
    auto guard = gate_guard(_gate);
    try {
        co_await create_internal_topic();
        co_await fetch_internal_topic();
        vlog(
          plog.info, "Schema registry successfully initialized internal topic");
    } catch (...) {
        vlog(
          plog.error,
          "Schema registry failed to initialize internal topic: {}",
          std::current_exception());
        throw;
    }
}

ss::future<> service::create_internal_topic() {
    // Use the default topic replica count, unless our specific setting
    // for the schema registry chooses to override it.
    int16_t replication_factor
      = _config.schema_registry_replication_factor().value_or(
        config::shard_local_cfg().default_topic_replication());

    vlog(
      plog.debug,
      "Schema registry: attempting to create internal topic (replication={})",
      replication_factor);

    auto make_internal_topic = [replication_factor]() {
        return kafka::creatable_topic{
          .name{model::schema_registry_internal_tp.topic},
          .num_partitions = 1,
          .replication_factor = replication_factor,
          .assignments{},
          .configs{
            {.name{ss::sstring{kafka::topic_property_cleanup_policy}},
             .value{"compact"}}}};
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
    const auto& topics = offset_res.data.topics;
    if (topics.size() != 1 || topics.front().partitions.size() != 1) {
        auto ec = kafka::error_code::unknown_topic_or_partition;
        throw kafka::exception(ec, make_error_code(ec).message());
    }
    const auto& partition = topics.front().partitions.front();
    if (partition.error_code != kafka::error_code::none) {
        auto ec = partition.error_code;
        throw kafka::exception(ec, make_error_code(ec).message());
    }

    auto max_offset = partition.offset;
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
  ss::sharded<seq_writer>& sequencer)
  : _config(config)
  , _mem_sem(max_memory)
  , _client(client)
  , _ctx{{{}, _mem_sem, {}, smp_sg}, *this}
  , _server(
      "schema_registry",
      ss::api_registry_builder20(_config.api_doc_dir(), "/v1"),
      "schema_registry_header",
      "/schema_registry_definitions",
      _ctx)
  , _store(store)
  , _writer(sequencer)
  , _ensure_started{[this]() { return do_start(); }} {}

ss::future<> service::start() {
    static std::vector<model::broker_endpoint> not_advertised{};
    _server.routes(get_schema_registry_routes(_gate, _ensure_started));
    return _server.start(
      _config.schema_registry_api(),
      _config.schema_registry_api_tls(),
      not_advertised,
      json::serialization_format::schema_registry_v1_json);
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
