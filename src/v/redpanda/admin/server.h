/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/fwd.h"
#include "config/endpoint_tls_config.h"
#include "coproc/fwd.h"
#include "json/json.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <seastar/core/scheduling.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/file_handler.hh>
#include <seastar/http/httpd.hh>
#include <seastar/util/log.hh>

#include <absl/container/flat_hash_map.h>
#include <rapidjson/schema.h>

namespace admin {

inline ss::logger logger{"admin_api_server"};

struct admin_server_cfg {
    std::vector<model::broker_endpoint> endpoints;
    std::vector<config::endpoint_tls_config> endpoints_tls;
    std::optional<ss::sstring> dashboard_dir;
    ss::sstring admin_api_docs_dir;
    bool enable_admin_api;
    ss::scheduling_group sg;
};

class admin_server {
public:
    explicit admin_server(
      admin_server_cfg,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<coproc::partition_manager>&,
      cluster::controller*,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&);

    ss::future<> start();
    ss::future<> stop();

    void set_ready() { _ready = true; }

private:
    ss::future<> configure_listeners();
    void configure_dashboard();
    void configure_metrics_route();
    void configure_admin_routes();
    void register_config_routes();
    void register_cluster_config_routes();
    void register_raft_routes();
    void register_security_routes();
    void register_status_routes();
    void register_broker_routes();
    void register_partition_routes();
    void register_hbadger_routes();

    ss::future<> throw_on_error(
      ss::httpd::request& req,
      std::error_code ec,
      model::ntp const& ntp,
      model::node_id id = model::node_id{-1}) const;
    ss::future<ss::httpd::redirect_exception>
    redirect_to_leader(ss::httpd::request& req, model::ntp const& ntp) const;

    struct level_reset {
        using time_point = ss::timer<>::clock::time_point;

        level_reset(ss::log_level level, time_point expires)
          : level(level)
          , expires(expires) {}

        ss::log_level level;
        time_point expires;
    };

    ss::timer<> _log_level_timer;
    absl::flat_hash_map<ss::sstring, level_reset> _log_level_resets;

    void rearm_log_level_timer();
    void log_level_timer_handler();

    ss::http_server _server;
    admin_server_cfg _cfg;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<coproc::partition_manager>& _cp_partition_manager;
    cluster::controller* _controller;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    bool _ready{false};
};

struct json_validator {
    explicit json_validator(const std::string& schema_text)
      : schema(make_schema_document(schema_text))
      , validator(schema) {}

    static rapidjson::SchemaDocument
    make_schema_document(const std::string& schema);

    const rapidjson::SchemaDocument schema;
    rapidjson::SchemaValidator validator;
};

rapidjson::Document parse_json_body(ss::httpd::request const& req);
void apply_validator(json_validator& validator, rapidjson::Document const& doc);
bool get_boolean_query_param(
  const ss::httpd::request& req, std::string_view name);

} // namespace admin
