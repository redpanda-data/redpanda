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
#include "model/metadata.h"
#include "seastarx.h"

#include <seastar/core/scheduling.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/file_handler.hh>
#include <seastar/http/httpd.hh>
#include <seastar/util/log.hh>

#include <absl/container/flat_hash_map.h>

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
      cluster::controller*,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&);

    ss::future<> start();
    ss::future<> stop();

    void set_ready() { _ready = true; }

private:
    /**
     * Prepend a / to the path component. This handles the case where path is an
     * empty string (e.g. url/) or when the path omits the root file path
     * directory (e.g. url/index.html vs url//index.html). The directory handler
     * in seastar is opininated and not very forgiving here so we help it a bit.
     */
    class dashboard_handler final : public ss::httpd::directory_handler {
    public:
        explicit dashboard_handler(const ss::sstring& dashboard_dir)
          : directory_handler(dashboard_dir) {}

        ss::future<std::unique_ptr<ss::httpd::reply>> handle(
          const ss::sstring& path,
          std::unique_ptr<ss::httpd::request> req,
          std::unique_ptr<ss::httpd::reply> rep) override {
            req->param.set("path", "/" + req->param.at("path"));
            return directory_handler::handle(
              path, std::move(req), std::move(rep));
        }
    };
    ss::future<> configure_listeners();
    void configure_dashboard();
    void configure_metrics_route();
    void configure_admin_routes();
    void register_config_routes();
    void register_raft_routes();
    void register_kafka_routes();
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
    cluster::controller* _controller;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    bool _ready{false};
};
