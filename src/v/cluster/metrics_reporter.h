/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/cluster_link/fwd.h"
#include "cluster/fwd.h"
#include "cluster/plugin_table.h"
#include "cluster/types.h"
#include "features/enterprise_features.h"
#include "features/fwd.h"
#include "http/client.h"
#include "model/fundamental.h"
#include "security/fwd.h"
#include "storage/fwd.h"
#include "utils/named_type.h"
#include "utils/notification_list.h"
#include "utils/prefix_logger.h"
#include "utils/unresolved_address.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/noncopyable_function.hh>

#include <cstdint>
#include <vector>

namespace cluster {

namespace details {
struct address {
    ss::sstring protocol;
    ss::sstring host;
    uint16_t port{0};
    ss::sstring path;
};

address parse_url(const ss::sstring&);

class metrics_http_client {
public:
    struct configs {
        address& addr;
        prefix_logger& logger;
        ss::abort_source& as;
    };

    static ss::future<> send_metrics(configs conf, iobuf);

private:
    ss::future<http::client> make_http_client();
    ss::future<> do_send_metrics(http::client&);

    metrics_http_client(configs conf, iobuf out)
      : _conf(conf)
      , _out(std::move(out)) {};

    configs _conf;
    iobuf _out;
};

}; // namespace details

class metrics_reporter {
public:
    struct node_disk_space {
        uint64_t free{0};
        uint64_t total{0};
    };

    struct node_metrics {
        model::node_id id;
        uint32_t cpu_count{0};
        bool is_alive{false};
        ss::sstring version;
        cluster_version logical_version{invalid_version};
        std::vector<node_disk_space> disks;
        uint64_t uptime_ms{0};
        std::vector<net::unresolved_address> advertised_listeners;
    };

    struct kubernetes_metrics {
        std::optional<ss::sstring> deployment_type;
        std::optional<ss::sstring> chart_version;
        std::optional<ss::sstring> operator_image_version;
        std::optional<ss::sstring> k8s_version;
        std::optional<ss::sstring> k8s_environment;
        std::optional<ss::sstring> k8s_cluster_id;
    };

    struct schema_registry_metrics {
        uint32_t context_count{0};
    };

    struct metrics_snapshot {
        ss::sstring cluster_uuid;
        ss::sstring storage_uuid;
        uint64_t cluster_creation_epoch{0};
        uint32_t topic_count{0};
        uint32_t partition_count{0};

        uint32_t topics_with_iceberg_kv{0};
        uint32_t topics_with_iceberg_schema_id{0};
        uint32_t topics_with_iceberg_schema_latest{0};

        cluster_version active_logical_version{invalid_version};
        cluster_version original_logical_version{invalid_version};

        std::vector<node_metrics> nodes;
        bool has_kafka_gssapi{false};
        bool has_oidc{false};
        uint32_t rbac_role_count{0};
        uint32_t unique_group_count{0};
        uint32_t data_transforms_count{0};

        static constexpr int64_t max_size_for_rp_env = 80;
        ss::sstring redpanda_environment;
        ss::sstring id_hash;

        bool has_enterprise_features{false};
        bool has_valid_license{false};

        std::optional<features::enterprise_feature_report> enterprise_features;

        ss::sstring host_name;
        ss::sstring domain_name;
        std::vector<ss::sstring> fqdns;

        uint32_t number_of_active_shadow_links{0};
        uint32_t number_of_shadow_topics{0};
        bool schema_registry_shadowed{false};

        std::optional<kubernetes_metrics> kubernetes;

        // Schema Registry metrics (nullopt when SR not configured)
        std::optional<schema_registry_metrics> schema_registry;
    };

    /// Callback type for external subsystems to contribute metrics data.
    using metrics_contributor_fn
      = ss::noncopyable_function<ss::future<>(metrics_snapshot&)>;

    /// ID type returned by register_metrics_contributor for later
    /// unregistration.
    using metrics_contributor_id
      = named_type<int32_t, struct metrics_contributor_id_tag>;

    static constexpr ss::shard_id shard = 0;

    metrics_reporter(
      consensus_ptr,
      ss::sharded<controller_stm>&,
      ss::sharded<members_table>&,
      ss::sharded<topic_table>&,
      ss::sharded<health_monitor_frontend>&,
      ss::sharded<config_frontend>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<security::role_store>& role_store,
      ss::sharded<security::authorizer>& authorizer,
      ss::sharded<plugin_table>*,
      ss::sharded<feature_manager>*,
      ss::sharded<storage::api>*,
      ss::sharded<cluster_link::frontend>*,
      ss::sharded<ss::abort_source>&);

    ss::future<> start();
    ss::future<> stop();

    ss::future<> wait_cluster_info_initialized(ss::abort_source&);

    /// Register a callback that will be invoked during metrics collection
    /// to populate additional fields in the snapshot.
    /// Returns an ID that can be used to unregister the contributor.
    metrics_contributor_id register_metrics_contributor(metrics_contributor_fn);

    /// Unregister a previously registered metrics contributor.
    void unregister_metrics_contributor(metrics_contributor_id);

private:
    void report_metrics();
    ss::future<> do_report_metrics();
    ss::future<result<metrics_snapshot>> build_metrics_snapshot();

    ss::future<> try_initialize_cluster_info();
    ss::future<> propagate_cluster_id();

    consensus_ptr _raft0;
    metrics_reporter_cluster_info& _cluster_info; // owned by controller_stm
    ss::condition_variable _cluster_info_initialized_cvar;
    ss::sharded<controller_stm>& _controller_stm;
    ss::sharded<members_table>& _members_table;
    ss::sharded<topic_table>& _topics;
    ss::sharded<health_monitor_frontend>& _health_monitor;
    ss::sharded<config_frontend>& _config_frontend;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<security::role_store>& _role_store;
    ss::sharded<security::authorizer>& _authorizer;
    ss::sharded<plugin_table>* _plugin_table;
    ss::sharded<feature_manager>* _feature_manager;
    ss::sharded<storage::api>* _storage;
    ss::sharded<cluster_link::frontend>* _clfe;
    ss::sharded<ss::abort_source>& _as;
    prefix_logger _logger;
    ss::timer<ss::lowres_clock> _tick_timer;
    details::address _address;
    ss::gate _gate;

    ss::lowres_clock::time_point _last_success
      = ss::lowres_clock::time_point::min();

    notification_list<metrics_contributor_fn, metrics_contributor_id>
      _metrics_contributors;
};

std::optional<metrics_reporter::kubernetes_metrics> get_kubernetes_metrics();

} // namespace cluster
namespace json {
void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::metrics_reporter::metrics_snapshot& v);
void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::metrics_reporter::node_disk_space& v);
void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::metrics_reporter::node_metrics& v);
void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::metrics_reporter::kubernetes_metrics& v);
void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::metrics_reporter::schema_registry_metrics& v);
} // namespace json
