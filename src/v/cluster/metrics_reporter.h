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

#include "cluster/fwd.h"
#include "cluster/health_monitor_types.h"
#include "cluster/members_table.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "features/fwd.h"
#include "http/client.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "raft/consensus.h"
#include "utils/prefix_logger.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include <cstdint>
#include <vector>

namespace cluster {

namespace details {
struct address {
    ss::sstring protocol;
    ss::sstring host;
    uint16_t port;
    ss::sstring path;
};

address parse_url(const ss::sstring&);

}; // namespace details

class metrics_reporter {
public:
    struct node_disk_space {
        uint64_t free;
        uint64_t total;
    };

    struct node_metrics {
        model::node_id id;
        uint32_t cpu_count;
        bool is_alive;
        ss::sstring version;
        cluster_version logical_version{invalid_version};
        std::vector<node_disk_space> disks;
        uint64_t uptime_ms;
    };

    struct metrics_snapshot {
        static constexpr int16_t version = 1;

        ss::sstring cluster_uuid;
        uint64_t cluster_creation_epoch;
        uint32_t topic_count;
        uint32_t partition_count;

        cluster_version active_logical_version{invalid_version};
        cluster_version original_logical_version{invalid_version};

        std::vector<node_metrics> nodes;
        bool has_kafka_gssapi;

        static constexpr int64_t max_size_for_rp_env = 80;
        ss::sstring redpanda_environment;
        ss::sstring id_hash;
    };
    static constexpr ss::shard_id shard = 0;

    metrics_reporter(
      consensus_ptr,
      ss::sharded<controller_stm>&,
      ss::sharded<members_table>&,
      ss::sharded<topic_table>&,
      ss::sharded<health_monitor_frontend>&,
      ss::sharded<config_frontend>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<ss::abort_source>&);

    ss::future<> start();
    ss::future<> stop();

private:
    void report_metrics();
    http::client::request_header make_header(const iobuf&);
    ss::future<> do_report_metrics();
    ss::future<result<metrics_snapshot>> build_metrics_snapshot();

    ss::future<http::client> make_http_client();
    ss::future<> try_initialize_cluster_info();
    ss::future<> propagate_cluster_id();

    consensus_ptr _raft0;
    metrics_reporter_cluster_info& _cluster_info; // owned by controller_stm
    ss::sharded<controller_stm>& _controller_stm;
    ss::sharded<members_table>& _members_table;
    ss::sharded<topic_table>& _topics;
    ss::sharded<health_monitor_frontend>& _health_monitor;
    ss::sharded<config_frontend>& _config_frontend;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<ss::abort_source>& _as;
    prefix_logger _logger;
    ss::timer<> _tick_timer;
    details::address _address;
    ss::gate _gate;

    ss::lowres_clock::time_point _last_success
      = ss::lowres_clock::time_point::min();
};
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
} // namespace json
