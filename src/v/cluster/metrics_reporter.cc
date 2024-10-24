/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/metrics_reporter.h"

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cluster/config_frontend.h"
#include "cluster/controller_stm.h"
#include "cluster/feature_manager.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/health_monitor_types.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/validators.h"
#include "features/enterprise_features.h"
#include "hashing/secure.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "model/namespace.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "net/tls.h"
#include "net/tls_certificate_probe.h"
#include "reflection/adl.h"
#include "rpc/types.h"
#include "security/role_store.h"
#include "ssx/sformat.h"
#include "utils/unresolved_address.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/net/tls.hh>

#include <absl/algorithm/container.h>
#include <absl/container/node_hash_map.h>
#include <boost/lexical_cast.hpp>
#include <boost/random/seed_seq.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/core.h>

#include <stdexcept>

namespace cluster {

namespace details {
address parse_url(const ss::sstring& url) {
    address ret;

    auto proto_delimiter = url.find("://");
    ret.protocol = url.substr(0, proto_delimiter);
    if (proto_delimiter == ss::sstring::npos) {
        throw std::invalid_argument(
          fmt::format("unable to parse url: {}", url));
    }
    proto_delimiter += 3; // skip over '://'
    auto port_delimiter = url.find(":", proto_delimiter);
    auto path_delimiter = url.find("/", proto_delimiter);
    // default port
    if (ret.protocol == "http") {
        ret.port = 80;
    } else if (ret.protocol == "https") {
        ret.port = 443;
    } else {
        throw std::invalid_argument(
          fmt::format("unable to parse url: {}", url));
    }

    if (port_delimiter == ss::sstring::npos) {
        ret.host = url.substr(
          proto_delimiter, path_delimiter - proto_delimiter);
    } else {
        auto str = url.substr(
          port_delimiter + 1, path_delimiter - (port_delimiter + 1));

        // parse port
        ret.port = boost::lexical_cast<int16_t>(str);

        ret.host = url.substr(
          proto_delimiter, port_delimiter - proto_delimiter);
    }

    if (path_delimiter != ss::sstring::npos) {
        ret.path = url.substr(path_delimiter);
    }
    return ret;
}
} // namespace details

static ss::logger logger("metrics-reporter");

metrics_reporter::metrics_reporter(
  consensus_ptr raft0,
  ss::sharded<controller_stm>& controller_stm,
  ss::sharded<members_table>& members_table,
  ss::sharded<topic_table>& topic_table,
  ss::sharded<health_monitor_frontend>& health_monitor,
  ss::sharded<config_frontend>& config_frontend,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<security::role_store>& role_store,
  ss::sharded<plugin_table>* pt,
  ss::sharded<feature_manager>* fm,
  ss::sharded<ss::abort_source>& as)
  : _raft0(std::move(raft0))
  , _cluster_info(controller_stm.local().get_metrics_reporter_cluster_info())
  , _controller_stm(controller_stm)
  , _members_table(members_table)
  , _topics(topic_table)
  , _health_monitor(health_monitor)
  , _config_frontend(config_frontend)
  , _feature_table(feature_table)
  , _role_store(role_store)
  , _plugin_table(pt)
  , _feature_manager(fm)
  , _as(as)
  , _logger(logger, "metrics-reporter") {}

ss::future<> metrics_reporter::start() {
    vlog(clusterlog.trace, "starting metrics reporter");
    _address = details::parse_url(
      config::shard_local_cfg().metrics_reporter_url());
    _tick_timer.set_callback([this] { report_metrics(); });

    _last_success = model::timeout_clock::now();

    // Immediately enter the report loop, as on a fresh cluster this will
    // be what initializes the cluster UUId.  It will not actually transmit
    // a report on the first call, because the interval has not yet elapsed
    // since the _last_success we just set to now().
    // It is important to do this promptly and not wait here, because the
    // controller cannot generate snapshots until the cluster UUID
    // is initialized.
    report_metrics();

    co_return;
}

ss::future<> metrics_reporter::stop() {
    vlog(clusterlog.info, "Stopping Metrics Reporter...");
    _tick_timer.cancel();
    co_await _gate.close();
}

void metrics_reporter::report_metrics() {
    ssx::background = ssx::spawn_with_gate_then(_gate, [this] {
                          return do_report_metrics().finally([this] {
                              if (!_gate.is_closed()) {
                                  _tick_timer.arm(
                                    config::shard_local_cfg()
                                      .metrics_reporter_tick_interval());
                              }
                          });
                      }).handle_exception([](const std::exception_ptr& e) {
        vlog(clusterlog.warn, "Exception reporting metrics: {}", e);
    });
}

ss::future<result<metrics_reporter::metrics_snapshot>>
metrics_reporter::build_metrics_snapshot() {
    metrics_snapshot snapshot;

    snapshot.cluster_uuid = _cluster_info.uuid;
    snapshot.cluster_creation_epoch = _cluster_info.creation_timestamp.value();

    absl::node_hash_map<model::node_id, node_metrics> metrics_map;

    auto report = co_await _health_monitor.local().get_cluster_health(
      cluster_report_filter{
        .node_report_filter
        = node_report_filter{.include_partitions = include_partitions_info::no}},
      force_refresh::no,
      config::shard_local_cfg().metrics_reporter_report_interval()
        + ss::lowres_clock::now());

    if (!report) {
        co_return result<metrics_snapshot>(report.error());
    }
    metrics_map.reserve(report.value().node_reports.size());

    for (auto& report : report.value().node_reports) {
        auto [it, _] = metrics_map.emplace(
          report->id, node_metrics{.id = report->id});
        auto& metrics = it->second;

        auto nm = _members_table.local().get_node_metadata_ref(report->id);
        if (!nm) {
            continue;
        }
        metrics.cpu_count = nm->get().broker.properties().cores;
        metrics.is_alive = _health_monitor.local().is_alive(report->id)
                           == cluster::alive::yes;
        metrics.version = report->local_state.redpanda_version;
        metrics.logical_version = report->local_state.logical_version;
        metrics.disks.reserve(report->local_state.shared_disk() ? 1 : 2);
        auto transform_disk = [](const storage::disk& d) -> node_disk_space {
            return node_disk_space{.free = d.free, .total = d.total};
        };
        metrics.disks.push_back(transform_disk(report->local_state.data_disk));
        if (!report->local_state.shared_disk()) {
            metrics.disks.push_back(
              transform_disk(*(report->local_state.cache_disk)));
        }

        metrics.uptime_ms = report->local_state.uptime / 1ms;
    }
    auto& topics = _topics.local().topics_map();
    snapshot.topic_count = 0;
    snapshot.partition_count = 0;
    for (const auto& [tp_ns, md] : topics) {
        // do not include internal topics
        if (
          tp_ns.ns == model::redpanda_ns
          || tp_ns.ns == model::kafka_internal_namespace) {
            continue;
        }

        snapshot.topic_count++;
        snapshot.partition_count += md.get_configuration().partition_count;
    }

    snapshot.nodes.reserve(metrics_map.size());
    for (auto& [_, m] : metrics_map) {
        snapshot.nodes.push_back(std::move(m));
    }

    snapshot.active_logical_version
      = _feature_table.local().get_active_version();
    snapshot.original_logical_version
      = _feature_table.local().get_original_version();

    auto feature_report = co_await _feature_manager->invoke_on(
      cluster::feature_manager::backend_shard,
      [](const cluster::feature_manager& fm) {
          return fm.report_enterprise_features();
      });

    snapshot.has_kafka_gssapi = feature_report.test(
      features::license_required_feature::gssapi);

    snapshot.has_oidc = feature_report.test(
      features::license_required_feature::oidc);

    snapshot.rbac_role_count = _role_store.local().size();

    snapshot.data_transforms_count = _plugin_table->local().size();

    auto env_value = std::getenv("REDPANDA_ENVIRONMENT");
    if (env_value) {
        snapshot.redpanda_environment = ss::sstring(env_value).substr(
          0, metrics_snapshot::max_size_for_rp_env);
    }

    auto license = _feature_table.local().get_license();
    if (license.has_value()) {
        snapshot.id_hash = license->checksum;
    }

    snapshot.has_valid_license = license.has_value()
                                 && !license.value().is_expired();
    snapshot.has_enterprise_features = feature_report.any();

    co_return snapshot;
}

ss::future<> metrics_reporter::try_initialize_cluster_info() {
    // already initialized, do nothing
    if (_cluster_info.is_initialized()) {
        co_return;
    }

    // Wait until the controller log has seen enough batches to generate
    // a UUID.  No timeout: we cannot generate any reports until this
    // happens.
    if (_raft0->committed_offset() < model::offset{2}) {
        vlog(clusterlog.info, "Waiting to initialize cluster metrics ID...");
        co_await _controller_stm.local().wait(
          model::offset{2},
          model::timeout_clock::time_point::max(),
          std::ref(_as.local()));
    }

    if (_raft0->start_offset() > model::offset{0}) {
        // Controller log already snapshotted, wait until cluster info gets
        // initialized from the snapshot.
        co_return;
    }

    storage::log_reader_config reader_cfg(
      model::offset(0), model::offset(2), ss::default_priority_class());
    auto reader = co_await _raft0->make_reader(reader_cfg);

    auto batches = co_await model::consume_reader_to_memory(
      std::move(reader), model::no_timeout);
    if (batches.size() < 2) {
        co_return;
    }

    auto& first_cfg = batches.front();

    auto data_bytes = iobuf_to_bytes(first_cfg.data());
    hash_sha256 sha256;
    sha256.update(data_bytes);
    _cluster_info.creation_timestamp = first_cfg.header().first_timestamp;
    // use timestamps of first two batches in raft-0 log.
    for (int i = 0; i < 2; ++i) {
        sha256.update(iobuf_to_bytes(
          reflection::to_iobuf(batches[i].header().first_timestamp())));
    }
    auto hash = sha256.reset();
    // seed prng with data and timestamps hash
    boost::random::mt19937 mersenne_twister;
    boost::random::seed_seq seed(hash.begin(), hash.end());
    mersenne_twister.seed(seed);

    boost::uuids::random_generator_mt19937 uuid_gen(mersenne_twister);

    _cluster_info.uuid = fmt::format("{}", uuid_gen());
    vlog(
      clusterlog.info, "Generated cluster metrics ID {}", _cluster_info.uuid);
}

/**
 * Having synthesized a unique cluster ID in try_initialize_cluster_info,
 * set it as the global cluster_id in the cluster configuration, if there
 * is not already a cluster_id set there.
 *
 * If this fails to write the configuration, it will simply log a warning,
 * in the expectation that this function is called again on next metrics
 * reporter tick.
 */
ss::future<> metrics_reporter::propagate_cluster_id() {
    if (config::shard_local_cfg().cluster_id().has_value()) {
        // Don't override any existing cluster_id
        co_return;
    }

    if (_cluster_info.uuid == "") {
        co_return;
    }

    auto result = co_await _config_frontend.local().patch(
      config_update_request{.upsert = {{"cluster_id", _cluster_info.uuid}}},
      model::timeout_clock::now() + 5s);
    if (result.errc) {
        vlog(
          clusterlog.warn, "Failed to initialize cluster_id: {}", result.errc);
    } else {
        vlog(
          clusterlog.info, "Initialized cluster_id to {}", _cluster_info.uuid);
    }
}

iobuf serialize_metrics_snapshot(
  const metrics_reporter::metrics_snapshot& snapshot) {
    json::StringBuffer sb;
    json::Writer<json::StringBuffer> writer(sb);

    json::rjson_serialize(writer, snapshot);
    iobuf out;
    out.append(sb.GetString(), sb.GetSize());

    return out;
}
ss::future<http::client> metrics_reporter::make_http_client() {
    net::base_transport::configuration client_configuration;
    client_configuration.server_addr = net::unresolved_address(
      ss::sstring(_address.host), _address.port);

    client_configuration.disable_metrics = net::metrics_disabled::yes;

    if (_address.protocol == "https") {
        ss::tls::credentials_builder builder;
        builder.set_client_auth(ss::tls::client_auth::NONE);
        builder.set_minimum_tls_version(
          config::from_config(config::shard_local_cfg().tls_min_version()));
        auto ca_file = co_await net::find_ca_file();
        if (ca_file) {
            vlog(
              _logger.trace, "using {} as metrics reporter CA store", ca_file);
            co_await builder.set_x509_trust_file(
              ca_file.value(), ss::tls::x509_crt_format::PEM);
        } else {
            vlog(
              _logger.trace,
              "ca file not found, defaulting to system trust store");
            co_await builder.set_system_trust();
        }

        client_configuration.credentials
          = co_await net::build_reloadable_credentials_with_probe<
            ss::tls::certificate_credentials>(
            std::move(builder), "metrics_reporter", "httpclient");
        client_configuration.tls_sni_hostname = _address.host;
    }
    co_return http::client(client_configuration, _as.local());
}

ss::future<>
metrics_reporter::do_send_metrics(http::client& client, iobuf body) {
    auto timeout = config::shard_local_cfg().metrics_reporter_tick_interval();
    auto res = co_await client.get_connected(timeout, _logger);
    // skip sending metrics, unable to connect
    if (res != http::reconnect_result_t::connected) {
        vlog(
          _logger.trace, "unable to send metrics report, connection timeout");
        co_return;
    }
    auto resp_stream = co_await client.post(
      _address.path, std::move(body), http::content_type::json, timeout);
    co_await resp_stream->prefetch_headers();
}

ss::future<> metrics_reporter::do_report_metrics() {
    // try initializing cluster info, if it is already present this operation
    // does nothing.
    // do this on every node to allow controller snapshotting to proceed.
    co_await try_initialize_cluster_info();

    // Update cluster_id in configuration, if not already set.  Wait until
    // we become leader, or it gets written by some other node.
    if (_cluster_info.is_initialized()) {
        while (!_as.local().abort_requested()
               && !config::shard_local_cfg().cluster_id().has_value()) {
            if (_raft0->is_elected_leader()) {
                co_await propagate_cluster_id();
            } else {
                co_await ss::sleep(
                  config::shard_local_cfg().raft_heartbeat_interval_ms());
            }
        }
    }

    // skip reporting if current node is not raft0 leader
    if (!_raft0->is_elected_leader()) {
        co_return;
    }

    // report interval has not elapsed
    if (
      _last_success
      > ss::lowres_clock::now()
          - config::shard_local_cfg().metrics_reporter_report_interval()) {
        co_return;
    }

    // If reporting is disabled, drop out here: we've initialized cluster_id
    // if needed but, will not send any reports home.
    if (!config::shard_local_cfg().enable_metrics_reporter()) {
        co_return;
    }

    // if not initialized, wait until next tick
    if (!_cluster_info.is_initialized()) {
        co_return;
    }

    // collect metrics
    auto snapshot = co_await build_metrics_snapshot();
    if (!snapshot) {
        vlog(
          _logger.trace,
          "error collecting cluster metrics snapshot - {}",
          snapshot.error().message());
        co_return;
    }
    auto out = serialize_metrics_snapshot(snapshot.value());
    try {
        co_await http::with_client(
          co_await make_http_client(), [this, &out](http::client& client) {
              return do_send_metrics(client, std::move(out));
          });
        _last_success = ss::lowres_clock::now();
    } catch (...) {
        vlog(
          _logger.trace,
          "exception thrown while reporting metrics - {}",
          std::current_exception());
    }
}

} // namespace cluster

namespace json {
void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::metrics_reporter::metrics_snapshot& snapshot) {
    w.StartObject();

    w.Key("cluster_uuid");
    w.String(snapshot.cluster_uuid);
    w.Key("cluster_created_ts");
    w.Uint64(snapshot.cluster_creation_epoch);
    w.Key("topic_count");
    w.Int(snapshot.topic_count);

    w.Key("partition_count");
    w.Int(snapshot.partition_count);

    w.Key("active_logical_version");
    w.Int(snapshot.active_logical_version);

    w.Key("original_logical_version");
    w.Int(snapshot.original_logical_version);

    w.Key("nodes");
    w.StartArray();
    for (const auto& m : snapshot.nodes) {
        rjson_serialize(w, m);
    }
    w.EndArray();
    w.Key("has_kafka_gssapi");
    w.Bool(snapshot.has_kafka_gssapi);

    w.Key("has_oidc");
    w.Bool(snapshot.has_oidc);

    w.Key("rbac_role_count");
    w.Int(snapshot.rbac_role_count);

    w.Key("data_transforms_count");
    w.Uint(snapshot.data_transforms_count);

    w.Key("config");
    config::shard_local_cfg().to_json_for_metrics(w);

    w.Key("redpanda_environment");
    w.String(snapshot.redpanda_environment);

    w.Key("id_hash");
    w.String(snapshot.id_hash);

    w.Key("has_valid_license");
    w.Bool(snapshot.has_valid_license);

    w.Key("has_enterprise_features");
    w.Bool(snapshot.has_enterprise_features);

    w.EndObject();
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::metrics_reporter::node_disk_space& ds) {
    w.StartObject();
    w.Key("free");
    w.Uint64(ds.free);
    w.Key("total");
    w.Uint64(ds.total);
    w.EndObject();
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::metrics_reporter::node_metrics& nm) {
    w.StartObject();
    w.Key("node_id");
    w.Int(nm.id);
    w.Key("cpu_count");
    w.Uint(nm.cpu_count);
    w.Key("version");
    w.String(nm.version);
    w.Key("logical_version");
    w.Int(nm.logical_version);
    w.Key("uptime_ms");
    w.Uint64(nm.uptime_ms);
    w.Key("is_alive");
    w.Bool(nm.is_alive);
    w.Key("disks");
    w.StartArray();
    for (auto& d : nm.disks) {
        rjson_serialize(w, d);
    }
    w.EndArray();

    w.EndObject();
}
} // namespace json
