#include "kafka/client/direct_consumer/verifier/application.h"

#include "absl/strings/strip.h"
#include "base/vlog.h"
#include "net/dns.h"
#include "serde/protobuf/rpc.h"
#include "ssx/future-util.h"
#include "utils/stop_signal.h"
#include "utils/unresolved_address.h"

#include <seastar/core/future.hh>
#include <seastar/http/api_docs.hh>
#include <seastar/util/short_streams.hh>

#include <utility>
using namespace std::chrono_literals;
namespace kafka::client {
/**
 * Runner
 */
consumer_runner::consumer_runner(
  connection_configuration connection_configuration,
  direct_consumer::configuration consumer_cfg)
  : _id(connection_configuration.get_client_id())
  , _cluster(std::make_unique<kafka::client::cluster>(
      std::move(connection_configuration))) {
    _consumer = std::make_unique<kafka::client::direct_consumer>(
      *_cluster, consumer_cfg);
}

ss::future<> consumer_runner::start() {
    co_await _cluster->start();
    co_await _consumer->start();
    ssx::repeat_until_gate_closed_or_aborted(
      _gate, _abort_source, [this] { return do_fetch(); });
    _reporter.set_callback([this] { report_stats(); });
    _reporter.arm_periodic(5s);
}

void consumer_runner::report_stats() {
    auto interval_ms = (std::chrono::steady_clock::now() - _last_report_time)
                       / 1ms;
    double interval_sec = static_cast<double>(interval_ms) / 1000.0;

    vlog(
      v_logger.info,
      "[client: {}] throughput {:.2f} MiB/s ({:.2f} records/s)",
      _id,
      (_total_bytes - _last_reported_bytes) / (1_MiB) / interval_sec,
      (_total_records - _last_reported_records) / interval_sec);

    _last_reported_bytes = _total_bytes;
    _last_reported_records = _total_records;
    _last_report_time = std::chrono::steady_clock::now();
}

ss::future<> consumer_runner::stop() {
    _reporter.cancel();
    _abort_source.request_abort();
    co_await _cluster->stop();
    co_await _consumer->stop();
    co_await _gate.close();
}

namespace {
struct records_stats {
    size_t count = 0;
    size_t bytes = 0;
};

records_stats fetch_totals(const chunked_vector<model::record_batch>& batches) {
    records_stats stats;
    for (const auto& batch : batches) {
        stats.count += batch.record_count();
        stats.bytes += batch.size_bytes();
    }
    return stats;
}
} // namespace

ss::future<> consumer_runner::do_fetch() {
    auto fetches = co_await _consumer->fetch_next(1s);
    if (fetches.has_error()) {
        vlog(v_logger.warn, "error fetching data: {}", fetches.error());
        _last_error_code = fetches.error();
        co_return;
    } else {
        _last_error_code = kafka::error_code::none;
    }

    for (auto& fetched_topic_data : fetches.value()) {
        for (auto& fetched_partition_data : fetched_topic_data.partitions) {
            if (fetched_partition_data.data.empty()) {
                continue; // no data to process
            }
            auto& partition_stats = _stats[fetched_topic_data.topic]
                                          [fetched_partition_data.partition_id];
            if (fetched_partition_data.error != kafka::error_code::none) {
                partition_stats.last_error_code = fetched_partition_data.error;
                continue;
            } else {
                partition_stats.last_error_code = kafka::error_code::none;
            }

            auto totals = fetch_totals(fetched_partition_data.data);
            if (totals.count > 0) {
                partition_stats.last_fetched_timestamp
                  = model::timestamp::now();
            }

            partition_stats.fetched_records += totals.count;
            partition_stats.fetched_bytes += totals.bytes;
            _total_bytes += totals.bytes;
            _total_records += totals.count;
            auto last_fetched_offset = model::offset_cast(
              fetched_partition_data.data.back().last_offset());
            if (last_fetched_offset <= partition_stats.last_fetched_offset) {
                _non_monotonic_fetches++;
                vlog(
                  v_logger.warn,
                  "[client: {}] non-monotonic fetch detected for topic: {}, "
                  "partition: {}, last fetched offset: {}, previous offset: {}",
                  _id,
                  fetched_topic_data.topic,
                  fetched_partition_data.partition_id,
                  last_fetched_offset,
                  partition_stats.last_fetched_offset);
            }
            partition_stats.last_fetched_offset = last_fetched_offset;
        }
    }
}
namespace {
inline verifier::api_response success() { return verifier::api_response{}; }
} // namespace

/**
 * Service
 */

seastar::future<verifier::api_response>
verifier_service_impl::status(verifier::status_request) {
    co_return success();
}

seastar::future<verifier::api_response> verifier_service_impl::create_consumer(
  verifier::create_direct_consumer_request req) {
    if (req.get_client_id().empty()) {
        throw serde::pb::rpc::invalid_argument_exception(
          "Client ID must be provided");
    }
    if (req.get_initial_brokers().empty()) {
        throw serde::pb::rpc::invalid_argument_exception(
          "Initial brokers must be provided");
    }
    kafka::client_id client_id{req.get_client_id()};
    auto it = _consumers.find(client_id);
    if (it != _consumers.end()) {
        throw serde::pb::rpc::invalid_argument_exception(
          ssx::sformat("Consumer with client ID {} already exists", client_id));
    }

    kafka::client::connection_configuration connection_config;
    connection_config.client_id = client_id;
    connection_config.initial_brokers.reserve(req.get_initial_brokers().size());
    for (const auto& broker : req.get_initial_brokers()) {
        connection_config.initial_brokers.emplace_back(
          broker.get_host(), static_cast<uint16_t>(broker.get_port()));
    }

    auto [entry_it, _] = _consumers.try_emplace(
      client_id, connection_config, direct_consumer::configuration{});
    co_await entry_it->second.start();
    co_return success();
}

seastar::future<verifier::api_response>
verifier_service_impl::assign_partitions(
  verifier::assign_partitions_request req) {
    auto it = _consumers.find(kafka::client_id(req.get_client_id()));
    if (it == _consumers.end()) {
        throw serde::pb::rpc::not_found_exception(ssx::sformat(
          "Consumer with client ID {} not found", req.get_client_id()));
    }
    auto& runner = it->second;
    chunked_vector<topic_assignment> assignments;
    assignments.reserve(req.get_topic_assignments().size());
    for (const auto& assignment : req.get_topic_assignments()) {
        topic_assignment topic_assignment;
        topic_assignment.topic = model::topic(assignment.get_topic());
        topic_assignment.partitions.reserve(assignment.get_partitions().size());
        for (const auto& partition : assignment.get_partitions()) {
            partition_assignment p_as{
              .partition_id = model::partition_id(
                partition.get_partition_id())};
            if (partition.has_offset()) {
                p_as.next_offset = kafka::offset(partition.get_offset());
            }
            topic_assignment.partitions.push_back(std::move(p_as));
        }
        assignments.push_back(std::move(topic_assignment));
    }

    co_await runner.assign_partitions(std::move(assignments));
    co_return success();
}

ss::future<verifier::api_response> verifier_service_impl::unassign_partitions(
  verifier::unassign_partitions_request req) {
    auto it = _consumers.find(kafka::client_id(req.get_client_id()));
    if (it == _consumers.end()) {
        throw serde::pb::rpc::not_found_exception(ssx::sformat(
          "Consumer with client ID {} not found", req.get_client_id()));
    }
    auto& runner = it->second;
    chunked_vector<model::topic_partition> topic_partitions;
    topic_partitions.reserve(req.get_topics().size());
    for (const auto& req_topic : req.get_topics()) {
        for (auto& partition : req_topic.get_partitions()) {
            topic_partitions.emplace_back(
              model::topic(req_topic.get_topic()),
              model::partition_id(partition));
        }
    }
    co_await runner.unassign_partitions(std::move(topic_partitions));
    co_return success();
}

ss::future<verifier::consumer_state_response>
verifier_service_impl::get_consumer_state(
  verifier::get_consumer_state_request req) {
    verifier::consumer_state_response resp;
    kafka::client_id client_id(req.get_client_id());
    auto it = _consumers.find(client_id);
    if (it == _consumers.end()) {
        // Consumer not found - return empty response with client_id
        throw serde::pb::rpc::not_found_exception(ssx::sformat(
          "Consumer with client ID {} not found", req.get_client_id()));
    }

    auto& runner = it->second;
    resp.set_client_id(std::move(client_id));
    resp.set_total_consumed_bytes(runner.total_bytes());
    resp.set_total_consumed_messages(runner.total_records());
    resp.set_last_error_code(static_cast<int32_t>(runner.last_error_code()));
    resp.set_non_monotonic_fetches(runner.non_monotonic_fetches());

    if (req.get_include_partition_states()) {
        auto& assigned_topics = resp.get_assigned_topics();
        assigned_topics.reserve(runner.stats().size());
        // Group partition stats by topic
        for (const auto& [topic, partitions] : runner.stats()) {
            verifier::topic_state topic_state;
            topic_state.set_topic(ss::sstring(topic));
            topic_state.get_partitions().reserve(partitions.size());
            for (auto& [partition_id, p_stats] : partitions) {
                verifier::partition_state p_state;
                p_state.set_partition_id(partition_id());
                p_state.set_last_fetch_offset(p_stats.last_fetched_offset());
                p_state.set_last_fetch_timestamp(
                  p_stats.last_fetched_timestamp.value());
                p_state.set_fetched_bytes(p_stats.fetched_bytes);
                p_state.set_fetched_records(p_stats.fetched_records);
                p_state.set_error_code(
                  static_cast<int32_t>(p_stats.last_error_code));
                topic_state.get_partitions().push_back(std::move(p_state));
            }
            assigned_topics.push_back(std::move(topic_state));
        }
    }

    co_return resp;
}

ss::future<> verifier_service_impl::stop() {
    for (auto& entry : _consumers) {
        vlog(v_logger.info, "Stopping consumer: {}", entry.first);
        co_await entry.second.stop();
    }
    _consumers.clear();
}

/**
 * Server
 */

verifier_server::verifier_server(
  listener_configuration conf, verifier_service_impl* service)
  : _config(std::move(conf))
  , _httpd("direct_consumer_verifier")
  , _service(service) {}

ss::future<> verifier_server::stop() { co_await _httpd.stop(); }

verifier_server::handler_impl::handler_impl(verifier_server* s, handler_fun h)
  : server(s)
  , handler(std::move(h)) {}

ss::future<std::unique_ptr<ss::http::reply>>
verifier_server::handler_impl::handle(
  const ss::sstring&,
  std::unique_ptr<ss::http::request> req,
  std::unique_ptr<ss::http::reply> reply) {
    return ss::with_gate(
      server->_gate,
      [this, req = std::move(req), reply = std::move(reply)]() mutable {
          return ss::smp::submit_to(
            0,
            [this, req = std::move(req), reply = std::move(reply)]() mutable {
                vlog(
                  v_logger.debug,
                  "Handling request: {}",
                  absl::StripTrailingAsciiWhitespace(req->request_line()));
                return handler(std::move(req), std::move(reply));
            });
      });
}

ss::shard_id verifier_server::client_shard_id() const { return 0; }

void verifier_server::setup_routes() {
    for (auto& route : _service->all_routes()) {
        // Handler is deleted by the httpd server, so we need to
        // use a raw pointer here.
        auto h = new handler_impl(this, route.handler);

        vlog(v_logger.info, "Adding route: {}", route.path);
        _httpd._routes.add(
          ss::httpd::operation_type::POST, ss::sstring(route.path), h);
    }
}

ss::future<> verifier_server::start() {
    setup_routes();
    net::unresolved_address addr(_config.host, _config.port);
    auto resolved = co_await net::resolve_dns(addr);
    vlog(v_logger.info, "Starting direct consumer verifier on {}", addr);
    _httpd.set_content_streaming(true);
    co_await _httpd.listen(resolved);
}

ss::future<int> verifier_application::run(listener_configuration conf) {
    int result = 0;
    try {
        ::stop_signal app_signal;
        _service = std::make_unique<verifier_service_impl>();
        co_await _server.start(conf, _service.get());
        co_await _server.invoke_on_all(
          [](verifier_server& server) { return server.start(); });

        co_await app_signal.wait();
    } catch (...) {
        vlog(
          v_logger.error,
          "Error running verifier server: {}",
          std::current_exception());
        result = 1;
    }

    co_await _server.stop();
    co_await _service->stop();
    co_return result;
}

} // namespace kafka::client
