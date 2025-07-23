/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/client/cluster.h"

#include "kafka/client/types.h"
#include "random/generators.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>
namespace kafka::client {

using namespace std::chrono_literals;
cluster::cluster(connection_configuration config)
  : _config(std::move(config))
  , _logger(
      kclog, fmt::format("{}", _config.client_id.value_or("kafka-client")))
  , _brokers(_logger, std::make_unique<remote_broker_factory>(_config, _logger))
  , _next_seed(random_generators::get_int<size_t>(
      0, _config.initial_brokers.size() - 1)) {}

cluster::cluster(
  connection_configuration config,
  std::unique_ptr<broker_factory> broker_factory)
  : _config(std::move(config))
  , _logger(
      kclog, fmt::format("{}", _config.client_id.value_or("kafka-client")))
  , _brokers(_logger, std::move(broker_factory))
  , _next_seed(random_generators::get_int<size_t>(
      0, _config.initial_brokers.size() - 1)) {}

ss::future<> cluster::start() {
    vlog(
      _logger.info,
      "Starting with initial brokers: {}",
      fmt::join(_config.initial_brokers, ", "));

    _metadata_update_timer.set_callback([this]() { update_timer_callback(); });

    co_await update_metadata();
    _metadata_update_timer.arm(_config.max_metadata_age);
}
void cluster::update_timer_callback() {
    ssx::spawn_with_gate(_gate, [this] {
        return update_metadata()
          .handle_exception([this](const std::exception_ptr& e) {
              vlog(_logger.warn, "Failed to update cluster metadata - {}", e);
          })
          .finally([this] {
              if (!_as.abort_requested()) {
                  _metadata_update_timer.arm(_config.max_metadata_age);
              }
          });
    });
}
ss::future<> cluster::stop() {
    vlog(_logger.info, "Stopping");
    _as.request_abort();
    _metadata_update_timer.cancel();
    _update_lock.broken();
    co_await _gate.close();
    co_await _brokers.stop();
}

ss::future<> cluster::update_metadata() {
    auto request_time = ss::lowres_clock::now();

    _as.check();
    auto u = co_await _update_lock.get_units();
    if (_last_update_time >= request_time) {
        co_return;
    }
    co_await dispatch_metadata_request();
}

ss::future<> cluster::request_metadata_update() {
    vlog(_logger.debug, "Requesting metadata update");
    auto h = _gate.hold();
    co_await update_metadata();
}

ss::future<> cluster::initialize_metadata_with_seed() {
    vlog(
      _logger.debug,
      "Initializing metadata with seed brokers: {}",
      _config.initial_brokers);

    std::exception_ptr error = nullptr;
    for (size_t i = 0; i < _config.initial_brokers.size(); ++i) {
        _next_seed = (_next_seed + 1) % _config.initial_brokers.size();
        const auto& address = _config.initial_brokers[_next_seed];
        auto broker = co_await _brokers.create_broker(unknown_node_id, address);

        auto reply_f = co_await ss::coroutine::as_future(
          broker->dispatch(metadata_request{.list_all_topics = false}));
        /**
         * stop the broker after the request is done to ensure that
         * the broker is not left in a connected state. This is important
         * because the broker is created with unknown_node_id and we don't
         * want to keep it connected.
         */
        co_await broker->stop();
        if (!reply_f.failed()) {
            co_await apply_metadata(
              std::get<metadata_response>(std::move(reply_f.get())));
            co_return;
        }
        auto ex = reply_f.get_exception();
        vlog(
          _logger.warn,
          "Failed to initialize metadata with seed broker {}:{} - {}",
          address.host(),
          address.port(),
          ex);
        error = ex;
    }
    if (error) {
        vlog(
          _logger.warn,
          "Failed to initialize metadata with any of the seed brokers - {}",
          error);
        std::rethrow_exception(error);
    }
}

ss::future<> cluster::dispatch_metadata_request() {
    auto h = _gate.hold();
    vlog(_logger.debug, "Dispatching metadata request");
    shared_broker_t broker;

    if (_brokers.empty()) {
        // If there are no brokers, connect to one of the seeds
        co_await initialize_metadata_with_seed();
    }

    try {
        // TODO: support topic subscription
        auto reply = co_await dispatch_to_any(
          metadata_request{.list_all_topics = true});

        co_await apply_metadata(std::move(reply));
    } catch (const broker_error& e) {
        vlog(
          _logger.warn, "Failed to dispatch metadata request - {}", e.what());
    }
}

void cluster::set_sasl_configuration(std::optional<sasl_configuration> creds) {
    vlog(_logger.debug, "Setting SASL configuration: {}", creds);
    _config.sasl_cfg = std::move(creds);
}

ss::future<> cluster::apply_metadata(metadata_response reply) {
    /**
     * this is the only place where the cluster brokers are updated. It happens
     * under the metadata update lock.
     */

    vlog(
      _logger.debug,
      "Applying metadata response with {{ cluster_id: {}, "
      "controller_id: {}, brokers: {} and {} topics }}",
      reply.data.cluster_id,
      reply.data.controller_id,
      reply.data.brokers,
      reply.data.topics.size());

    reply.data.cluster_id = reply.data.cluster_id;
    if (reply.data.controller_id == unknown_node_id) {
        _controller_id.reset();
    } else {
        _controller_id = reply.data.controller_id;
    }
    _topic_cache.apply(reply.data.topics);
    co_await _brokers.apply(reply.data.brokers);
    _last_update_time = ss::lowres_clock::now();
    // trigger notification last, after the metadata is applied
    _notifications.notify(reply.data);
}

} // namespace kafka::client
