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

#include "kafka/client/configuration.h"
#include "kafka/client/types.h"
#include "random/generators.h"
#include "ssx/future-util.h"
#include "utils/unresolved_address.h"

#include <seastar/coroutine/as_future.hh>
namespace kafka::client {

namespace {
using topic_metadata_included_t
  = ss::bool_class<struct topic_metadata_included_tag>;
metadata_update to_metadata_update(
  metadata_response r, topic_metadata_included_t topic_metadata_included) {
    metadata_update update;
    update.brokers.reserve(r.data.brokers.size());
    for (auto& b : r.data.brokers) {
        update.brokers.push_back(
          metadata_update::broker{
            .node_id = b.node_id,
            .host = std::move(b.host),
            .port = b.port,
            .rack = std::move(b.rack)});
    }
    update.cluster_id = std::move(r.data.cluster_id);
    update.controller_id = r.data.controller_id;
    if (topic_metadata_included) {
        update.topics = std::move(r.data.topics);
    }
    // cluster_authorized_operations field was deprecated in v11
    update.cluster_authorized_operations = r.data.cluster_authorized_operations;

    return update;
}
metadata_update to_metadata_update(describe_cluster_response r) {
    metadata_update update;
    update.brokers.reserve(r.data.brokers.size());
    for (auto& b : r.data.brokers) {
        update.brokers.push_back(
          metadata_update::broker{
            .node_id = b.broker_id,
            .host = std::move(b.host),
            .port = b.port,
            .rack = std::move(b.rack)});
    }
    update.cluster_id = std::move(r.data.cluster_id);
    update.controller_id = r.data.controller_id;
    update.cluster_authorized_operations = r.data.cluster_authorized_operations;
    return update;
}
} // namespace

using namespace std::chrono_literals;
cluster::cluster(connection_configuration config)
  : _config(std::move(config))
  , _logger(
      kclog, fmt::format("{}", _config.client_id.value_or("kafka-client")))
  , _topic_cache(metadata_age_multiplier * _config.max_metadata_age)
  , _brokers(_logger, std::make_unique<remote_broker_factory>(_config, _logger))
  , _next_seed(
      random_generators::get_int<size_t>(0, _config.initial_brokers.size() - 1))
  , _update_config_queue([this](const std::exception_ptr& ex) {
      vlog(_logger.warn, "unexpected client error: {}", ex);
  }) {}

cluster::cluster(
  connection_configuration config,
  std::unique_ptr<broker_factory> broker_factory)
  : _config(std::move(config))
  , _logger(
      kclog, fmt::format("{}", _config.client_id.value_or("kafka-client")))
  , _brokers(_logger, std::move(broker_factory))
  , _next_seed(
      random_generators::get_int<size_t>(0, _config.initial_brokers.size() - 1))
  , _update_config_queue([this](const std::exception_ptr& ex) {
      vlog(_logger.warn, "unexpected client error: {}", ex);
  }) {}

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
    co_await _update_config_queue.shutdown();
    co_await _gate.close();
    co_await _brokers.stop();
}

ss::future<> cluster::update_metadata(
  std::optional<chunked_vector<model::topic>> topics_request_list) {
    auto request_time = ss::lowres_clock::now();

    _as.check();
    auto u = co_await _update_lock.get_units();
    if (_last_update_time >= request_time) {
        co_return;
    }
    co_await dispatch_and_apply_metadata_updates(
      std::move(topics_request_list));
}

ss::future<> cluster::dispatch_and_apply_metadata_updates(
  std::optional<chunked_vector<model::topic>> topics_request_list) {
    auto h = _gate.hold();

    vlog(_logger.debug, "Starting cluster metadata dispatch and update");

    if (_brokers.empty()) {
        // If there are no brokers, connect to one of the seeds
        co_await initialize_metadata_with_seed();
    }

    try {
        auto broker = _brokers.any();
        auto describe_cluster_version = co_await broker->get_supported_versions(
          describe_cluster_api::key, _as);
        metadata_update mu;
        if (describe_cluster_version) {
            auto describe_cluster_resp
              = co_await dispatch_describe_cluster_request(broker);
            mu = to_metadata_update(std::move(describe_cluster_resp));
        } else {
            // If describe cluster isn't supported, then dispatch a metadata
            // request with no topics at API version 10 in order to get the
            // cluster level authorized operations
            auto metadata_resp = co_await dispatch_metadata_request(
              broker, {}, api_version(10));
            mu = to_metadata_update(
              std::move(metadata_resp), topic_metadata_included_t::no);
        }

        if (!topics_request_list.has_value() || !topics_request_list->empty()) {
            // If there are topics to request, then do another metadata request
            // with the most up-to-date version
            auto metadata_resp = co_await dispatch_metadata_request(
              broker, std::move(topics_request_list));
            mu.topics = std::move(metadata_resp.data.topics);
        }

        co_await apply_metadata(std::move(mu));
    } catch (const broker_error& e) {
        // TODO(CORE-14956) - We need to improve handling of broker errors
        vlog(
          _logger.debug, "Failed to dispatch metadata request - {}", e.what());
    }
}

ss::future<> cluster::request_metadata_update(
  std::optional<chunked_vector<model::topic>> topics_request_list) {
    vlog(_logger.debug, "Requesting metadata update");
    auto h = _gate.hold();
    co_await update_metadata(std::move(topics_request_list));
}

namespace {

template<KafkaApi Api>
ss::future<api_version> get_required_api_version(
  shared_broker_t broker,
  std::optional<std::reference_wrapper<ss::abort_source>> as,
  api_version min_required) {
    auto supported_versions = co_await broker->get_supported_versions(
      Api::key, as);
    if (!supported_versions || supported_versions->max < min_required) {
        throw broker_error(
          broker->id(),
          error_code::unsupported_version,
          fmt::format(
            "Broker {} at {}:{} does not support {} API with required "
            "version >= {}",
            broker->id(),
            broker->get_address().host(),
            broker->get_address().port(),
            Api::name,
            min_required));
    }
    co_return std::min(supported_versions->max, Api::max_valid);
}

ss::future<api_version> get_metadata_request_version(
  shared_broker_t broker,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    return get_required_api_version<metadata_api>(
      std::move(broker), as, api_version(1));
}

ss::future<api_version> get_describe_cluster_request_version(
  shared_broker_t broker,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    return get_required_api_version<describe_cluster_api>(
      std::move(broker), as, api_version(0));
}
} // namespace
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

        auto describe_cluster_version_f = co_await ss::coroutine::as_future(
          broker->get_supported_versions(describe_cluster_api::key, _as));
        if (describe_cluster_version_f.failed()) {
            error = describe_cluster_version_f.get_exception();
            vlog(
              _logger.warn,
              "Failed to get supported describe cluster version {}:{} - {}",
              address.host(),
              address.port(),
              error);
            co_await broker->stop();
            continue;
        }

        auto describe_cluster_version = describe_cluster_version_f.get();
        ss::future<> f = ss::now();
        if (describe_cluster_version.has_value()) {
            f = co_await ss::coroutine::as_future(
              initialize_with_describe_cluster(broker));
        } else {
            f = co_await ss::coroutine::as_future(
              initialize_with_metadata(broker));
        }

        /**
         * stop the broker after the request is done to ensure that
         * the broker is not left in a connected state. This is important
         * because the broker is created with unknown_node_id and we don't
         * want to keep it connected.
         */
        co_await broker->stop();

        if (f.failed()) {
            error = f.get_exception();
            vlog(
              _logger.warn,
              "Failed to initialize metadata using {} with seed broker {}:{} - "
              "{}",
              describe_cluster_version ? "describe_cluster" : "metadata",
              address.host(),
              address.port(),
              error);
            continue;
        }

        // If we are successful in connecting to the broker, then we can stop
        co_return;
    }
    if (error) {
        vlog(
          _logger.warn,
          "Failed to initialize metadata with any of the seed brokers - {}",
          error);
        std::rethrow_exception(error);
    }
}

ss::future<> cluster::initialize_with_describe_cluster(shared_broker_t broker) {
    vlog(_logger.debug, "Initializing cluster with describe_cluster request");
    auto resp = co_await dispatch_describe_cluster_request(std::move(broker));
    co_await apply_metadata(to_metadata_update(std::move(resp)));
}

ss::future<> cluster::initialize_with_metadata(shared_broker_t broker) {
    vlog(_logger.debug, "Initializing cluster with metadata request");
    auto resp = co_await dispatch_metadata_request(std::move(broker), {});
    co_await apply_metadata(
      to_metadata_update(std::move(resp), topic_metadata_included_t::no));
}

ss::future<metadata_response> cluster::dispatch_metadata_request(
  shared_broker_t broker,
  std::optional<chunked_vector<model::topic>> topics_request_list,
  std::optional<api_version> requested_version) {
    auto h = _gate.hold();
    vlog(_logger.debug, "Dispatching metadata request");

    std::optional<chunked_vector<metadata_request_topic>> topics_to_request
      = std::nullopt;
    if (topics_request_list.has_value()) {
        topics_to_request.emplace();
        topics_to_request->reserve(topics_request_list->size());
        std::ranges::transform(
          std::move(topics_request_list.value()),
          std::back_inserter(*topics_to_request),
          [](model::topic& t) {
              return metadata_request_topic{.name = std::move(t)};
          });
    }

    auto request_version = co_await get_metadata_request_version(broker, _as);
    auto metadata_version = std::min(
      requested_version.value_or(request_version), request_version);
    // TODO: support topic subscription
    auto reply = co_await broker->dispatch(
      metadata_request{.data{
        .topics = std::move(topics_to_request),
        .allow_auto_topic_creation = false,
        .include_cluster_authorized_operations = bool(
          _config.include_authorized_operations),
        .include_topic_authorized_operations = bool(
          _config.include_authorized_operations)}},
      metadata_version);
    vassert(
      std::holds_alternative<kafka::metadata_response>(reply),
      "Metadata response is required to be returned as a result of "
      "metadata request");

    co_return std::get<metadata_response>(std::move(reply));
}

ss::future<describe_cluster_response>
cluster::dispatch_describe_cluster_request(shared_broker_t broker) {
    auto h = _gate.hold();
    vlog(_logger.debug, "Dispatching describe cluster request");

    auto request_version = co_await get_describe_cluster_request_version(
      broker, _as);
    auto reply = co_await broker->dispatch(
      describe_cluster_request{.data{
        .include_cluster_authorized_operations = bool(
          _config.include_authorized_operations)}},
      request_version);
    vassert(
      std::holds_alternative<kafka::describe_cluster_response>(reply),
      "Describe cluster response is required to be returned as a result of "
      "describe cluster request");

    co_return std::get<describe_cluster_response>(std::move(reply));
}

void cluster::set_sasl_configuration(std::optional<sasl_configuration> creds) {
    vlog(_logger.debug, "Setting SASL configuration: {}", creds);
    _config.sasl_cfg = std::move(creds);
}

void cluster::update_configuration(connection_configuration config) {
    _update_config_queue.submit([this, config = std::move(config)] mutable {
        return do_update_configuration(std::move(config))
          .handle_exception([this](std::exception_ptr ex) {
              vlog(_logger.warn, "Failed to update broker config: {}", ex);
          });
    });
}

ss::future<> cluster::do_update_configuration(connection_configuration config) {
    auto u = co_await _update_lock.get_units();

    vlog(_logger.debug, "Updating cluster config to '{}'", config);
    _config = std::move(config);

    _topic_cache.set_topic_timeout(
      metadata_age_multiplier * _config.max_metadata_age);

    // all updates to the cluster brokers should happen under the
    // metadata update lock
    co_await _brokers.clear();
    co_await dispatch_and_apply_metadata_updates();
}

ss::future<> cluster::apply_metadata(metadata_update reply) {
    /**
     * All updates to the cluster brokers should happen under the metadata
     * update lock.
     */

    vlog(
      _logger.debug,
      "Applying metadata response with {{ cluster_id: {}, "
      "controller_id: {}, brokers: {} and {} topics }}",
      reply.cluster_id,
      reply.controller_id,
      reply.brokers,
      reply.topics.has_value() ? reply.topics->size() : 0);

    _cluster_id = reply.cluster_id;
    _cluster_authorized_operations = reply.cluster_authorized_operations;
    if (reply.controller_id == unknown_node_id) {
        _controller_id.reset();
    } else {
        _controller_id = reply.controller_id;
    }

    if (reply.topics.has_value()) {
        // Only apply topics if the response contains any, an empty list may be
        // because none were requested
        _topic_cache.apply(reply.topics.value());
    }
    co_await _brokers.apply(reply.brokers);
    _last_update_time = ss::lowres_clock::now();
    // trigger notification last, after the metadata is applied
    _notifications.notify(reply);
}

ss::future<std::optional<api_version_range>> cluster::supported_api_versions(
  api_key key, std::optional<std::reference_wrapper<ss::abort_source>> as) {
    return _brokers.supported_api_versions(key, as.has_value() ? as : _as);
}

ss::future<std::optional<api_version_range>> cluster::supported_api_versions(
  model::node_id id,
  api_key key,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    return _brokers.supported_api_versions(id, key, as.has_value() ? as : _as);
}

} // namespace kafka::client
