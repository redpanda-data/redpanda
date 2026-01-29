// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/client.h"

#include "absl/container/node_hash_map.h"
#include "base/seastarx.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "kafka/client/broker.h"
#include "kafka/client/configuration.h"
#include "kafka/client/consumer.h"
#include "kafka/client/exceptions.h"
#include "kafka/client/fetcher.h"
#include "kafka/client/logger.h"
#include "kafka/client/topic_cache.h"
#include "kafka/client/utils.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/leave_group.h"
#include "kafka/protocol/list_offset.h"
#include "kafka/protocol/metadata.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/coroutine/exception.hh>

#include <algorithm>
#include <cstdlib>
#include <exception>
#include <iterator>
#include <system_error>

namespace kafka::client {

client::client(
  const YAML::Node& cfg, std::optional<external_mitigate> mitigater)
  : client(
      client_configuration::from_config_store(configuration(cfg)),
      std::move(mitigater)) {}

client::client(
  const client_configuration& cfg, std::optional<external_mitigate> mitigater)
  : _retries_config(cfg.retries_cfg)
  , _producer_config(cfg.producer_cfg)
  , _consumer_config(cfg.consumer_cfg)
  , _logger(kclog, cfg.connection_cfg.client_id.value_or("kafka-client"))
  , _external_mitigate(std::move(mitigater))
  , _cluster(std::make_unique<cluster>(cfg.connection_cfg))
  , _producer(
      _producer_config,
      _retries_config,
      _cluster->get_topics(),
      _cluster->get_brokers(),
      _logger,
      [this](std::exception_ptr ex) { return mitigate_error(ex); }) {
    _metadata_callback_id = _cluster->register_metadata_cb(
      [this](const metadata_update& res) { on_metadata_update(res); });
}

ss::future<> client::connect() {
    if (!_is_started) {
        co_await _cluster->start();
        _is_started = true;
    }
}

namespace {
template<typename Func>
ss::future<> catch_and_log(const prefix_logger& logger, Func&& f) noexcept {
    return ss::futurize_invoke(std::forward<Func>(f))
      .discard_result()
      .handle_exception([&logger](std::exception_ptr e) {
          vlog(logger.debug, "exception during stop: {}", e);
      });
}

} // namespace

ss::future<> client::stop() noexcept {
    if (std::exchange(_is_stopped, true)) {
        co_return;
    }
    _as.request_abort();
    co_await catch_and_log(_logger, [this]() { return _producer.stop(); });
    _cluster->unregister_metadata_cb(_metadata_callback_id);
    co_await _gate.close();
    for (auto& [id, group] : _consumers) {
        while (!group.empty()) {
            auto c = *group.begin();
            co_await catch_and_log(_logger, [c]() {
                // The consumer is constructed with an on_stopped which erases
                // istelf from the map after leave() completes.
                return c->leave();
            });
        }
    }
    co_await catch_and_log(_logger, [this]() { return _cluster->stop(); });
}

void client::on_metadata_update(const metadata_update& res) {
    _partitioners.apply_metadata(res);
}

void client::set_credentials(std::optional<sasl_configuration> creds) {
    vlog(_logger.debug, "Setting credentials: {}", creds);
    _cluster->set_sasl_configuration(std::move(creds));
}

ss::future<> client::external_mitigate_error(std::exception_ptr ex) const {
    if (_external_mitigate) {
        return (*_external_mitigate)(ex);
    }
    return ss::make_exception_future(ex);
}

ss::future<> client::update_metadata() {
    if (_current_reconnect_retry >= _retries_config.max_retries) {
        _current_reconnect_retry = 0;
        throw broker_error(
          unknown_node_id,
          error_code::broker_not_available,
          fmt::format(
            "max retries of {} exceeded", _retries_config.max_retries));
    }
    return _cluster->request_metadata_update()
      .then([this] {
          // reset retry counter after a successful metadata update
          _current_reconnect_retry = 0;
      })
      .handle_exception(
        [this](const std::exception_ptr& ex) { return mitigate_error(ex); });
}

ss::future<> client::mitigate_error(std::exception_ptr ex) {
    return external_mitigate_error(ex).handle_exception(
      [this](std::exception_ptr ex) {
          _gate.check();
          try {
              std::rethrow_exception(ex);
          } catch (const broker_error& ex) {
              // If there are no brokers, reconnect
              if (ex.node_id == unknown_node_id) {
                  /**
                   * Count the number of retries connecting to the seed brokers.
                   * If max retires is exceed, client will give up and throw an
                   * exception.
                   */
                  vlog(
                    _logger.debug,
                    "broker_error, reconnect_retry: {} - {}",
                    _current_reconnect_retry,
                    ex);
                  _current_reconnect_retry++;
                  vlog(_logger.warn, "broker_error: {}", ex);
                  return update_metadata();
              } else {
                  vlog(_logger.debug, "broker_error: {}", ex);
                  return update_metadata();
              }
          } catch (const consumer_error& ex) {
              switch (ex.error) {
              case error_code::coordinator_not_available:
                  vlog(_logger.debug, "consumer_error: {}", ex);
                  return update_metadata();
              default:
                  vlog(_logger.warn, "consumer_error: {}", ex);
                  return ss::make_exception_future(ex);
              }
          } catch (const partition_error& ex) {
              switch (ex.error) {
              case error_code::unknown_topic_or_partition:
              case error_code::not_leader_for_partition:
              case error_code::leader_not_available: {
                  vlog(_logger.debug, "partition_error: {}", ex);
                  return update_metadata();
              }
              default:
                  vlog(_logger.warn, "partition_error: {}", ex);
                  return ss::make_exception_future(ex);
              }
          } catch (const topic_error& ex) {
              switch (ex.error) {
              case error_code::unknown_topic_or_partition:
                  vlog(_logger.debug, "topic_error: {}", ex);
                  return update_metadata();
              default:
                  vlog(_logger.warn, "topic_error: {}", ex);
                  return ss::make_exception_future(ex);
              }
          } catch (const ss::gate_closed_exception&) {
              vlog(_logger.debug, "gate_closed_exception");
          } catch (const std::system_error& ex) {
              if (net::is_reconnect_error(ex)) {
                  vlog(_logger.debug, "system_error: {}", ex);
                  return update_metadata();
              } else {
                  vlog(_logger.warn, "system_error: {}", ex);
                  return ss::make_exception_future(ex);
              }
          } catch (const ss::abort_requested_exception& ex) {
              vlog(_logger.debug, "abort_requested_exception: {}", ex);
          } catch (const std::exception& ex) {
              // TODO(Ben): Probably vassert
              vlog(_logger.error, "unknown exception: {}", ex);
          }
          return ss::make_exception_future(ex);
      });
}

ss::future<produce_response::partition> client::produce_record_batch(
  model::topic_partition tp, model::record_batch&& batch) {
    return ss::try_with_gate(
      _gate, [this, tp{std::move(tp)}, batch{std::move(batch)}]() mutable {
          vlog(
            _logger.debug,
            "produce record_batch: {}, {{record_count: {}}}",
            tp,
            batch.record_count());
          return _producer.produce(std::move(tp), std::move(batch));
      });
}

ss::future<produce_response> client::produce_records(
  model::topic topic, chunked_vector<record_essence> records) {
    absl::node_hash_map<model::partition_id, storage::record_batch_builder>
      partition_builders;

    // Assign records to batches per topic_partition
    for (auto& record : records) {
        auto p_id = record.partition_id;
        if (!p_id) {
            p_id = co_await gated_retry_with_mitigation([&, this]() {
                       return ss::make_ready_future<model::partition_id>(
                         _partitioners.partition_for(topic, record));
                   }).handle_exception([](std::exception_ptr) {
                // Assume auto topic creation is on and assign to first
                // partition
                return model::partition_id{0};
            });
        }
        auto it = partition_builders.find(*p_id);
        if (it == partition_builders.end()) {
            it = partition_builders
                   .emplace(
                     *p_id,
                     storage::record_batch_builder(
                       model::record_batch_type::raft_data, model::offset(0)))
                   .first;
        }
        it->second.add_raw_kw(
          std::move(record.key).value_or(iobuf{}),
          std::move(record.value),
          std::move(record.headers));
    }

    // Convert to request::partition
    std::vector<kafka::produce_request::partition> partitions;
    partitions.reserve(partition_builders.size());
    for (auto& pb : partition_builders) {
        partitions.emplace_back(
          kafka::produce_request::partition{
            .partition_index = pb.first,
            .records = kafka::produce_request_record_data{
              std::move(pb.second).build()}});
    }

    // Produce batch to tp
    auto responses = co_await ssx::parallel_transform(
      std::move(partitions),
      [this, topic](kafka::produce_request::partition p) mutable
        -> ss::future<produce_response::partition> {
          return produce_record_batch(
            model::topic_partition(topic, p.partition_index),
            std::move(*p.records->adapter.batch));
      });

    chunked_vector<topic_produce_response> responses_cv;
    responses_cv.emplace_back(
      topic_produce_response{
        .name{std::move(topic)}, .partitions{std::move(responses)}});

    co_return produce_response{
      .data = produce_response_data{
        .responses = std::move(responses_cv),
        .throttle_time_ms{{std::chrono::milliseconds{0}}}}};
}
ss::future<metadata_response> client::fetch_metadata(metadata_request req) {
    co_return co_await gated_retry_with_mitigation(
      [this, req = std::move(req)]() {
          return _cluster->dispatch_to_any(
            req.copy(), api_version_for(metadata_api::key));
      });
}

ss::future<create_topics_response> client::create_topic(
  kafka::creatable_topic req, validate_only_t validate_only) {
    return gated_retry_with_mitigation(
      [this, req{std::move(req)}, validate_only]() {
          auto controller = _cluster->get_controller_id().value_or(
            unknown_node_id);
          chunked_vector<kafka::creatable_topic> cv;
          cv.push_back(req);
          return _cluster->dispatch_to(
            controller,
            kafka::create_topics_request{
              .data = {
                .topics = std::move(cv),
                .validate_only = bool(validate_only),
                }}, api_version_for(create_topics_api::key))
          .then([controller, validate_only](auto res) {
              // If only validating, then immediately return the response
              if (validate_only) {
                return ss::make_ready_future<create_topics_response>(
                    std::move(res));
              }
              auto ec = res.data.topics[0].error_code;
              switch (ec) {
              case error_code::not_controller:
                  return ss::make_exception_future<create_topics_response>(
                    broker_error(controller, ec));
              case error_code::throttling_quota_exceeded:
                  return ss::make_exception_future<create_topics_response>(
                    topic_error(
                      model::topic_view{res.data.topics[0].name}, ec));
              case error_code::topic_authorization_failed:
                  return ss::make_exception_future<create_topics_response>(
                    topic_error(
                      model::topic_view{res.data.topics[0].name}, ec));
              default:
                  return ss::make_ready_future<create_topics_response>(
                    std::move(res));
              }
          });
      });
}

namespace {
template<typename T, typename ErrorPredicate>
requires(KafkaApi<typename T::api_type>)
void throw_on_error(const T& r, ErrorPredicate should_throw) {
    using type = std::remove_cvref_t<T>;
    if constexpr (std::is_same_v<type, list_offsets_response>) {
        for (const auto& topic : r.data.topics) {
            for (const auto& partition : topic.partitions) {
                if (
                  partition.error_code != error_code::none
                  && should_throw(partition.error_code)) {
                    throw partition_error(
                      model::topic_partition{
                        topic.name, partition.partition_index},
                      partition.error_code);
                }
            }
        }
    }
}

template<typename T>
requires(KafkaApi<typename T::api_type>)
void throw_on_error(const T& r) {
    return throw_on_error(r, [](error_code) { return true; });
}

} // namespace

ss::future<list_offsets_response>
client::list_offsets(list_offsets_request req) {
    co_return co_await gated_retry_with_mitigation([this, &req]() {
        return do_list_offsets(req);
    }).then([](auto res) {
        throw_on_error<list_offsets_response>(res);
        return res;
    });
}

ss::future<list_offsets_response>
client::list_offsets(model::topic_partition tp) {
    kafka::list_offsets_request req;
    req.data.topics.emplace_back(
      kafka::list_offset_topic{
        .name = std::move(tp.topic),
        .partitions = {kafka::list_offset_partition{
          .partition_index = tp.partition,
          .max_num_offsets = 1,
        }}});
    return list_offsets(std::move(req));
}

ss::future<list_offsets_response>
client::do_list_offsets(const list_offsets_request& unsharded_req) {
    // Group requests by the broker they are sent to
    chunked_hash_map<model::node_id, kafka::list_offsets_request> reqs;

    for (const auto& topic : unsharded_req.data.topics) {
        for (const auto& partition : topic.partitions) {
            model::topic_partition tp{topic.name, partition.partition_index};
            auto node_id = _cluster->get_topics().leader(tp);
            if (!node_id) {
                throw partition_error(
                  tp, error_code::unknown_topic_or_partition);
            }
            auto& topics
              = reqs.try_emplace(*node_id, kafka::list_offsets_request{})
                  .first->second.data.topics;
            auto topic_it = std::ranges::find(
              topics, tp.topic, &list_offset_topic::name);
            auto& topic = topic_it == topics.end()
                            ? topics.emplace_back(std::move(tp.topic))
                            : *topic_it;
            topic.partitions.emplace_back(partition);
        }
    }

    auto mapper = [this](auto kv) {
        auto node_id = kv.first;
        return _cluster->dispatch_to(
          node_id,
          std::move(kv.second),
          api_version_for(list_offsets_api::key));
    };

    auto reducer = [](list_offsets_response result, list_offsets_response val) {
        result.data.throttle_time_ms += val.data.throttle_time_ms;
        for (auto& topic : val.data.topics) {
            auto topic_it = std::ranges::find(
              result.data.topics,
              topic.name,
              &list_offset_topic_response::name);
            if (topic_it == result.data.topics.end()) {
                result.data.topics.push_back(std::move(topic));
            } else {
                std::ranges::move(
                  topic.partitions, std::back_inserter(topic_it->partitions));
            }
        }
        return result;
    };

    auto res = co_await ss::map_reduce(
      std::make_move_iterator(reqs.begin()),
      std::make_move_iterator(reqs.end()),
      mapper,
      list_offsets_response{},
      reducer);

    // We must always throw and retry if a custom mitigator is
    // configured, as the mitigator may need to handle this error
    throw_on_error(res, [&](auto ec) {
        return kafka::is_retriable(ec) || _external_mitigate.has_value();
    });

    co_return res;
}

namespace {
ss::future<fetch_response> maybe_throw_exception(
  model::node_id broker_id, model::topic_partition tp, fetch_response res) {
    if (res.data.error_code != error_code::none) {
        return ss::make_exception_future<fetch_response>(
          broker_error(broker_id, res.data.error_code));
    }

    const auto& topics = res.data.responses;
    if (topics.size() != 1 || topics[0].partitions.size() != 1) {
        return ss::make_exception_future<fetch_response>(
          partition_error(tp, error_code::unknown_server_error));
    }

    const auto& part = topics[0].partitions[0];
    if (part.error_code != error_code::none) {
        return ss::make_exception_future<fetch_response>(
          partition_error(tp, part.error_code));
    }

    return ss::make_ready_future<fetch_response>(std::move(res));
}
} // namespace

ss::future<fetch_response> client::fetch_partition(
  model::topic_partition tp,
  model::offset offset,
  std::chrono::milliseconds timeout,
  std::optional<int32_t> max_bytes) {
    const auto min_bytes = _consumer_config.fetch_min_bytes;
    const int32_t max_bytes_value = max_bytes.value_or(
      _consumer_config.fetch_max_bytes);
    auto build_request = [offset, min_bytes, max_bytes_value, timeout](
                           model::topic_partition& tp) {
        return make_fetch_request(
          tp, offset, min_bytes, max_bytes_value, timeout);
    };

    return ss::do_with(
      std::move(build_request),
      std::move(tp),
      [this](auto& build_request, model::topic_partition& tp) {
          return gated_retry_with_mitigation([this, &tp, &build_request]() {
                     auto leader_id = _cluster->get_topics().leader(tp);
                     if (!leader_id) {
                         return ss::make_exception_future<fetch_response>(
                           partition_error(
                             tp, error_code::unknown_topic_or_partition));
                     }
                     return _cluster
                       ->dispatch_to(
                         *leader_id,
                         build_request(tp),
                         api_version_for(fetch_api::key))
                       .then([leader_id, &tp](fetch_response res) {
                           return maybe_throw_exception(
                             *leader_id, tp, std::move(res));
                       });
                 })
            .handle_exception([&tp](std::exception_ptr ex) {
                return make_fetch_response(tp, ex);
            });
      });
}

ss::future<member_id>
client::create_consumer(const group_id& group_id, member_id name) {
    return find_coordinator_with_retry_and_mitigation(
             _gate,
             _retries_config.max_retries,
             _retries_config.retry_base_backoff,
             _cluster->get_brokers(),
             group_id,
             name,
             [this](std::exception_ptr ex) { return mitigate_error(ex); })
      .then([this, group_id, name](shared_broker_t coordinator) mutable {
          auto on_stopped = [this, group_id](const member_id& name) {
              _consumers[group_id].erase(name);
          };
          return make_consumer(
            _consumer_config,
            _retries_config,
            _cluster->get_topics(),
            _cluster->get_brokers(),
            std::move(coordinator),
            group_id,
            std::move(name),
            std::move(on_stopped),
            [this](std::exception_ptr ex) { return mitigate_error(ex); },
            _logger);
      })
      .then([this, group_id](shared_consumer_t c) {
          auto name = c->name();
          _consumers[group_id].insert(std::move(c));
          return name;
      });
}

ss::future<shared_consumer_t>
client::get_consumer(const group_id& g_id, const member_id& name) {
    if (auto g_it = _consumers.find(g_id); g_it != _consumers.end()) {
        if (auto c_it = g_it->second.find(name); c_it != g_it->second.end()) {
            return ss::make_ready_future<shared_consumer_t>(*c_it);
        }
    }
    return ss::make_exception_future<shared_consumer_t>(
      consumer_error(g_id, name, error_code::unknown_member_id));
}

ss::future<> client::remove_consumer(group_id g_id, const member_id& name) {
    auto c = co_await get_consumer(g_id, name);
    auto& group = _consumers[g_id];
    group.erase(c);
    if (group.empty()) {
        _consumers.erase(g_id);
    }

    auto res = co_await c->leave();
    if (res.data.error_code != error_code::none) {
        throw consumer_error(
          c->group_id(), c->member_id(), res.data.error_code);
    }
}

ss::future<> client::subscribe_consumer(
  const group_id& g_id,
  const member_id& name,
  chunked_vector<model::topic> topics) {
    return get_consumer(g_id, name)
      .then([topics{std::move(topics)}](shared_consumer_t c) mutable {
          return c->subscribe(std::move(topics));
      });
}

ss::future<chunked_vector<model::topic>>
client::consumer_topics(const group_id& g_id, const member_id& name) {
    return get_consumer(g_id, name).then([](shared_consumer_t c) {
        return ss::make_ready_future<chunked_vector<model::topic>>(
          c->topics().copy());
    });
}

ss::future<assignment>
client::consumer_assignment(const group_id& g_id, const member_id& name) {
    return get_consumer(g_id, name).then([](shared_consumer_t c) {
        return ss::make_ready_future<assignment>(c->assignment());
    });
}

ss::future<offset_fetch_response> client::consumer_offset_fetch(
  const group_id& g_id,
  const member_id& name,
  chunked_vector<offset_fetch_request_topic> topics) {
    return get_consumer(g_id, name)
      .then([this, topics{std::move(topics)}](shared_consumer_t c) mutable {
          return gated_retry_with_mitigation([c, topics{std::move(topics)}]() {
              return c->offset_fetch(topics.copy());
          });
      });
}

ss::future<offset_commit_response> client::consumer_offset_commit(
  const group_id& g_id,
  const member_id& name,
  chunked_vector<offset_commit_request_topic> topics) {
    return get_consumer(g_id, name)
      .then([this, topics{std::move(topics)}](shared_consumer_t c) mutable {
          return gated_retry_with_mitigation([c, topics{std::move(topics)}]() {
              return c->offset_commit(make_copy(topics));
          });
      });
}

ss::future<kafka::fetch_response> client::consumer_fetch(
  const group_id& g_id,
  const member_id& name,
  std::optional<std::chrono::milliseconds> timeout,
  std::optional<int32_t> max_bytes) {
    const auto config_timout = _consumer_config.request_timeout;
    const auto end = model::timeout_clock::now()
                     + std::min(config_timout, timeout.value_or(config_timout));
    return gated_retry_with_mitigation([this, g_id, name, end, max_bytes]() {
        vlog(
          _logger.debug, "consumer_fetch: group_id: {}, name: {}", g_id, name);
        return get_consumer(g_id, name)
          .then([end, max_bytes](shared_consumer_t c) {
              auto timeout = std::max(
                model::timeout_clock::duration{0},
                end - model::timeout_clock::now());
              return c->fetch(
                std::chrono::duration_cast<std::chrono::milliseconds>(timeout),
                max_bytes);
          })
          .then([this](kafka::fetch_response res) {
              bool has_error = std::any_of(
                res.data.responses.begin(),
                res.data.responses.end(),
                [](const auto& topics) {
                    return std::any_of(
                      topics.partitions.begin(),
                      topics.partitions.end(),
                      [](const auto& p) {
                          return p.error_code != error_code::none;
                      });
                });
              return (has_error ? _cluster->request_metadata_update()
                                : ss::now())
                .then(
                  [res{std::move(res)}]() mutable { return std::move(res); });
          });
    });
}

ss::future<kafka::describe_configs_response> client::describe_topics(
  chunked_vector<model::topic> topics,
  std::optional<chunked_vector<ss::sstring>> configuration_keys) {
    co_return co_await gated_retry_with_mitigation(
      [this, &topics, &configuration_keys]() {
          // Copy here to ensure retries can call the lambda again
          return do_describe_topics(
            topics.copy(),
            configuration_keys.transform(&chunked_vector<ss::sstring>::copy));
      });
}

ss::future<kafka::describe_configs_response> client::do_describe_topics(
  chunked_vector<model::topic> topics,
  std::optional<chunked_vector<ss::sstring>> configuration_keys) {
    chunked_vector<describe_configs_resource> dcr;
    dcr.reserve(topics.size());
    for (const auto& topic : topics) {
        dcr.push_back(
          describe_configs_resource{
            .resource_type = config_resource_type::topic,
            .resource_name = topic(),
            .configuration_keys = configuration_keys.transform(
              &chunked_vector<ss::sstring>::copy),
          });
    }

    co_return co_await _cluster->dispatch_to_any(
      describe_configs_request{.data = {.resources = std::move(dcr)}},
      api_version_for(describe_configs_request::api_type::key));
}

} // namespace kafka::client
