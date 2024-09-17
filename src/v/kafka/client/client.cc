// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/client.h"

#include "base/seastarx.h"
#include "kafka/client/broker.h"
#include "kafka/client/configuration.h"
#include "kafka/client/consumer.h"
#include "kafka/client/exceptions.h"
#include "kafka/client/logger.h"
#include "kafka/client/partitioners.h"
#include "kafka/client/sasl_client.h"
#include "kafka/client/utils.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/find_coordinator.h"
#include "kafka/protocol/leave_group.h"
#include "kafka/protocol/list_offset.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "ssx/future-util.h"
#include "utils/unresolved_address.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/coroutine/exception.hh>

#include <absl/container/node_hash_map.h>

#include <cstdlib>
#include <exception>
#include <system_error>

namespace kafka::client {

client::client(const YAML::Node& cfg, external_mitigate mitigater)
  : _config{cfg}
  , _seeds{_config.brokers()}
  , _topic_cache{}
  , _brokers{_config}
  , _wait_or_start_update_metadata{[this](wait_or_start::tag tag) {
      return update_metadata(tag);
  }}
  , _producer{_config, _topic_cache, _brokers, _config.produce_ack_level(), [this](std::exception_ptr ex) {
      return mitigate_error(std::move(ex));
  }}
  , _external_mitigate(std::move(mitigater)) {}

ss::future<> client::do_connect(net::unresolved_address addr) {
    return make_broker(unknown_node_id, addr, _config)
      .then([this](shared_broker_t broker) {
          return broker->dispatch(metadata_request{.list_all_topics = true})
            .then(
              [this](metadata_response res) { return apply(std::move(res)); })
            .finally([broker]() {});
      });
}

ss::future<> client::connect() {
    std::shuffle(
      _seeds.begin(), _seeds.end(), random_generators::internal::gen);

    return ss::do_with(size_t{0}, [this](size_t& retries) {
        return retry_with_mitigation(
          _config.retries(),
          _config.retry_base_backoff(),
          [this, retries]() {
              return do_connect(_seeds[retries % _seeds.size()]);
          },
          [this, &retries](std::exception_ptr ex) {
              ++retries;
              return _external_mitigate(ex);
          },
          _as);
    });
}

namespace {
template<typename Func>
ss::future<> catch_and_log(const client& c, Func&& f) noexcept {
    return ss::futurize_invoke(std::forward<Func>(f))
      .discard_result()
      .handle_exception([&c](std::exception_ptr e) {
          vlog(kclog.debug, "{}exception during stop: {}", c, e);
      });
}

} // namespace

ss::future<> client::stop() noexcept {
    _as.request_abort();
    co_await catch_and_log(*this, [this]() { return _producer.stop(); });
    co_await _gate.close();
    for (auto& [id, group] : _consumers) {
        while (!group.empty()) {
            auto c = *group.begin();
            co_await catch_and_log(*this, [c]() {
                // The consumer is constructed with an on_stopped which erases
                // istelf from the map after leave() completes.
                return c->leave();
            });
        }
    }
    co_await catch_and_log(*this, [this]() { return _brokers.stop(); });
}

ss::future<> client::update_metadata(wait_or_start::tag) {
    return ss::try_with_gate(_gate, [this]() {
        vlog(kclog.debug, "{}updating metadata", *this);
        return _brokers.any()
          .then([this](shared_broker_t broker) {
              return broker->dispatch(metadata_request{.list_all_topics = true})
                .then([this](metadata_response res) {
                    // Create new seeds from the returned set of brokers if
                    // they're not empty
                    if (!res.data.brokers.empty()) {
                        std::vector<net::unresolved_address> seeds;
                        seeds.reserve(res.data.brokers.size());
                        for (const auto& b : res.data.brokers) {
                            seeds.emplace_back(b.host, b.port);
                        }
                        std::swap(_seeds, seeds);
                    }

                    return apply(std::move(res));
                })
                .finally(
                  [this]() { vlog(kclog.trace, "{}updated metadata", *this); });
          })
          .handle_exception_type(
            [this](const broker_error&) { return connect(); });
    });
}

ss::future<> client::apply(metadata_response res) {
    try {
        co_await _brokers.apply(std::move(res.data.brokers));
        co_await _topic_cache.apply(std::move(res.data.topics));
        _controller = res.data.controller_id;
    } catch (const std::exception& ex) {
        vlog(kclog.debug, "{}Failed to apply metadata request: {}", *this, ex);
        throw;
    }
}

ss::future<> client::mitigate_error(std::exception_ptr ex) {
    return _external_mitigate(ex).handle_exception(
      [this](std::exception_ptr ex) {
          _gate.check();
          try {
              std::rethrow_exception(ex);
          } catch (const broker_error& ex) {
              // If there are no brokers, reconnect
              if (ex.node_id == unknown_node_id) {
                  vlog(kclog.warn, "{}broker_error: {}", *this, ex);
                  return connect();
              } else if (ex.error == error_code::not_controller) {
                  vlog(kclog.debug, "{}broker_error: {}", *this, ex);
                  return _wait_or_start_update_metadata();
              } else {
                  vlog(kclog.debug, "{}broker_error: {}", *this, ex);
                  return _brokers.erase(ex.node_id).then([this]() {
                      return _wait_or_start_update_metadata();
                  });
              }
          } catch (const consumer_error& ex) {
              switch (ex.error) {
              case error_code::coordinator_not_available:
                  vlog(kclog.debug, "{}consumer_error: {}", *this, ex);
                  return _wait_or_start_update_metadata();
              default:
                  vlog(kclog.warn, "{}consumer_error: {}", *this, ex);
                  return ss::make_exception_future(ex);
              }
          } catch (const partition_error& ex) {
              switch (ex.error) {
              case error_code::unknown_topic_or_partition:
              case error_code::not_leader_for_partition:
              case error_code::leader_not_available: {
                  vlog(kclog.debug, "{}partition_error: {}", *this, ex);
                  return _wait_or_start_update_metadata();
              }
              default:
                  vlog(kclog.warn, "{}partition_error: {}", *this, ex);
                  return ss::make_exception_future(ex);
              }
          } catch (const topic_error& ex) {
              switch (ex.error) {
              case error_code::unknown_topic_or_partition:
                  vlog(kclog.debug, "{}topic_error: {}", *this, ex);
                  return _wait_or_start_update_metadata();
              default:
                  vlog(kclog.warn, "{}topic_error: {}", *this, ex);
                  return ss::make_exception_future(ex);
              }
          } catch (const ss::gate_closed_exception&) {
              vlog(kclog.debug, "{}gate_closed_exception", *this);
          } catch (const std::system_error& ex) {
              if (net::is_reconnect_error(ex)) {
                  vlog(kclog.debug, "{}system_error: {}", *this, ex);
                  return _wait_or_start_update_metadata();
              } else {
                  vlog(kclog.warn, "{}system_error: {}", *this, ex);
                  return ss::make_exception_future(ex);
              }
          } catch (const std::exception& ex) {
              // TODO(Ben): Probably vassert
              vlog(kclog.error, "{}unknown exception: {}", *this, ex);
          }
          return ss::make_exception_future(ex);
      });
}

ss::future<produce_response::partition> client::produce_record_batch(
  model::topic_partition tp, model::record_batch&& batch) {
    return ss::try_with_gate(
      _gate, [this, tp{std::move(tp)}, batch{std::move(batch)}]() mutable {
          vlog(
            kclog.debug,
            "{}produce record_batch: {}, {{record_count: {}}}",
            *this,
            tp,
            batch.record_count());
          return _producer.produce(std::move(tp), std::move(batch));
      });
}

ss::future<produce_response> client::produce_records(
  model::topic topic, std::vector<record_essence> records) {
    absl::node_hash_map<model::partition_id, storage::record_batch_builder>
      partition_builders;

    // Assign records to batches per topic_partition
    for (auto& record : records) {
        auto p_id = record.partition_id;
        if (!p_id) {
            p_id = co_await gated_retry_with_mitigation([&, this]() {
                       return _topic_cache.partition_for(topic, record);
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
        partitions.emplace_back(kafka::produce_request::partition{
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
    responses_cv.emplace_back(topic_produce_response{
      .name{std::move(topic)}, .partitions{std::move(responses)}});

    co_return produce_response{
      .data = produce_response_data{
        .responses = std::move(responses_cv),
        .throttle_time_ms{{std::chrono::milliseconds{0}}}}};
}

ss::future<create_topics_response>
client::create_topic(kafka::creatable_topic req) {
    return gated_retry_with_mitigation([this, req{std::move(req)}]() {
        auto controller = _controller;
        return _brokers.find(controller)
          .then([req](auto broker) mutable {
              chunked_vector<kafka::creatable_topic> cv;
              cv.push_back(std::move(req));
              return broker->dispatch(kafka::create_topics_request{
                .data = {
                  .topics = std::move(cv),
                }});
          })
          .then([controller](auto res) {
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

ss::future<list_offsets_response>
client::list_offsets(model::topic_partition tp) {
    return gated_retry_with_mitigation(
      [this, tp{std::move(tp)}]() { return do_list_offsets(tp); });
}

ss::future<list_offsets_response>
client::do_list_offsets(model::topic_partition tp) {
    auto node_id = co_await _topic_cache.leader(tp);
    auto broker = co_await _brokers.find(node_id);
    chunked_vector<kafka::list_offset_topic> cv;
    cv.push_back(kafka::list_offset_topic{
      .name{tp.topic},
      .partitions{
        {
          {.partition_index{tp.partition}, .max_num_offsets = 1},
        },
      },
    });
    auto res = co_await broker->dispatch(kafka::list_offsets_request{
      .data = {
        .topics = std::move(cv),
      }});

    const auto& topics = res.data.topics;
    auto ec = error_code::none;
    if (topics.size() != 1 || topics[0].partitions.size() != 1) {
        co_return ss::coroutine::exception(std::make_exception_ptr(
          broker_error(node_id, error_code::unknown_server_error)));
    }
    ec = topics[0].partitions[0].error_code;
    if (ec != error_code::none) {
        co_return ss::coroutine::exception(
          std::make_exception_ptr(partition_error(tp, ec)));
    }
    co_return res;
}

namespace {
ss::future<fetch_response> maybe_throw_exception(
  shared_broker_t b, model::topic_partition tp, fetch_response res) {
    if (res.data.error_code != error_code::none) {
        return ss::make_exception_future<fetch_response>(
          broker_error(b->id(), res.data.error_code));
    }

    const auto& topics = res.data.topics;
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
  int32_t max_bytes,
  std::chrono::milliseconds timeout) {
    const auto min_bytes = _config.consumer_request_min_bytes();
    auto build_request =
      [offset, min_bytes, max_bytes, timeout](model::topic_partition& tp) {
          return make_fetch_request(tp, offset, min_bytes, max_bytes, timeout);
      };

    return ss::do_with(
      std::move(build_request),
      std::move(tp),
      [this](auto& build_request, model::topic_partition& tp) {
          return gated_retry_with_mitigation([this, &tp, &build_request]() {
                     return _topic_cache.leader(tp)
                       .then([this](model::node_id leader) {
                           return _brokers.find(leader);
                       })
                       .then([&tp, &build_request](shared_broker_t&& b) {
                           return b->dispatch(build_request(tp))
                             .then([b, &tp](fetch_response res) {
                                 return maybe_throw_exception(
                                   b, tp, std::move(res));
                             });
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
             _config,
             _brokers,
             group_id,
             name,
             [this](std::exception_ptr ex) { return mitigate_error(ex); })
      .then([this, group_id, name](shared_broker_t coordinator) mutable {
          auto on_stopped = [this, group_id](const member_id& name) {
              _consumers[group_id].erase(name);
          };
          return make_consumer(
            _config,
            _topic_cache,
            _brokers,
            std::move(coordinator),
            std::move(group_id),
            std::move(name),
            std::move(on_stopped),
            [this](std::exception_ptr ex) { return mitigate_error(ex); });
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
  std::vector<offset_fetch_request_topic> topics) {
    return get_consumer(g_id, name)
      .then([this, topics{std::move(topics)}](shared_consumer_t c) mutable {
          return gated_retry_with_mitigation([c, topics{std::move(topics)}]() {
              return c->offset_fetch(topics);
          });
      });
}

ss::future<offset_commit_response> client::consumer_offset_commit(
  const group_id& g_id,
  const member_id& name,
  std::vector<offset_commit_request_topic> topics) {
    return get_consumer(g_id, name)
      .then([this, topics{std::move(topics)}](shared_consumer_t c) mutable {
          return gated_retry_with_mitigation([c, topics{std::move(topics)}]() {
              return c->offset_commit(topics);
          });
      });
}

ss::future<kafka::fetch_response> client::consumer_fetch(
  const group_id& g_id,
  const member_id& name,
  std::optional<std::chrono::milliseconds> timeout,
  std::optional<int32_t> max_bytes) {
    const auto config_timout = _config.consumer_request_timeout.value();
    const auto end = model::timeout_clock::now()
                     + std::min(config_timout, timeout.value_or(config_timout));
    return gated_retry_with_mitigation([this, g_id, name, end, max_bytes]() {
        vlog(
          kclog.debug,
          "{}consumer_fetch: group_id: {}, name: {}",
          *this,
          g_id,
          name);
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
                res.data.topics.begin(),
                res.data.topics.end(),
                [](const auto& topics) {
                    return std::any_of(
                      topics.partitions.begin(),
                      topics.partitions.end(),
                      [](const auto& p) {
                          return p.error_code != error_code::none;
                      });
                });
              return (has_error ? _wait_or_start_update_metadata() : ss::now())
                .then(
                  [res{std::move(res)}]() mutable { return std::move(res); });
          });
    });
}

} // namespace kafka::client
