/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/data/rpc/client.h"

#include "kafka/data/rpc/rpc_service.h"
#include "logger.h"
#include "rpc/connection_cache.h"
#include "ssx/async_algorithm.h"
#include "utils/backoff_policy.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

#include <chrono>

namespace kafka::data::rpc {

namespace {
constexpr auto timeout = std::chrono::seconds(1);
constexpr int max_client_retries = 5;

template<typename T>
concept ResponseWithErrorCode = requires(T resp) {
    { resp.ec } -> std::same_as<cluster::errc>;
};

template<typename Func>
std::invoke_result_t<Func> retry_with_backoff(Func func, ss::abort_source* as) {
    constexpr auto base_backoff_duration = 100ms;
    constexpr auto max_backoff_duration = base_backoff_duration
                                          * max_client_retries;
    auto backoff = ::make_exponential_backoff_policy<ss::lowres_clock>(
      base_backoff_duration, max_backoff_duration);
    int attempts = 0;
    while (true) {
        ++attempts;
        co_await ss::sleep_abortable<ss::lowres_clock>(
          backoff.current_backoff_duration(), *as);
        using result_type
          = ss::futurize<typename std::invoke_result_t<Func>>::value_type;
        auto fut = co_await ss::coroutine::as_future<result_type>(
          ss::futurize_invoke(func));
        backoff.next_backoff();
        if (fut.failed()) {
            if (attempts >= max_client_retries) {
                co_return co_await std::move(fut);
            }
            auto ex = fut.get_exception();
            vlog(log.debug, "Retrying after error: {}", ex);
            continue;
        }
        result_type r = fut.get();
        cluster::errc ec = cluster::errc::success;
        if constexpr (std::is_same_v<cluster::errc, result_type>) {
            ec = r;
        } else if constexpr (outcome::is_basic_result_v<result_type>) {
            ec = r.has_error() ? r.error() : cluster::errc::success;
        } else if constexpr (ResponseWithErrorCode<result_type>) {
            ec = r.ec;
        } else {
            static_assert(
              base::unsupported_type<result_type>::value,
              "unsupported response type");
        }
        switch (ec) {
        case cluster::errc::not_leader:
        case cluster::errc::timeout:
            // We've ran out of retries, return our error
            if (attempts >= max_client_retries) {
                co_return r;
            }
            break;
        case cluster::errc::success:
        // Don't retry arbitrary error codes.
        default:
            co_return r;
        }
    }
    __builtin_unreachable();
}

cluster::errc map_errc(std::error_code ec) {
    if (ec.category() == cluster::error_category()) {
        return static_cast<cluster::errc>(ec.value());
    } else if (ec.category() == raft::error_category()) {
        auto raft_ec = static_cast<raft::errc>(ec.value());
        switch (raft_ec) {
        case raft::errc::not_leader:
        case raft::errc::leadership_transfer_in_progress:
            return cluster::errc::not_leader;
        default:
            vlog(
              log.error,
              "unexpected transform produce raft error: {}",
              ::rpc::error_category().message(int(raft_ec)));
            break;
        }
    } else if (ec.category() == ::rpc::error_category()) {
        auto rpc_ec = static_cast<::rpc::errc>(ec.value());
        switch (rpc_ec) {
        case ::rpc::errc::client_request_timeout:
        case ::rpc::errc::connection_timeout:
        case ::rpc::errc::disconnected_endpoint:
        case ::rpc::errc::exponential_backoff:
        case ::rpc::errc::shutting_down:
        case ::rpc::errc::service_unavailable:
            return cluster::errc::timeout;
        default:
            vlog(
              log.error,
              "kafka data produce rpc error: {}",
              ::rpc::error_category().message(int(rpc_ec)));
            break;
        }
    } else {
        vlog(log.error, "unexpected kafka data produce error: {}", ec);
    }
    return cluster::errc::timeout;
}
} // namespace

client::client(
  model::node_id self,
  std::unique_ptr<kafka::data::rpc::partition_leader_cache> l,
  std::unique_ptr<kafka::data::rpc::topic_creator> t,
  std::unique_ptr<kafka::data::rpc::topic_metadata_cache> mdc,
  ss::sharded<::rpc::connection_cache>* c,
  ss::sharded<local_service>* s)
  : _self(self)
  , _leaders(std::move(l))
  , _topic_creator(std::move(t))
  , _metadata_cache(std::move(mdc))
  , _connections(c)
  , _local_service(s) {}

template<typename Func>
std::invoke_result_t<Func> client::retry(Func&& func) {
    return retry_with_backoff(std::forward<Func>(func), &_as);
}

ss::future<cluster::errc> client::produce(
  model::topic_partition tp, ss::chunked_fifo<model::record_batch> batches) {
    if (batches.empty()) {
        co_return cluster::errc::success;
    }
    produce_request req;
    req.topic_data.emplace_back(std::move(tp), std::move(batches));
    req.timeout = timeout;
    co_return co_await retry(
      [this, &req]() { return do_produce_once(req.share()); });
}

ss::future<cluster::errc>
client::produce(model::topic_partition tp, model::record_batch batch) {
    produce_request req;
    req.topic_data.emplace_back(std::move(tp), std::move(batch));
    req.timeout = timeout;
    co_return co_await retry(
      [this, &req]() { return do_produce_once(req.share()); });
}

ss::future<cluster::errc> client::do_produce_once(produce_request req) {
    vassert(
      req.topic_data.size() == 1,
      "expected a single batch: {}",
      req.topic_data.size());
    const auto& tp = req.topic_data.front().tp;
    auto leader = _leaders->get_leader_node(
      model::topic_namespace_view(model::kafka_namespace, tp.topic),
      tp.partition);
    if (!leader) {
        co_return cluster::errc::not_leader;
    }
    vlog(log.trace, "do_produce_once_request(node={}): {}", *leader, req);
    auto reply = co_await (
      *leader == _self ? do_local_produce(std::move(req))
                       : do_remote_produce(*leader, std::move(req)));
    vlog(log.trace, "do_produce_once_reply(node={}): {}", *leader, req);
    vassert(
      reply.results.size() == 1,
      "expected a single result: {}",
      reply.results.size());

    co_return reply.results.front().err;
}

ss::future<cluster::errc> client::create_topic(
  model::topic_namespace_view tp,
  cluster::topic_properties props,
  std::optional<int32_t> partition_count,
  std::optional<int16_t> replication_factor) {
    co_return co_await retry(
      [this, tp, partition_count, replication_factor, p = std::move(props)]() {
          return try_create_topic(tp, p, partition_count, replication_factor);
      });
}

ss::future<cluster::errc> client::try_create_topic(
  model::topic_namespace_view nt,
  cluster::topic_properties props,
  std::optional<int32_t> partition_count,
  std::optional<int16_t> replication_factor) {
    auto fut = co_await ss::coroutine::as_future<cluster::errc>(
      _topic_creator->create_topic(
        nt,
        partition_count.value_or(
          config::shard_local_cfg().default_topic_partitions()),
        std::move(props),
        replication_factor));
    if (fut.failed()) {
        throw std::runtime_error(
          fmt::format(
            "Error creating topic '{}': {}", nt, fut.get_exception()));
    }
    auto ec = fut.get();
    if (
      ec != cluster::errc::success
      && ec != cluster::errc::topic_already_exists) {
        throw std::runtime_error(
          fmt::format("Failed to create topic '{}'", nt));
    }
    co_return ec;
}

ss::future<cluster::errc>
client::update_topic(cluster::topic_properties_update update) {
    return _topic_creator->update_topic(std::move(update));
}

ss::future<> client::start() { return ss::now(); }

ss::future<> client::stop() {
    _as.request_abort();
    co_await _gate.close();
}

ss::future<produce_reply> client::do_local_produce(produce_request req) {
    auto r = co_await _local_service->local().produce(
      std::move(req.topic_data), req.timeout);
    co_return produce_reply(std::move(r));
}

ss::future<produce_reply>
client::do_remote_produce(model::node_id node, produce_request req) {
    auto resp = co_await _connections->local()
                  .with_node_client<
                    kafka::data::rpc::impl::kafka_data_rpc_client_protocol>(
                    _self,
                    ss::this_shard_id(),
                    node,
                    timeout,
                    [req = req.share()](
                      impl::kafka_data_rpc_client_protocol proto) mutable {
                        return proto.produce(
                          std::move(req),
                          ::rpc::client_opts(
                            model::timeout_clock::now() + timeout));
                    })
                  .then(&::rpc::get_ctx_data<produce_reply>);
    if (resp.has_error()) {
        cluster::errc ec = map_errc(resp.assume_error());
        produce_reply reply;
        for (const auto& data : req.topic_data) {
            reply.results.emplace_back(data.tp, ec);
        }
        co_return reply;
    }
    co_return std::move(resp).value();
}

namespace {
chunked_vector<topic_partitions> topic_partition_map_to_vector(
  chunked_hash_map<model::topic, chunked_vector<model::partition_id>> map) {
    chunked_vector<topic_partitions> result;
    result.reserve(map.size());
    for (auto& [topic, partitions] : map) {
        topic_partitions tp;
        tp.topic = std::move(topic);
        tp.partitions = std::move(partitions);
        result.push_back(std::move(tp));
    }
    return result;
}

void join_maps(
  partition_offsets_map& aggregate, partition_offsets_map&& element) {
    for (auto& [topic, partitions] : element) {
        auto& aggregate_partitions = aggregate[topic];
        for (auto& [partition, offsets] : partitions) {
            aggregate_partitions[partition] = std::move(offsets);
        }
    }
}

chunked_vector<topic_partitions>
copy(const chunked_vector<topic_partitions>& vec) {
    chunked_vector<topic_partitions> result;
    result.reserve(vec.size());
    for (const auto& item : vec) {
        result.push_back(
          topic_partitions{
            .topic = item.topic,
            .partitions = item.partitions.copy(),
          });
    }
    return result;
}

partition_offsets_map make_error_results(
  const chunked_vector<topic_partitions>& topic_partitions,
  cluster::errc error) {
    partition_offsets_map results;
    results.reserve(topic_partitions.size());
    for (auto& topic : topic_partitions) {
        results[topic.topic].reserve(topic.partitions.size());
        for (auto& partition : topic.partitions) {
            results[topic.topic][partition] = partition_offset_result(error);
        }
    }
    return results;
}
} // namespace

ss::future<result<partition_offsets_map, cluster::errc>>
client::get_partition_offsets(
  chunked_vector<topic_partitions> topic_partitions) {
    partition_offsets_map results;
    ssx::async_counter cnt;
    chunked_hash_map<
      model::node_id,
      chunked_hash_map<model::topic, chunked_vector<model::partition_id>>>
      per_node_partitions;

    for (auto& topic : topic_partitions) {
        co_await ssx::async_for_each_counter(
          cnt,
          topic.partitions,
          [this, &topic, &per_node_partitions, &results](
            model::partition_id partition) {
              model::topic_namespace_view tp_ns(
                model::kafka_namespace, topic.topic);
              auto tp_cfg = _metadata_cache->find_topic_cfg(tp_ns);
              if (!tp_cfg) {
                  results[topic.topic][partition] = partition_offset_result(
                    cluster::errc::topic_not_exists);
                  return;
              }
              auto leader = _leaders->get_leader_node(
                model::topic_namespace_view(
                  model::kafka_namespace, topic.topic),
                partition);
              if (!leader) {
                  results[topic.topic][partition] = partition_offset_result(
                    cluster::errc::not_leader);
              } else {
                  per_node_partitions[*leader][topic.topic].push_back(
                    partition);
              }
          });
    }
    co_await ss::parallel_for_each(
      per_node_partitions, [this, &results](auto& pair) {
          if (pair.first == _self) {
              // If the leader is this node, we can use the local service
              return _local_service->local()
                .get_offsets(
                  topic_partition_map_to_vector(std::move(pair.second)))
                .then([&results](partition_offsets_map local_results) {
                    join_maps(results, std::move(local_results));
                });
          }
          auto topic_partitions = topic_partition_map_to_vector(
            std::move(pair.second));
          return get_remote_partition_offsets(
                   pair.first, copy(topic_partitions))
            .then([leader_id = pair.first,
                   topic_partitions = std::move(topic_partitions)](
                    result<partition_offsets_map, cluster::errc> res) {
                if (res.has_error()) {
                    vlog(
                      log.warn,
                      "Failed to get partition offsets from node {}: {}",
                      leader_id,
                      res.error());
                    return make_error_results(topic_partitions, res.error());
                }
                return std::move(res.value());
            })
            .then([&results](partition_offsets_map remote_results) {
                join_maps(results, std::move(remote_results));
            });
      });
    co_return results;
}

ss::future<result<partition_offsets_map, cluster::errc>>
client::get_remote_partition_offsets(
  model::node_id leader, chunked_vector<topic_partitions> topics) {
    using ret_t = result<partition_offsets_map, cluster::errc>;
    auto result
      = co_await _connections->local()
          .with_node_client<
            kafka::data::rpc::impl::kafka_data_rpc_client_protocol>(
            _self,
            ss::this_shard_id(),
            leader,
            timeout,
            [topics = std::move(topics)](
              impl::kafka_data_rpc_client_protocol proto) mutable {
                return proto.get_offsets(
                  get_offsets_request(std::move(topics)),
                  ::rpc::client_opts(model::timeout_clock::now() + timeout));
            })
          .then([](auto ctx) {
              return ::rpc::get_ctx_data<get_offsets_reply>(std::move(ctx));
          });

    if (result.has_error()) {
        co_return ret_t(map_errc(result.assume_error()));
    }
    co_return ret_t(std::move(result.value().partition_offsets));
}

ss::future<result<consume_reply, cluster::errc>> client::consume(
  model::topic_partition tp,
  kafka::offset start_offset,
  kafka::offset max_offset,
  size_t min_bytes,
  size_t max_bytes,
  model::timeout_clock::duration timeout) {
    using ret_t = result<consume_reply, cluster::errc>;

    // Check if topic exists first
    auto topic_cfg = _metadata_cache->find_topic_cfg(
      model::topic_namespace_view(model::kafka_namespace, tp.topic));
    if (!topic_cfg) {
        consume_reply reply;
        reply.tp = tp;
        reply.err = cluster::errc::topic_not_exists;
        co_return ret_t(std::move(reply));
    }

    // Find the leader for this partition
    auto ktp = model::ktp(tp.topic, tp.partition);
    auto leader = _leaders->get_leader_node(
      model::topic_namespace_view(model::kafka_namespace, tp.topic),
      tp.partition);

    if (!leader) {
        consume_reply reply;
        reply.tp = tp;
        reply.err = cluster::errc::not_leader;
        co_return ret_t(std::move(reply));
    }

    consume_request req(
      tp, start_offset, max_offset, min_bytes, max_bytes, timeout);

    // If leader is local, call local service
    if (*leader == _self) {
        auto reply = co_await _local_service->local().consume(std::move(req));
        co_return ret_t(std::move(reply));
    }

    // Otherwise call remote service
    auto result
      = co_await _connections->local()
          .with_node_client<
            kafka::data::rpc::impl::kafka_data_rpc_client_protocol>(
            _self,
            ss::this_shard_id(),
            *leader,
            timeout,
            [req = std::move(req),
             timeout](impl::kafka_data_rpc_client_protocol proto) mutable {
                return proto.consume(
                  std::move(req),
                  ::rpc::client_opts(model::timeout_clock::now() + timeout));
            })
          .then([](auto ctx) {
              return ::rpc::get_ctx_data<consume_reply>(std::move(ctx));
          });

    if (result.has_error()) {
        consume_reply reply;
        reply.tp = tp;
        reply.err = map_errc(result.assume_error());
        co_return ret_t(std::move(reply));
    }

    co_return ret_t(std::move(result.value()));
}
} // namespace kafka::data::rpc
