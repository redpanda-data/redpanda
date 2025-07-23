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
#include "rpc/backoff_policy.h"
#include "rpc/connection_cache.h"

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
    auto backoff = ::rpc::make_exponential_backoff_policy<ss::lowres_clock>(
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
            if (attempts < max_client_retries) {
                co_return co_await std::move(fut);
            }
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
  ss::sharded<::rpc::connection_cache>* c,
  ss::sharded<local_service>* s)
  : _self(self)
  , _leaders(std::move(l))
  , _topic_creator(std::move(t))
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
  std::optional<int32_t> partition_count) {
    co_return co_await retry(
      [this, tp, partition_count, p = std::move(props)]() {
          return try_create_topic(tp, p, partition_count);
      });
}

ss::future<cluster::errc> client::try_create_topic(
  model::topic_namespace_view nt,
  cluster::topic_properties props,
  std::optional<int32_t> partition_count) {
    auto fut = co_await ss::coroutine::as_future<cluster::errc>(
      _topic_creator->create_topic(
        nt,
        partition_count.value_or(
          config::shard_local_cfg().default_topic_partitions()),
        std::move(props)));
    if (fut.failed()) {
        throw std::runtime_error(fmt::format(
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

} // namespace kafka::data::rpc
