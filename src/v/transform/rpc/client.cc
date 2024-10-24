/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "transform/rpc/client.h"

#include "base/type_traits.h"
#include "cluster/errc.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "logger.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "model/transform.h"
#include "raft/errc.h"
#include "rpc/backoff_policy.h"
#include "rpc/errc.h"
#include "rpc/types.h"
#include "transform/rpc/deps.h"
#include "transform/rpc/rpc_service.h"
#include "transform/rpc/serde.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/map_reduce.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <boost/fusion/sequence/intrinsic/back.hpp>
#include <boost/outcome/basic_result.hpp>
#include <boost/range/irange.hpp>

#include <algorithm>
#include <chrono>
#include <memory>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <utility>

namespace transform::rpc {

namespace {
constexpr auto timeout = std::chrono::seconds(1);
constexpr int max_client_retries = 5;
static constexpr auto coordinator_partition = model::partition_id{0};

model::ntp offsets_ntp(model::partition_id id) {
    return {
      model::kafka_internal_namespace, model::transform_offsets_topic, id};
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
              "unexpected transform produce rpc error: {}",
              ::rpc::error_category().message(int(rpc_ec)));
            break;
        }
    } else {
        vlog(log.error, "unexpected transform produce error: {}", ec);
    }
    return cluster::errc::timeout;
}

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

template<typename T>
requires outcome::is_basic_result_v<T>
std::ostream& operator<<(std::ostream& os, T result) {
    if (result.has_value()) {
        return fmt::print(os, "{{ value: {} }}", result.value());
    } else {
        return fmt::print(os, "{{ error: {} }}", result.error());
    }
    return os;
}

} // namespace

client::client(
  model::node_id self,
  std::unique_ptr<partition_leader_cache> l,
  std::unique_ptr<topic_metadata_cache> md_cache,
  std::unique_ptr<topic_creator> t,
  std::unique_ptr<cluster_members_cache> m,
  ss::sharded<::rpc::connection_cache>* c,
  ss::sharded<local_service>* s,
  config::binding<size_t> b)
  : _self(self)
  , _cluster_members(std::move(m))
  , _leaders(std::move(l))
  , _topic_metadata(std::move(md_cache))
  , _topic_creator(std::move(t))
  , _connections(c)
  , _local_service(s)
  , _max_wasm_binary_size(std::move(b)) {}

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

ss::future<> client::start() {
    if (ss::this_shard_id() != 0) {
        co_return;
    }
    ssx::spawn_with_gate(_gate, [this] { return update_wasm_binary_size(); });
    _max_wasm_binary_size.watch([this] {
        ssx::spawn_with_gate(
          _gate, [this] { return update_wasm_binary_size(); });
    });
}

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
                  .with_node_client<impl::transform_rpc_client_protocol>(
                    _self,
                    ss::this_shard_id(),
                    node,
                    timeout,
                    [req = req.share()](
                      impl::transform_rpc_client_protocol proto) mutable {
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

ss::future<result<stored_wasm_binary_metadata, cluster::errc>>
client::store_wasm_binary(
  model::wasm_binary_iobuf data, model::timeout_clock::duration timeout) {
    co_return co_await retry([this, &data, timeout]() {
        return do_store_wasm_binary_once(
          model::share_wasm_binary(data), timeout);
    });
}

ss::future<result<stored_wasm_binary_metadata, cluster::errc>>
client::do_store_wasm_binary_once(
  model::wasm_binary_iobuf data, model::timeout_clock::duration timeout) {
    auto leader = co_await compute_wasm_binary_ntp_leader();
    if (!leader) {
        co_return cluster::errc::not_leader;
    }
    vlog(
      log.trace,
      "do_store_wasm_binary_once_request(node={}): size={}",
      *leader,
      data()->size_bytes());
    auto reply = co_await (
      leader == _self
        ? do_local_store_wasm_binary(std::move(data), timeout)
        : do_remote_store_wasm_binary(*leader, std::move(data), timeout));
    vlog(
      log.trace,
      "do_store_wasm_binary_once_response(node={}): {}",
      *leader,
      reply);
    co_return reply;
}

ss::future<result<stored_wasm_binary_metadata, cluster::errc>>
client::do_local_store_wasm_binary(
  model::wasm_binary_iobuf data, model::timeout_clock::duration timeout) {
    return _local_service->local().store_wasm_binary(std::move(data), timeout);
}

ss::future<result<stored_wasm_binary_metadata, cluster::errc>>
client::do_remote_store_wasm_binary(
  model::node_id node,
  model::wasm_binary_iobuf data,
  model::timeout_clock::duration timeout) {
    auto resp = co_await _connections->local()
                  .with_node_client<impl::transform_rpc_client_protocol>(
                    _self,
                    ss::this_shard_id(),
                    node,
                    timeout,
                    [timeout, data = std::move(data)](
                      impl::transform_rpc_client_protocol proto) mutable {
                        return proto.store_wasm_binary(
                          store_wasm_binary_request(std::move(data), timeout),
                          ::rpc::client_opts(
                            model::timeout_clock::now() + timeout));
                    })
                  .then(&::rpc::get_ctx_data<store_wasm_binary_reply>);
    if (resp.has_error()) {
        co_return map_errc(resp.assume_error());
    }
    auto reply = resp.value();
    if (reply.ec != cluster::errc::success) {
        co_return reply.ec;
    }
    co_return reply.stored;
}

ss::future<cluster::errc>
client::delete_wasm_binary(uuid_t key, model::timeout_clock::duration timeout) {
    return retry([this, key, timeout]() {
        return do_delete_wasm_binary_once(key, timeout);
    });
}

ss::future<cluster::errc> client::do_delete_wasm_binary_once(
  uuid_t key, model::timeout_clock::duration timeout) {
    auto leader = co_await compute_wasm_binary_ntp_leader();
    if (!leader) {
        co_return cluster::errc::not_leader;
    }
    vlog(
      log.trace,
      "do_delete_wasm_binary_once_request(node={}): {}",
      *leader,
      key);
    auto reply = co_await (
      leader == _self ? do_local_delete_wasm_binary(key, timeout)
                      : do_remote_delete_wasm_binary(*leader, key, timeout));
    vlog(
      log.trace,
      "do_delete_wasm_binary_once_response(node={}): {}",
      *leader,
      reply);
    co_return reply;
}

ss::future<cluster::errc> client::do_local_delete_wasm_binary(
  uuid_t key, model::timeout_clock::duration timeout) {
    return _local_service->local().delete_wasm_binary(key, timeout);
}

ss::future<cluster::errc> client::do_remote_delete_wasm_binary(
  model::node_id node, uuid_t key, model::timeout_clock::duration timeout) {
    auto resp
      = co_await _connections->local()
          .with_node_client<impl::transform_rpc_client_protocol>(
            _self,
            ss::this_shard_id(),
            node,
            timeout,
            [timeout, key](impl::transform_rpc_client_protocol proto) mutable {
                return proto.delete_wasm_binary(
                  delete_wasm_binary_request(key, timeout),
                  ::rpc::client_opts(model::timeout_clock::now() + timeout));
            })
          .then(&::rpc::get_ctx_data<delete_wasm_binary_reply>);
    if (resp.has_error()) {
        co_return map_errc(resp.assume_error());
    }
    co_return resp.value().ec;
}

ss::future<result<model::wasm_binary_iobuf, cluster::errc>>
client::load_wasm_binary(
  model::offset offset, model::timeout_clock::duration timeout) {
    return retry([this, offset, timeout]() {
        return do_load_wasm_binary_once(offset, timeout);
    });
}

ss::future<result<model::wasm_binary_iobuf, cluster::errc>>
client::do_load_wasm_binary_once(
  model::offset offset, model::timeout_clock::duration timeout) {
    auto leader = co_await compute_wasm_binary_ntp_leader();
    if (!leader) {
        co_return cluster::errc::not_leader;
    }
    vlog(
      log.trace,
      "do_load_wasm_binary_once_request(node={}): {}",
      *leader,
      offset);
    auto reply = co_await (
      leader == _self ? do_local_load_wasm_binary(offset, timeout)
                      : do_remote_load_wasm_binary(*leader, offset, timeout));
    vlog(
      log.trace,
      "do_load_wasm_binary_once_response(node={}): {}",
      *leader,
      reply);
    co_return reply;
}

ss::future<result<model::wasm_binary_iobuf, cluster::errc>>
client::do_local_load_wasm_binary(
  model::offset offset, model::timeout_clock::duration timeout) {
    return _local_service->local().load_wasm_binary(offset, timeout);
}

ss::future<result<model::wasm_binary_iobuf, cluster::errc>>
client::do_remote_load_wasm_binary(
  model::node_id node,
  model::offset offset,
  model::timeout_clock::duration timeout) {
    auto resp = co_await _connections->local()
                  .with_node_client<impl::transform_rpc_client_protocol>(
                    _self,
                    ss::this_shard_id(),
                    node,
                    timeout,
                    [timeout, offset](
                      impl::transform_rpc_client_protocol proto) mutable {
                        return proto.load_wasm_binary(
                          load_wasm_binary_request(offset, timeout),
                          ::rpc::client_opts(
                            model::timeout_clock::now() + timeout));
                    })
                  .then(&::rpc::get_ctx_data<load_wasm_binary_reply>);
    if (resp.has_error()) {
        co_return map_errc(resp.assume_error());
    }
    auto reply = std::move(resp).value();
    if (reply.ec != cluster::errc::success) {
        co_return reply.ec;
    }
    co_return std::move(reply.data);
}

ss::future<bool> client::try_create_wasm_binary_ntp() {
    cluster::topic_properties topic_props;
    topic_props.batch_max_bytes = _max_wasm_binary_size();
    // Mark all these as disabled
    topic_props.retention_bytes = tristate<size_t>();
    topic_props.retention_local_target_bytes = tristate<size_t>();
    topic_props.retention_duration = tristate<std::chrono::milliseconds>();
    topic_props.retention_local_target_ms
      = tristate<std::chrono::milliseconds>();
    topic_props.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;

    auto fut = co_await ss::coroutine::as_future<cluster::errc>(
      _topic_creator->create_topic(
        model::topic_namespace_view(model::wasm_binaries_internal_ntp),
        /*partition_count=*/1,
        topic_props));
    if (fut.failed()) {
        vlog(
          log.warn,
          "unable to create internal wasm binary topic: {}",
          std::move(fut).get_exception());
        co_return false;
    }
    cluster::errc ec = fut.get();
    if (
      ec == cluster::errc::success
      || ec == cluster::errc::topic_already_exists) {
        co_return true;
    }
    vlog(log.warn, "unable to create internal wasm binary topic: {}", ec);
    co_return false;
}

ss::future<bool> client::try_create_transform_offsets_topic() {
    cluster::topic_properties properties;
    properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    properties.retention_bytes = tristate<size_t>();
    properties.retention_local_target_bytes = tristate<size_t>();
    properties.retention_duration = tristate<std::chrono::milliseconds>();
    properties.retention_local_target_ms
      = tristate<std::chrono::milliseconds>();
    auto fut = co_await ss::coroutine::as_future<cluster::errc>(
      _topic_creator->create_topic(
        model::transform_offsets_nt, 1, std::move(properties)));
    if (fut.failed()) {
        vlog(
          log.warn,
          "unable to create internal offset tracking topic {}: {}",
          model::transform_offsets_nt,
          std::move(fut).get_exception());
        co_return false;
    }
    cluster::errc ec = fut.get();
    if (
      ec == cluster::errc::success
      || ec == cluster::errc::topic_already_exists) {
        co_return true;
    }
    vlog(log.warn, "unable to create internal offset tracking topic: {}", ec);
    co_return false;
}

ss::future<std::optional<model::node_id>>
client::compute_wasm_binary_ntp_leader() {
    auto leader = _leaders->get_leader_node(model::wasm_binaries_internal_ntp);
    if (!leader.has_value()) {
        if (_topic_metadata->find_topic_cfg(
              model::topic_namespace_view(model::wasm_binaries_internal_ntp))) {
            co_return std::nullopt;
        }
        bool success = co_await try_create_wasm_binary_ntp();
        if (!success) {
            co_return std::nullopt;
        }
        leader = _leaders->get_leader_node(model::wasm_binaries_internal_ntp);
    }
    co_return leader;
}

ss::future<result<model::partition_id, cluster::errc>>
client::find_coordinator(model::transform_offsets_key key) {
    // todo: lookup in a local cache first.
    return retry([key, this] { return find_coordinator_once(key); });
}

ss::future<result<model::partition_id, cluster::errc>>
client::find_coordinator_once(model::transform_offsets_key key) {
    auto ntp = offsets_ntp(coordinator_partition);
    auto leader = _leaders->get_leader_node(ntp);
    if (!leader) {
        // See if we know if this topic exists at all
        if (_topic_metadata->find_topic_cfg(model::transform_offsets_nt)) {
            co_return cluster::errc::not_leader;
        }
        // If not try to create it.
        bool success = co_await try_create_transform_offsets_topic();
        if (!success) {
            co_return cluster::errc::not_leader;
        }
        leader = _leaders->get_leader_node(ntp);
        if (!leader) {
            co_return cluster::errc::not_leader;
        }
    }
    find_coordinator_request request;
    request.add(key);
    vlog(log.trace, "find_coordinator_request(node={}): {}", *leader, key);
    auto response = co_await (
      *leader == _self
        ? do_local_find_coordinator(std::move(request))
        : do_remote_find_coordinator(*leader, std::move(request)));
    vlog(
      log.trace, "find_coordinator_response(node={}): {}", *leader, response);
    auto it = response.coordinators.find(key);
    if (it != response.coordinators.end()) {
        co_return it->second;
    } else {
        co_return response.errors.at(key);
    }
}

ss::future<find_coordinator_response>
client::do_local_find_coordinator(find_coordinator_request request) {
    return _local_service->local().find_coordinator(std::move(request));
}

ss::future<find_coordinator_response> client::do_remote_find_coordinator(
  model::node_id node, find_coordinator_request request) {
    auto response
      = co_await _connections->local()
          .with_node_client<impl::transform_rpc_client_protocol>(
            _self,
            ss::this_shard_id(),
            node,
            timeout,
            [req = request](impl::transform_rpc_client_protocol proto) mutable {
                return proto.find_coordinator(
                  std::move(req),
                  ::rpc::client_opts(model::timeout_clock::now() + timeout));
            })
          .then(&::rpc::get_ctx_data<find_coordinator_response>);
    if (!response) {
        find_coordinator_response find_response;
        cluster::errc ec = map_errc(response.error());
        for (const auto& key : request.keys) {
            find_response.errors[key] = ec;
        }
        co_return find_response;
    }
    co_return response.value();
}

ss::future<cluster::errc> client::batch_offset_commit(
  model::partition_id coordinator,
  absl::btree_map<model::transform_offsets_key, model::transform_offsets_value>
    kvs) {
    return retry([coordinator, kvs = std::move(kvs), this] {
        return batch_offset_commit_once(coordinator, kvs);
    });
}

ss::future<cluster::errc> client::batch_offset_commit_once(
  model::partition_id coordinator,
  absl::btree_map<model::transform_offsets_key, model::transform_offsets_value>
    kvs) {
    if (kvs.empty()) {
        co_return cluster::errc::success;
    }

    auto ntp = offsets_ntp(coordinator);
    auto leader = _leaders->get_leader_node(ntp);
    if (!leader) {
        co_return cluster::errc::not_leader;
    }

    offset_commit_request request{coordinator, std::move(kvs)};

    vlog(
      log.trace, "offset_commit_once_request(node={}): {}", *leader, request);
    auto resp = co_await (
      *leader == _self ? do_local_offset_commit(std::move(request))
                       : do_remote_offset_commit(*leader, std::move(request)));
    vlog(log.trace, "offset_commit_once_response(node={}): {}", *leader, resp);
    co_return resp.errc;
}

ss::future<offset_commit_response>
client::do_local_offset_commit(offset_commit_request request) {
    return _local_service->local().offset_commit(std::move(request));
}

ss::future<offset_commit_response> client::do_remote_offset_commit(
  model::node_id node, offset_commit_request request) {
    auto response = co_await _connections->local()
                      .with_node_client<impl::transform_rpc_client_protocol>(
                        _self,
                        ss::this_shard_id(),
                        node,
                        timeout,
                        [request = std::move(request)](
                          impl::transform_rpc_client_protocol proto) mutable {
                            return proto.offset_commit(
                              std::move(request),
                              ::rpc::client_opts(
                                model::timeout_clock::now() + timeout));
                        })
                      .then(&::rpc::get_ctx_data<offset_commit_response>);
    if (!response) {
        offset_commit_response commit_response{};
        commit_response.errc = map_errc(response.error());
        co_return commit_response;
    }
    co_return response.value();
}

ss::future<result<std::optional<model::transform_offsets_value>, cluster::errc>>
client::offset_fetch(model::transform_offsets_key key) {
    return retry([key, this] { return offset_fetch_once(key); });
}

ss::future<result<std::optional<model::transform_offsets_value>, cluster::errc>>
client::offset_fetch_once(model::transform_offsets_key key) {
    auto coordinator = co_await find_coordinator(key);
    if (!coordinator) {
        co_return coordinator.error();
    }

    auto ntp = offsets_ntp(coordinator.value());
    auto leader = _leaders->get_leader_node(ntp);
    if (!leader) {
        co_return cluster::errc::not_leader;
    }

    offset_fetch_request request{key, coordinator.value()};
    vlog(log.trace, "offset_fetch_once_request(node={}): {}", *leader, request);
    auto resp = co_await (
      *leader == _self ? do_local_offset_fetch(request)
                       : do_remote_offset_fetch(*leader, request));
    vlog(log.trace, "offset_fetch_once_response(node={}): {}", *leader, resp);

    {
        auto it = resp.errors.find(key);
        if (it != resp.errors.end()) {
            co_return it->second;
        }
    }
    auto it = resp.results.find(key);
    if (it == resp.results.end()) {
        co_return std::nullopt;
    }
    co_return it->second;
}

ss::future<offset_fetch_response>
client::do_local_offset_fetch(offset_fetch_request request) {
    return _local_service->local().offset_fetch(std::move(request));
}

ss::future<offset_fetch_response> client::do_remote_offset_fetch(
  model::node_id node, offset_fetch_request request) {
    auto response
      = co_await _connections->local()
          .with_node_client<impl::transform_rpc_client_protocol>(
            _self,
            ss::this_shard_id(),
            node,
            timeout,
            [request](impl::transform_rpc_client_protocol proto) mutable {
                return proto.offset_fetch(
                  std::move(request),
                  ::rpc::client_opts(model::timeout_clock::now() + timeout));
            })
          .then(&::rpc::get_ctx_data<offset_fetch_response>);
    if (!response) {
        offset_fetch_response fetch_response;
        cluster::errc ec = map_errc(response.error());
        for (const auto& key : request.keys) {
            fetch_response.errors[key] = ec;
        }
        co_return fetch_response;
    }
    co_return response.value();
}

ss::future<model::cluster_transform_report> client::generate_report() {
    co_return co_await ss::map_reduce(
      _cluster_members->all_cluster_members(),
      [this](model::node_id node) { return generate_one_report(node); },
      model::cluster_transform_report{},
      [](
        model::cluster_transform_report agg,
        const model::cluster_transform_report& report) {
          agg.merge(report);
          return agg;
      });
}

ss::future<model::cluster_transform_report>
client::generate_one_report(model::node_id node) {
    if (node == _self) {
        co_return co_await _local_service->local().compute_node_local_report();
    }
    auto result = co_await retry(
      [this, node]() { return generate_remote_report(node); });
    if (result.has_error()) {
        auto msg = cluster::error_category().message(int(result.error()));
        throw std::runtime_error(
          ss::format("failed to generate transform report: {}", msg));
    }
    co_return std::move(result).value();
}

ss::future<result<model::cluster_transform_report, cluster::errc>>
client::generate_remote_report(model::node_id node) {
    vlog(log.trace, "generate_one_report_request(node={})", node);
    auto resp = co_await _connections->local()
                  .with_node_client<impl::transform_rpc_client_protocol>(
                    _self,
                    ss::this_shard_id(),
                    node,
                    timeout,
                    [](impl::transform_rpc_client_protocol proto) mutable {
                        return proto.generate_report(
                          {},
                          ::rpc::client_opts(
                            model::timeout_clock::now() + timeout));
                    })
                  .then(&::rpc::get_ctx_data<generate_report_reply>);
    vlog(log.trace, "generate_one_report_response(node={}): {}", node, resp);
    if (resp.has_error()) {
        co_return map_errc(resp.error());
    }
    co_return std::move(resp).value().report;
}

ss::future<cluster::errc> client::create_transform_logs_topic() {
    co_return co_await retry(
      [this]() { return try_create_transform_logs_topic(); });
}

ss::future<cluster::errc> client::try_create_transform_logs_topic() {
    cluster::topic_properties topic_props;
    topic_props.retention_bytes = tristate<size_t>(2_GiB);
    topic_props.retention_duration = tristate<std::chrono::milliseconds>(
      std::chrono::duration_cast<std::chrono::milliseconds>(24h));
    topic_props.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::deletion;
    auto fut = co_await ss::coroutine::as_future<cluster::errc>(
      _topic_creator->create_topic(
        model::topic_namespace_view(model::transform_log_internal_nt),
        config::shard_local_cfg().default_topic_partitions(),
        std::move(topic_props)));
    if (fut.failed()) {
        throw std::runtime_error(fmt::format(
          "Error creating transform logs topic: {}", fut.get_exception()));
    }
    auto ec = fut.get();
    if (
      ec != cluster::errc::success
      && ec != cluster::errc::topic_already_exists) {
        throw std::runtime_error("Failed to create transform logs topic");
    }
    co_return ec;
}

template<typename Func>
std::invoke_result_t<Func> client::retry(Func&& func) {
    return retry_with_backoff(std::forward<Func>(func), &_as);
}

ss::future<> client::update_wasm_binary_size() {
    mutex::units _ = co_await _wasm_binary_max_size_updater_mu.get_units();
    auto tn = model::topic_namespace_view(model::wasm_binaries_internal_ntp);
    auto config = _topic_metadata->find_topic_cfg(tn);
    if (!config) {
        // Topic hasn't been created yet.
        co_return;
    }
    if (
      config->properties.batch_max_bytes.has_value()
      && config->properties.batch_max_bytes.value()
           == uint32_t(_max_wasm_binary_size())) {
        // Nothing to do.
        co_return;
    }
    // We need to update the size.
    ss::future<cluster::errc> fut
      = co_await ss::coroutine::as_future<cluster::errc>(retry([this, tn] {
            auto updates = cluster::incremental_topic_updates();
            updates.batch_max_bytes.value = _max_wasm_binary_size();
            updates.batch_max_bytes.op
              = cluster::incremental_update_operation::set;
            return _topic_creator->update_topic(
              cluster::topic_properties_update(
                model::topic_namespace(tn),
                updates,
                cluster::incremental_topic_custom_updates()));
        }));
    if (fut.failed()) {
        vlog(
          log.warn,
          "unable to update internal wasm binary topic size: {}",
          fut.get_exception());
    }
    cluster::errc ec = fut.get();
    if (ec == cluster::errc::success) {
        co_return;
    }
    vlog(
      log.warn,
      "unable to update internal wasm binary topic size: {}",
      cluster::error_category().message(int(ec)));
}

ss::future<result<model::transform_offsets_map, cluster::errc>>
client::list_committed_offsets() {
    auto cfg = _topic_metadata->find_topic_cfg(model::transform_offsets_nt);
    if (!cfg) {
        co_return model::transform_offsets_map{};
    }
    using ret_t = result<model::transform_offsets_map, cluster::errc>;
    co_return co_await ss::map_reduce(
      boost::irange(0, cfg->partition_count),
      [this](int32_t id) {
          auto partition = model::partition_id(id);
          return do_list_committed_offsets(partition);
      },
      ret_t(model::transform_offsets_map{}),
      [](ret_t agg, ret_t one) {
          if (agg.has_error()) {
              return agg;
          }
          if (one.has_error()) {
              return one;
          }
          agg.value().merge(one.value());
          return agg;
      });
}

ss::future<result<model::transform_offsets_map, cluster::errc>>
client::do_list_committed_offsets(model::partition_id partition) {
    return retry(
      [this, partition] { return do_list_committed_offsets_once(partition); });
}

ss::future<result<model::transform_offsets_map, cluster::errc>>
client::do_list_committed_offsets_once(model::partition_id partition) {
    auto leader = _leaders->get_leader_node(offsets_ntp(partition));
    if (!leader) {
        co_return cluster::errc::not_leader;
    }
    if (leader == _self) {
        co_return co_await do_local_list_committed_offsets(partition);
    } else {
        co_return co_await do_remote_list_committed_offsets(
          *leader, partition, timeout);
    }
}

ss::future<result<model::transform_offsets_map, cluster::errc>>
client::do_local_list_committed_offsets(model::partition_id partition) {
    return _local_service->local().list_committed_offsets(
      list_commits_request(partition));
}

ss::future<result<model::transform_offsets_map, cluster::errc>>
client::do_remote_list_committed_offsets(
  model::node_id node,
  model::partition_id partition,
  model::timeout_clock::duration timeout) {
    vlog(log.trace, "list_committed_offsets(node={}): {}", node, partition);
    auto resp = co_await _connections->local()
                  .with_node_client<impl::transform_rpc_client_protocol>(
                    _self,
                    ss::this_shard_id(),
                    node,
                    timeout,
                    [partition, timeout](
                      impl::transform_rpc_client_protocol proto) mutable {
                        return proto.list_committed_offsets(
                          list_commits_request(partition),
                          ::rpc::client_opts(
                            model::timeout_clock::now() + timeout));
                    })
                  .then(&::rpc::get_ctx_data<list_commits_reply>);
    vlog(log.trace, "list_committed_offsets(node={}): {}", node, resp);
    if (resp.has_error()) {
        co_return map_errc(resp.error());
    }
    auto reply = std::move(resp).value();
    if (reply.errc != cluster::errc::success) {
        co_return reply.errc;
    }
    co_return reply.map;
}

ss::future<cluster::errc>
client::delete_committed_offsets(absl::btree_set<model::transform_id> ids) {
    if (ids.empty()) {
        co_return cluster::errc::success;
    }
    auto cfg = _topic_metadata->find_topic_cfg(model::transform_offsets_nt);
    if (!cfg) {
        co_return cluster::errc::topic_not_exists;
    }
    co_return co_await ss::map_reduce(
      boost::irange(0, cfg->partition_count),
      [this, &ids](int32_t id) {
          auto partition = model::partition_id(id);
          return do_delete_committed_offsets(partition, ids);
      },
      cluster::errc::success,
      [](cluster::errc agg, cluster::errc one) {
          if (agg != cluster::errc::success) {
              return agg;
          }
          return one;
      });
}

ss::future<cluster::errc> client::do_delete_committed_offsets(
  model::partition_id partition, absl::btree_set<model::transform_id> ids) {
    return retry([this, partition, ids = std::move(ids)] {
        return do_delete_committed_offsets_once(partition, ids);
    });
}

ss::future<cluster::errc> client::do_delete_committed_offsets_once(
  model::partition_id partition, absl::btree_set<model::transform_id> ids) {
    auto leader = _leaders->get_leader_node(offsets_ntp(partition));
    if (!leader) {
        co_return cluster::errc::not_leader;
    }
    if (leader == _self) {
        co_return co_await do_local_delete_committed_offsets(
          partition, std::move(ids));
    } else {
        co_return co_await do_remote_delete_committed_offsets(
          *leader, partition, std::move(ids), timeout);
    }
}

ss::future<cluster::errc> client::do_local_delete_committed_offsets(
  model::partition_id partition, absl::btree_set<model::transform_id> ids) {
    return _local_service->local().delete_committed_offsets(
      partition, std::move(ids));
}

ss::future<cluster::errc> client::do_remote_delete_committed_offsets(
  model::node_id node,
  model::partition_id partition,
  absl::btree_set<model::transform_id> ids,
  model::timeout_clock::duration timeout) {
    vlog(
      log.trace,
      "delete_committed_offsets(node={}): {} {}",
      node,
      partition,
      ids.size());
    auto resp = co_await _connections->local()
                  .with_node_client<impl::transform_rpc_client_protocol>(
                    _self,
                    ss::this_shard_id(),
                    node,
                    timeout,
                    [partition, timeout, ids = std::move(ids)](
                      impl::transform_rpc_client_protocol proto) mutable {
                        return proto.delete_committed_offsets(
                          delete_commits_request(partition, std::move(ids)),
                          ::rpc::client_opts(
                            model::timeout_clock::now() + timeout));
                    })
                  .then(&::rpc::get_ctx_data<delete_commits_reply>);
    vlog(
      log.trace,
      "delete_committed_offsets(node={}): {} {}",
      node,
      resp,
      ids.size());
    if (resp.has_error()) {
        co_return map_errc(resp.error());
    }
    co_return resp.value().errc;
}

} // namespace transform::rpc
