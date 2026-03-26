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

#include "base/type_traits.h"
#include "cluster/errc.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "kafka/data/rpc/client.h"
#include "kafka/data/rpc/deps.h"
#include "logger.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/transform.h"
#include "raft/errc.h"
#include "rpc/errc.h"
#include "rpc/types.h"
#include "transform/rpc/client.h"
#include "transform/rpc/deps.h"
#include "transform/rpc/rpc_service.h"
#include "transform/rpc/serde.h"
#include "utils/backoff_policy.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/as_future.hh>

#include <boost/outcome/basic_result.hpp>
#include <boost/range/irange.hpp>

#include <chrono>

using namespace std::chrono_literals;

namespace transform::rpc {

namespace {

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

} // namespace

template<typename Func>
std::invoke_result_t<Func> client::retry(Func&& func) {
    return retry_with_backoff(std::forward<Func>(func), &_as);
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
      _kafka_client->local().try_create_topic(
        model::topic_namespace_view(model::wasm_binaries_internal_ntp),
        topic_props,
        /*partition_count=*/1));
    if (fut.failed()) {
        auto ex = std::move(fut).get_exception();
        vlog(log.warn, "unable to create internal wasm binary topic: {}", ex);
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

ss::future<> client::update_wasm_binary_size() {
    ssx::mutex::units _ = co_await _wasm_binary_max_size_updater_mu.get_units();
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
            return _kafka_client->local().update_topic(
              cluster::topic_properties_update(
                model::topic_namespace(tn),
                updates,
                cluster::incremental_topic_custom_updates()));
        }));
    if (fut.failed()) {
        auto ex = fut.get_exception();
        vlog(
          log.warn, "unable to update internal wasm binary topic size: {}", ex);
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

} // namespace transform::rpc
