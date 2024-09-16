// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/seastarx.h"
#include "base/vformat.h"
#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/shard_table.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "rpc/connection_cache.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace rpc {
class connection_cache;
} // namespace rpc

namespace cluster {

template<typename req_t, typename resp_t, typename handler_t>
concept IsHandler = requires(req_t req, resp_t resp, handler_t handler) {
    // RPC protocol with which to perform remote dispatch.
    { typename handler_t::proto_t(nullptr) };

    // Returns a short description of the process being dispatched to leader,
    // suitable for logging.
    { handler_t::process_name() } -> std::same_as<ss::sstring>;

    // Returns an error response with the given code.
    { handler_t::error_resp(cluster::errc()) } -> std::same_as<resp_t>;

    // Sends the given request on the given client protocol with the given
    // timeout.
    {
        handler_t::dispatch(
          typename handler_t::proto_t(nullptr),
          req,
          model::timeout_clock::duration())
    } -> std::same_as<ss::future<result<rpc::client_context<resp_t>>>>;

    // Processes the given request on the given shard. This is expected to run
    // on the leader.
    {
        handler.process(ss::shard_id(), req)
    } -> std::same_as<ss::future<resp_t>>;
};

// Encapsulates logic to direct RPCs to a specific NTP's leader.
//
// When a caller invokes process_or_dispatch() directed at an NTP whose leader
// is on a different broker, the router redirects the request to the service
// located on the leader broker.
//
// When a caller is colocated with the leader, the router calls
// find_shard_and_process() directly, calling into the handler's process()
// implementations.
template<typename req_t, typename resp_t, typename handler_t>
requires IsHandler<req_t, resp_t, handler_t>
class leader_router {
public:
    using duration = model::timeout_clock::duration;
    leader_router(
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      handler_t&,
      model::node_id self,
      int16_t retries,
      std::chrono::milliseconds timeout);

    // Sends the given request to the leader, or processes it on the
    // appropriate shard if this node is the leader.
    ss::future<resp_t>
    process_or_dispatch(req_t req, model::ntp, duration timeout);

    // Finds the shard appropriate for the NTP and processes the given request,
    // expecting the leader is on this node.
    ss::future<resp_t>
    find_shard_and_process(req_t req, model::ntp, duration timeout);

    void request_shutdown() {
        vlog(
          clusterlog.debug,
          "Requesting shutdown of {} router",
          handler_t::process_name());
        _as.request_abort();
    }

    ss::future<> shutdown() {
        vlog(
          clusterlog.debug,
          "Shutting down {} router",
          handler_t::process_name());
        if (!_as.abort_requested()) {
            _as.request_abort();
        }
        co_await _gate.close();
        vlog(
          clusterlog.debug, "Shut down {} router", handler_t::process_name());
    }

private:
    // Sends the given request to the given leader node ID..
    virtual ss::future<resp_t>
    dispatch_to_leader(req_t req, model::node_id, duration timeout);

    ss::abort_source _as;
    ss::gate _gate;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    ss::sharded<rpc::connection_cache>& _connection_cache;
    ss::sharded<partition_leaders_table>& _leaders;
    handler_t& _handler;
    const model::node_id _self;

    int16_t _retries{1};
    std::chrono::milliseconds _retry_delay_ms;
};

template<typename req_t, typename resp_t, typename handler_t>
requires IsHandler<req_t, resp_t, handler_t>
leader_router<req_t, resp_t, handler_t>::leader_router(
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  handler_t& handler,
  model::node_id self,
  int16_t retries,
  std::chrono::milliseconds retry_delay_ms)
  : _shard_table(shard_table)
  , _metadata_cache(metadata_cache)
  , _connection_cache(connection_cache)
  , _leaders(leaders)
  , _handler(handler)
  , _self(self)
  , _retries(retries)
  , _retry_delay_ms(retry_delay_ms) {}

template<typename req_t, typename resp_t, typename handler_t>
requires IsHandler<req_t, resp_t, handler_t>
ss::future<resp_t> leader_router<req_t, resp_t, handler_t>::process_or_dispatch(
  req_t req, model::ntp ntp, model::timeout_clock::duration timeout) {
    auto retries = _retries;
    auto delay_ms = _retry_delay_ms;
    std::optional<std::string> error;

    auto holder = ss::gate::holder(_gate);
    auto r = handler_t::error_resp(errc::not_leader);
    while (!_as.abort_requested() && 0 < retries--) {
        auto leader_opt = _leaders.local().get_leader(ntp);
        if (unlikely(!leader_opt)) {
            error = vformat(
              fmt::runtime("can't find {} in the leaders cache"), ntp);
            vlog(
              clusterlog.trace,
              "waiting for {} to fill leaders cache, retries left: {}",
              ntp,
              retries);
            co_await sleep_abortable(delay_ms, _as);
            continue;
        }
        auto leader = leader_opt.value();

        if (leader == _self) {
            r = co_await find_shard_and_process(req, ntp, timeout);
        } else {
            vlog(
              clusterlog.trace,
              "dispatching request {} for ntp {} from {} to {}",
              handler_t::process_name(),
              ntp,
              _self,
              leader);
            r = co_await dispatch_to_leader(req, leader, timeout);
        }

        if (likely(r.ec == errc::success)) {
            error = std::nullopt;
            break;
        }

        if (likely(r.ec != errc::replication_error)) {
            error = vformat(
              fmt::runtime("{} failed with {}"),
              handler_t::process_name(),
              r.ec);
            break;
        }

        error = vformat(
          fmt::runtime("{} failed with {}"), handler_t::process_name(), r.ec);
        vlog(
          clusterlog.trace,
          "request {} for ntp {} failed, error: {} retries left: {}, delay ms: "
          "{}",
          handler_t::process_name(),
          ntp,
          error,
          retries,
          delay_ms.count());
        co_await sleep_abortable(delay_ms, _as);
    }
    if (error) {
        vlog(
          clusterlog.warn,
          "{} failed: {}",
          handler_t::process_name(),
          error.value());
    }

    co_return r;
}

template<typename req_t, typename resp_t, typename handler_t>
requires IsHandler<req_t, resp_t, handler_t>
ss::future<resp_t> leader_router<req_t, resp_t, handler_t>::dispatch_to_leader(
  req_t req, model::node_id leader_id, model::timeout_clock::duration timeout) {
    using proto_t = handler_t::proto_t;
    return _connection_cache.local()
      .with_node_client<proto_t>(
        _self,
        ss::this_shard_id(),
        leader_id,
        timeout,
        [req = std::move(req), timeout](proto_t proto) {
            return handler_t::dispatch(
              std::move(proto), std::move(req), timeout);
        })
      .then(&rpc::get_ctx_data<resp_t>)
      .then([](result<resp_t> r) {
          if (r.has_error()) {
              vlog(
                clusterlog.warn,
                "got error {} on remote {}",
                r.error().message(),
                handler_t::process_name());
              return handler_t::error_resp(errc::timeout);
          }
          return r.value();
      });
}

template<typename req_t, typename resp_t, typename handler_t>
requires IsHandler<req_t, resp_t, handler_t>
ss::future<resp_t>
leader_router<req_t, resp_t, handler_t>::find_shard_and_process(
  req_t req, model::ntp ntp, model::timeout_clock::duration) {
    auto holder = ss::gate::holder(_gate);
    auto shard = _shard_table.local().shard_for(ntp);
    if (unlikely(!shard)) {
        auto retries = _retries;
        auto delay_ms = _retry_delay_ms;
        while (!_as.abort_requested() && !shard && 0 < retries--) {
            try {
                co_await sleep_abortable(delay_ms, _as);
            } catch (const ss::sleep_aborted&) {
                break;
            }
            shard = _shard_table.local().shard_for(ntp);
        }

        if (!shard) {
            vlog(clusterlog.warn, "can't find {} in the shard table", ntp);
            co_return handler_t::error_resp(errc::no_leader_controller);
        }
        _as.check();
    }
    co_return co_await _handler.process(*shard, std::move(req));
}

} // namespace cluster
