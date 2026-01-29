/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "redpanda/admin/services/internal/level_zero_gc.h"

#include "base/vassert.h"
#include "cloud_topics/frontend/frontend.h"
#include "cloud_topics/level_zero/gc/level_zero_gc.h"
#include "cloud_topics/state_accessors.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "serde/protobuf/rpc.h"
#include "ssx/sformat.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/shard_id.hh>

namespace {
ss::logger gclog("level_zero_gc_service");
constexpr model::timeout_clock::duration leader_timeout = 5s;
} // namespace

namespace admin {

namespace {
constexpr proto::admin::level_zero_gc::status
map_gc_state(cloud_topics::level_zero_gc::state st) {
    using namespace proto::admin::level_zero_gc;
    switch (st) {
        using enum cloud_topics::level_zero_gc::state;
    case paused:
        return status::l0_gc_status_paused;
    case running:
        return status::l0_gc_status_running;
    case stopping:
        return status::l0_gc_status_stopping;
    case stopped:
        return status::l0_gc_status_stopped;
    }
    vunreachable("Unrecognized level_zero_gc::state {}", st);
}
} // namespace

level_zero_gc_service_impl::level_zero_gc_service_impl(
  model::node_id self,
  admin::proxy::client pc,
  ss::sharded<cloud_topics::level_zero_gc>* gc,
  ss::sharded<cluster::members_table>* mt,
  ss::sharded<cluster::partition_manager>* pm,
  ss::sharded<cluster::partition_leaders_table>* pl,
  ss::sharded<cluster::shard_table>* st)
  : _self(self)
  , _proxy_client(std::move(pc))
  , _gc(gc)
  , _members_table(mt)
  , _partition_manager(pm)
  , _partition_leaders(pl)
  , _shard_table(st) {}

seastar::future<proto::admin::level_zero_gc::get_status_response>
level_zero_gc_service_impl::get_status(
  serde::pb::rpc::context ctx,
  proto::admin::level_zero_gc::get_status_request req) {
    using namespace proto::admin::level_zero_gc;
    auto [local, remote] = validate_request_routing(ctx, req);

    get_status_response response;
    if (local == apply_local::yes) {
        auto gc_states = co_await _gc->map(
          [](const cloud_topics::level_zero_gc& gc) { return gc.get_state(); });
        if (gc_states.size() != ss::smp::count) {
            throw serde::pb::rpc::internal_exception(
              "Status collection failed");
        }
        auto& node = response.get_nodes().emplace_back();
        node.set_node_id(_self);
        node.get_shards().reserve(gc_states.size());
        std::ranges::transform(
          std::views::zip(
            std::views::iota(0u, static_cast<unsigned>(gc_states.size())),
            gc_states),
          std::back_inserter(node.get_shards()),
          [](auto pr) {
              shard_status shard;
              shard.set_shard_id(std::get<0>(pr));
              shard.set_status(map_gc_state(std::get<1>(pr)));
              return shard;
          });
    }

    if (remote == apply_remote::yes) {
        auto results = co_await dispatch_and_collect<
          get_status_request,
          get_status_response,
          node_status,
          level_zero_gc_service_client>(
          req,
          [ctx](
            level_zero_gc_service_client& cl,
            model::node_id id,
            const get_status_request&) {
              get_status_request proxy_req;
              proxy_req.set_node_id(id);
              return cl.get_status(ctx, std::move(proxy_req));
          },
          [](get_status_response rsp) { return std::move(rsp.get_nodes()); });
        response.get_nodes().reserve(
          response.get_nodes().size() + results.size());
        std::ranges::move(results, std::back_inserter(response.get_nodes()));
    }

    co_return response;
}

seastar::future<proto::admin::level_zero_gc::start_response>
level_zero_gc_service_impl::start(
  serde::pb::rpc::context ctx, proto::admin::level_zero_gc::start_request req) {
    using namespace proto::admin::level_zero_gc;
    auto [local, remote] = validate_request_routing(ctx, req);

    start_response response;
    if (local == apply_local::yes) {
        co_await _gc->invoke_on_all(&cloud_topics::level_zero_gc::start);
        auto& result = response.get_results().emplace_back();
        result.set_node_id(_self);
    }

    if (remote == apply_remote::yes) {
        auto results = co_await dispatch_and_collect<
          start_request,
          start_response,
          start_result,
          level_zero_gc_service_client>(
          req,
          [ctx](
            level_zero_gc_service_client& cl,
            model::node_id id,
            const start_request&) {
              start_request proxy_req;
              proxy_req.set_node_id(id);
              return cl.start(ctx, std::move(proxy_req));
          },
          [](start_response rsp) { return std::move(rsp.get_results()); });
        response.get_results().reserve(
          response.get_results().size() + results.size());
        std::ranges::move(results, std::back_inserter(response.get_results()));
    }

    co_return response;
}

seastar::future<proto::admin::level_zero_gc::pause_response>
level_zero_gc_service_impl::pause(
  serde::pb::rpc::context ctx, proto::admin::level_zero_gc::pause_request req) {
    using namespace proto::admin::level_zero_gc;
    auto [local, remote] = validate_request_routing(ctx, req);

    pause_response response;
    if (local == apply_local::yes) {
        co_await _gc->invoke_on_all(&cloud_topics::level_zero_gc::pause);
        auto& result = response.get_results().emplace_back();
        result.set_node_id(_self);
    }

    if (remote == apply_remote::yes) {
        auto results = co_await dispatch_and_collect<
          pause_request,
          pause_response,
          pause_result,
          level_zero_gc_service_client>(
          req,
          [ctx](
            level_zero_gc_service_client& cl,
            model::node_id id,
            const pause_request&) {
              pause_request proxy_req;
              proxy_req.set_node_id(id);
              return cl.pause(ctx, std::move(proxy_req));
          },
          [](pause_response rsp) { return std::move(rsp.get_results()); });
        response.get_results().reserve(
          response.get_results().size() + results.size());
        std::ranges::move(results, std::back_inserter(response.get_results()));
    }

    co_return response;
}

namespace {

std::expected<std::unique_ptr<cloud_topics::frontend>, ss::sstring>
try_make_ct_frontend(cluster::partition_manager& pm, const model::ntp& ntp) {
    auto partition = pm.get(ntp);
    if (partition == nullptr) {
        return std::unexpected{ssx::sformat(
          "try_make_ct_frontend: TopicPartition {} not found", ntp.tp)};
    }
    if (!partition->get_ntp_config().cloud_topic_enabled()) {
        return std::unexpected{ssx::sformat(
          "try_make_ct_frontend: TopicPartition {} is not a cloud topic",
          ntp.tp)};
    }
    auto ct_state = partition->get_cloud_topics_state();
    if (ct_state == nullptr || !ct_state->local_is_initialized()) {
        return std::unexpected{ssx::sformat(
          "try_make_ct_frontend: Cloud topics subsystem is not initialized")};
    }
    return std::make_unique<cloud_topics::frontend>(
      partition, ct_state->local().get_data_plane());
}

proto::admin::level_zero_gc::epoch_info
epoch_info_to_pb(cloud_topics::frontend::epoch_info info) {
    proto::admin::level_zero_gc::epoch_info result;
    result.set_estimated_inactive_epoch(info.estimated_inactive_epoch);
    result.set_max_applied_epoch(info.max_applied_epoch);
    result.set_last_reconciled_log_offset(info.last_reconciled_log_offset);
    result.set_current_epoch_window_offset(info.current_epoch_window_offset);
    return result;
}

seastar::future<proto::admin::level_zero_gc::advance_epoch_response>
do_advance_epoch(cluster::partition_manager& pm, const model::ntp& ntp) {
    using namespace proto::admin::level_zero_gc;
    advance_epoch_response response;
    auto fe_res = try_make_ct_frontend(pm, ntp);
    if (!fe_res.has_value()) {
        response.set_error(std::move(fe_res.error()));
        co_return response;
    }

    auto fe = std::move(fe_res).value();
    auto new_epoch = co_await fe->get_current_epoch();
    if (!new_epoch.has_value()) {
        response.set_error(
          ssx::sformat(
            "advance_epoch failed to get cluster epoch: {}",
            new_epoch.error()));
        co_return response;
    }

    try {
        auto info = co_await fe->advance_epoch(
          new_epoch.value(), model::timeout_clock::now() + 10s);
        if (!info.has_value()) {
            response.set_error(
              ssx::sformat("advance_epoch failed: {}", info.error()));
        } else {
            response.set_epoch(epoch_info_to_pb(info.value()));
        }
    } catch (...) {
        response.set_error(
          ssx::sformat(
            "advance_epoch failed with exception: {}",
            std::current_exception()));
    }
    co_return response;
}

} // namespace

seastar::future<proto::admin::level_zero_gc::advance_epoch_response>
level_zero_gc_service_impl::advance_epoch(
  serde::pb::rpc::context ctx,
  proto::admin::level_zero_gc::advance_epoch_request req) {
    using namespace proto::admin::level_zero_gc;
    auto& tp = req.get_partition();
    auto ntp = model::ntp{
      model::kafka_namespace, tp.get_topic(), tp.get_partition()};
    advance_epoch_response response{};
    auto leader_fut = co_await ss::coroutine::as_future(
      _partition_leaders->local().wait_for_leader(
        ntp, model::timeout_clock::now() + leader_timeout, std::nullopt));

    if (leader_fut.failed()) {
        auto e = leader_fut.get_exception();
        response.set_error(ssx::sformat("Not leader: {}", e));
        co_return response;
    }
    auto leader = leader_fut.get();
    if (leader != _self) {
        if (proxy::is_proxied(ctx)) {
            response.set_error("Not leader");
            co_return response;
        }
        auto rsp_fut = co_await ss::coroutine::as_future(
          _proxy_client
            .make_client_for_node<level_zero_gc_service_client>(leader)
            .advance_epoch(ctx, std::move(req)));

        if (rsp_fut.failed()) {
            auto e = rsp_fut.get_exception();
            response.set_error(ssx::sformat("{}", e));
            co_return response;
        }

        auto rsp = std::move(rsp_fut).get();
        if (rsp.has_error() == rsp.has_epoch()) {
            throw serde::pb::rpc::internal_exception(
              "Malformed advance_epoch_response: expected one of epoch info "
              "and error");
        }
        if (rsp.has_error()) {
            response.set_error(std::move(rsp.get_error()));
        } else if (rsp.has_epoch()) {
            response.set_epoch(std::move(rsp.get_epoch()));
        }

        co_return response;
    }

    auto shard = _shard_table->local().shard_for(ntp);
    if (!shard.has_value()) {
        response.set_error(ssx::sformat("Not leader"));
        co_return response;
    }

    co_return co_await _partition_manager->invoke_on(
      shard.value(),
      [ntp](
        cluster::partition_manager& pm) -> ss::future<advance_epoch_response> {
          return do_advance_epoch(pm, ntp);
      });
}

} // namespace admin
