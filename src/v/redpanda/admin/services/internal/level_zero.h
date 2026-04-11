/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_zero/gc/level_zero_gc.h"
#include "cluster/fwd.h"
#include "cluster/members_table.h"
#include "proto/redpanda/core/admin/internal/cloud_topics/v1/level_zero.proto.h"
#include "redpanda/admin/proxy/client.h"
#include "redpanda/admin/proxy/context.h"

#include <seastar/core/sharded.hh>

namespace cloud_topics {
class frontend;
}

namespace admin {

namespace level_zero::detail {
/**
 * Concept for requests that can target a specific node.
 */
template<typename T>
concept NodeTargetedRequest = requires(T req, model::node_id id) {
    { req.has_node_id() } -> std::same_as<bool>;
    { req.get_node_id() } -> std::convertible_to<model::node_id::type>;
    req.set_node_id(id);
};
/**
 * Concept for messages that support setting an optional error string.
 */
template<typename T>
concept NodeResult = requires(T res, model::node_id id, ss::sstring err) {
    { res.has_error() } -> std::same_as<bool>;
    { res.get_error() } -> std::convertible_to<ss::sstring>;
    res.set_error(std::move(err));
};
} // namespace level_zero::detail

class level_zero_service_impl
  : public proto::admin::level_zero::level_zero_service {
public:
    level_zero_service_impl(
      model::node_id self,
      admin::proxy::client pc,
      ss::sharded<cloud_topics::level_zero_gc>* gc,
      ss::sharded<cluster::members_table>* mt,
      ss::sharded<cluster::partition_manager>* pm,
      ss::sharded<cluster::partition_leaders_table>* pl,
      ss::sharded<cluster::shard_table>* st);

    seastar::future<proto::admin::level_zero::get_status_response> get_status(
      serde::pb::rpc::context,
      proto::admin::level_zero::get_status_request) override;

    seastar::future<proto::admin::level_zero::start_gc_response> start_gc(
      serde::pb::rpc::context,
      proto::admin::level_zero::start_gc_request) override;

    seastar::future<proto::admin::level_zero::pause_gc_response> pause_gc(
      serde::pb::rpc::context,
      proto::admin::level_zero::pause_gc_request) override;

    seastar::future<proto::admin::level_zero::reset_gc_response> reset_gc(
      serde::pb::rpc::context,
      proto::admin::level_zero::reset_gc_request) override;

    seastar::future<proto::admin::level_zero::advance_epoch_response>
      advance_epoch(
        serde::pb::rpc::context,
        proto::admin::level_zero::advance_epoch_request) override;

    seastar::future<proto::admin::level_zero::get_epoch_info_response>
      get_epoch_info(
        serde::pb::rpc::context,
        proto::admin::level_zero::get_epoch_info_request) override;

    seastar::future<proto::admin::level_zero::get_size_estimate_response>
      get_size_estimate(
        serde::pb::rpc::context,
        proto::admin::level_zero::get_size_estimate_request) override;

private:
    using apply_local = ss::bool_class<struct apply_local_tag>;
    using apply_remote = ss::bool_class<struct apply_remote_tag>;

    /**
     * Throws not_found_exception if the request targets a node that doesn't
     * exist. Otherwise returns a pair of bools indicating whether the request
     * should be applied locally, remotely, or both.
     *
     * Local/remote routing:
     *  - if node field is empty - local & remote (whole cluster)
     *  - if node field is self - apply locally and stop
     *  - if node field is NOT self - dispatch to specified node
     *  - if request has ALREADY been proxied, do not dispatch a second time
     */
    template<level_zero::detail::NodeTargetedRequest Req>
    [[nodiscard]] std::pair<apply_local, apply_remote> validate_request_routing(
      const serde::pb::rpc::context& ctx, const Req& req) {
        if (req.has_node_id()) {
            model::node_id id{req.get_node_id()};
            if (!_members_table->local().nodes().contains(id)) {
                throw serde::pb::rpc::not_found_exception(
                  ssx::sformat("Node ID {} not found", id));
            }
        }

        auto loc = apply_local::no;
        auto rem = apply_remote::no;

        if (!req.has_node_id()) {
            // absence of node ID means "apply to whole cluster"
            loc = apply_local::yes;
            rem = apply_remote::yes;
        } else if (req.get_node_id() == _self) {
            // otherwise apply locally iff node ID is _self
            loc = apply_local::yes;
        } else {
            // otherwise dispatch to remote
            rem = apply_remote::yes;
        }

        // short circuit remote dispatch if the request has already been
        // forwarded once
        if (proxy::is_proxied(ctx)) {
            rem = apply_remote::no;
        }

        return std::make_pair(loc, rem);
    }

    /**
     * @brief Forward some Request to one or more other nodes and collect the
     * results into a vector.
     *
     * If a request fails for some reason (i.e. rpc() throws an exception),
     * sticks a Result w/ an error message into the returned collection and move
     * to the next node.
     *
     * @param req - The request to forward
     * @param rpc - fn(RpcClient, node_id, Request) called for each target node.
     *              Should invoke some rpc on the provided client.
     * @param getter - fn(Response) should produce a collection of Results from
     *                 the Response of some rpc.
     */
    template<
      level_zero::detail::NodeTargetedRequest Request,
      typename Response,
      level_zero::detail::NodeResult Result,
      typename RpcClient>
    [[nodiscard]] ss::future<chunked_vector<Result>> dispatch_and_collect(
      const Request& req,
      std::function<
        ss::future<Response>(RpcClient&, model::node_id, const Request&)> rpc,
      std::function<chunked_vector<Result>(Response)> getter) {
        chunked_vector<Result> results;
        for (const auto& [id, _] : _members_table->local().nodes()) {
            if (id == _self || (req.has_node_id() && req.get_node_id() != id)) {
                continue;
            }
            auto client = _proxy_client.make_client_for_node<RpcClient>(id);
            auto rsp = co_await ss::coroutine::as_future(
              std::invoke(rpc, std::ref(client), id, std::cref(req)));
            if (rsp.failed()) {
                auto& result = results.emplace_back();
                result.set_node_id(id);
                result.set_error(ssx::sformat("{}", rsp.get_exception()));
            } else {
                std::ranges::move(
                  std::invoke(getter, std::move(rsp.get())),
                  std::back_inserter(results));
            }
        }
        co_return results;
    }

    model::node_id _self;
    admin::proxy::client _proxy_client;
    ss::sharded<cloud_topics::level_zero_gc>* _gc;
    ss::sharded<cluster::members_table>* _members_table;
    ss::sharded<cluster::partition_manager>* _partition_manager;
    ss::sharded<cluster::partition_leaders_table>* _partition_leaders;
    ss::sharded<cluster::shard_table>* _shard_table;
};

} // namespace admin
