/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/client/assignment_plans.h"
#include "kafka/client/brokers.h"
#include "kafka/client/configuration.h"
#include "kafka/client/fetch_session.h"
#include "kafka/client/logger.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/types.h"

#include <seastar/core/shared_ptr.hh>

#include <absl/container/node_hash_map.h>
#include <absl/hash/hash.h>

#include <chrono>
#include <iterator>

namespace kafka::client {

// consumer manages the lifetime of a consumer within a group.
class consumer final : public ss::enable_lw_shared_from_this<consumer> {
    using assignment_t = client::assignment;
    using broker_reqs_t = absl::node_hash_map<shared_broker_t, fetch_request>;

public:
    consumer(
      const configuration& config,
      brokers& brokers,
      shared_broker_t coordinator,
      group_id group_id)
      : _config(config)
      , _brokers(brokers)
      , _coordinator(std::move(coordinator))
      , _group_id(std::move(group_id))
      , _topics() {}

    const kafka::group_id& group_id() const { return _group_id; }
    const kafka::member_id& member_id() const { return _member_id; }
    const std::vector<model::topic>& topics() const { return _topics; }
    const assignment_t& assignment() const { return _assignment; }

    ss::future<> join();
    ss::future<leave_group_response> leave();
    ss::future<> subscribe(std::vector<model::topic> topics);
    ss::future<offset_fetch_response>
    offset_fetch(std::vector<offset_fetch_request_topic> topics);
    ss::future<offset_commit_response>
    offset_commit(std::vector<offset_commit_request_topic> topics);
    ss::future<fetch_response>
    fetch(std::chrono::milliseconds timeout, int32_t max_bytes);

private:
    bool is_leader() const {
        return _member_id != no_member && _leader_id == _member_id;
    }

    void start();
    ss::future<> stop();

    void on_leader_join(const join_group_response& res);

    ss::future<> sync();

    ss::future<std::vector<metadata_response::topic>>
    get_subscribed_topic_metadata();

    ss::future<> heartbeat();

    ss::future<describe_groups_response> describe_group();

    ss::future<fetch_response> dispatch_fetch(broker_reqs_t::value_type br);

    template<typename RequestFactory>
    ss::future<
      typename std::invoke_result_t<RequestFactory>::api_type::response_type>
    req_res(RequestFactory req) {
        using api_t = typename std::invoke_result_t<RequestFactory>::api_type;
        using response_t = typename api_t::response_type;
        return ss::with_gate(_gate, [this, req{std::move(req)}]() {
            auto r = req();
            kclog.debug("Consumer: {}: {} req: {}", *this, api_t::name, r);
            return _coordinator->dispatch(std::move(r))
              .then([this](response_t res) {
                  kclog.debug(
                    "Consumer: {}: {} res: {}", *this, api_t::name, res);
                  return res;
              });
        });
    }

    const configuration& _config;
    brokers& _brokers;
    shared_broker_t _coordinator;
    ss::abort_source _as;
    ss::gate _gate{};
    ss::timer<> _timer;

    kafka::group_id _group_id;
    generation_id _generation_id{no_generation};
    kafka::member_id _member_id{no_member};
    kafka::member_id _leader_id{no_leader};
    std::vector<model::topic> _topics{};
    std::vector<kafka::member_id> _members{};
    std::vector<model::topic> _subscribed_topics{};
    std::unique_ptr<assignment_plan> _plan{};
    assignment_t _assignment{};
    absl::node_hash_map<shared_broker_t, fetch_session> _fetch_sessions;

    friend std::ostream& operator<<(std::ostream& os, const consumer& c) {
        fmt::print(
          os,
          "type={}, id={}",
          c.is_leader() ? "leader" : "member",
          c._member_id);
        return os;
    }
};

using shared_consumer_t = ss::lw_shared_ptr<consumer>;

ss::future<shared_consumer_t> make_consumer(
  const configuration& config,
  brokers& brokers,
  shared_broker_t coordinator,
  group_id group_id);

namespace detail {

struct consumer_hash {
    using is_transparent = void;
    size_t operator()(const member_id& id) const {
        return absl::Hash<member_id>{}(id);
    }
    size_t operator()(const consumer& c) const {
        return (*this)(c.member_id());
    }
    size_t operator()(const shared_consumer_t& c) const {
        return (*this)(c->member_id());
    }
};

struct consumer_eq {
    using is_transparent = void;
    bool operator()(
      const shared_consumer_t& lhs, const shared_consumer_t& rhs) const {
        return lhs->member_id() == rhs->member_id();
    }
    bool operator()(const member_id& lhs, const shared_consumer_t& rhs) const {
        return lhs == rhs->member_id();
    }
    bool operator()(const shared_consumer_t& lhs, const member_id& rhs) const {
        return lhs->member_id() == rhs;
    }
};

} // namespace detail

} // namespace kafka::client
