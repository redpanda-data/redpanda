/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "container/fragmented_vector.h"
#include "kafka/client/assignment_plans.h"
#include "kafka/client/brokers.h"
#include "kafka/client/configuration.h"
#include "kafka/client/fetch_session.h"
#include "kafka/client/logger.h"
#include "kafka/client/topic_cache.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_fetch.h"

#include <seastar/core/shared_ptr.hh>

#include <absl/container/node_hash_map.h>
#include <absl/hash/hash.h>

#include <chrono>
#include <iterator>

namespace kafka::client {

// consumer manages the lifetime of a consumer within a group.
class consumer final : public ss::enable_lw_shared_from_this<consumer> {
    using assignment_t = assignment;
    using broker_reqs_t = absl::node_hash_map<shared_broker_t, fetch_request>;

public:
    /// \brief Construct a consumer
    ///
    /// \param coordinator - The coordinator broker for this group. There should
    /// be no other owners, as it is used for the long-poll fetch.
    ///
    /// \param name - If this is unknowm_member_id, then the name is generated
    /// by the broker.
    ///
    /// \param on_stopped - Called when a consumer is destroyed.
    /// The consumer may become inactive of its own accord through a timeout.
    /// This callback can be used as a notification system for cleanup.
    consumer(
      const configuration& config,
      topic_cache& topic_cache,
      brokers& brokers,
      shared_broker_t coordinator,
      group_id group_id,
      member_id name,
      ss::noncopyable_function<void(const member_id&)> on_stopped,
      ss::noncopyable_function<ss::future<>(std::exception_ptr)> mitigater);

    const kafka::group_id& group_id() const { return _group_id; }
    const kafka::member_id& member_id() const { return _member_id; }
    const kafka::member_id& name() const {
        return _name != kafka::no_member ? _name : _member_id;
    }
    const chunked_vector<model::topic>& topics() const { return _topics; }
    const assignment_t& assignment() const { return _assignment; }

    ss::future<> initialize();
    ss::future<leave_group_response> leave();
    ss::future<> subscribe(chunked_vector<model::topic> topics);
    ss::future<offset_fetch_response>
    offset_fetch(std::vector<offset_fetch_request_topic> topics);
    ss::future<offset_commit_response>
    offset_commit(std::vector<offset_commit_request_topic> topics);
    ss::future<fetch_response>
    fetch(std::chrono::milliseconds timeout, std::optional<int32_t> max_bytes);

private:
    bool is_leader() const {
        return _member_id != no_member && _leader_id == _member_id;
    }

    void start();
    ss::future<> stop();

    void on_leader_join(const join_group_response& res);

    ss::future<> join();
    ss::future<> sync();

    ss::future<chunked_vector<metadata_response::topic>>
    get_subscribed_topic_metadata();

    ss::future<> heartbeat();
    void refresh_inactivity_timer();

    ss::future<describe_groups_response> describe_group();

    ss::future<fetch_response> dispatch_fetch(broker_reqs_t::value_type br);

    template<typename RequestFactory>
    requires requires(const RequestFactory v) { v.operator()(); }
    ss::future<
      typename std::invoke_result_t<RequestFactory>::api_type::response_type>
    req_res(RequestFactory req) {
        using api_t = typename std::invoke_result_t<RequestFactory>::api_type;
        using response_t = typename api_t::response_type;
        return ss::try_with_gate(_gate, [this, req{std::move(req)}]() mutable {
            auto r = req();
            kclog.debug(
              "Consumer: {}: {} req: {}, coordinator {}",
              *this,
              api_t::name,
              r,
              _coordinator->id());
            return _coordinator->dispatch(std::move(r))
              .then([this, req{std::move(req)}](response_t res) mutable {
                  kclog.debug(
                    "Consumer: {}: {} res: {}, coordinator {}",
                    *this,
                    api_t::name,
                    res,
                    _coordinator->id());
                  return maybe_process_response_errors(
                    std::move(req), std::move(res));
              });
        });
    }

    // The base template for handling response errors
    template<typename request_factory, typename response_t>
    ss::future<response_t>
    maybe_process_response_errors(request_factory req, response_t res);

    // Some template specializations for handling response errors of specific
    // response types
    template<typename request_factory>
    ss::future<metadata_response>
    maybe_process_response_errors(request_factory req, metadata_response res);
    template<typename request_factory>
    ss::future<offset_commit_response> maybe_process_response_errors(
      request_factory req, offset_commit_response res);
    template<typename request_factory>
    ss::future<describe_groups_response> maybe_process_response_errors(
      request_factory req, describe_groups_response res);

    template<typename request_factory>
    ss::future<
      typename std::invoke_result_t<request_factory>::api_type::response_type>
    reset_coordinator_and_retry_request(request_factory req);

    const configuration& _config;
    topic_cache& _topic_cache;
    brokers& _brokers;
    shared_broker_t _coordinator;
    ss::abort_source _as;
    ss::gate _gate{};
    ss::timer<> _heartbeat_timer;
    ss::timer<> _inactive_timer;

    kafka::group_id _group_id;
    generation_id _generation_id{no_generation};
    kafka::member_id _member_id{no_member};
    kafka::member_id _name{no_member};
    kafka::member_id _leader_id{no_leader};
    chunked_vector<model::topic> _topics{};
    chunked_vector<kafka::member_id> _members{};
    chunked_vector<model::topic> _subscribed_topics{};
    std::unique_ptr<assignment_plan> _plan{};
    assignment_t _assignment{};
    absl::node_hash_map<shared_broker_t, fetch_session> _fetch_sessions;
    ss::noncopyable_function<void(const kafka::member_id&)> _on_stopped;
    ss::noncopyable_function<ss::future<>(std::exception_ptr)>
      _external_mitigate;

    friend std::ostream& operator<<(std::ostream& os, const consumer& c) {
        fmt::print(
          os,
          "type={}, member_id={}, name={}",
          c.is_leader() ? "leader" : "member",
          c._member_id,
          c._name);
        return os;
    }
};

using shared_consumer_t = ss::lw_shared_ptr<consumer>;

ss::future<shared_consumer_t> make_consumer(
  const configuration& config,
  topic_cache& topic_cache,
  brokers& brokers,
  shared_broker_t coordinator,
  group_id group_id,
  member_id name,
  ss::noncopyable_function<void(const member_id&)> _on_stopped,
  ss::noncopyable_function<ss::future<>(std::exception_ptr)> mitigater);

namespace detail {

struct consumer_hash {
    using is_transparent = void;
    size_t operator()(const member_id& id) const {
        return absl::Hash<member_id>{}(id);
    }
    size_t operator()(const consumer& c) const { return (*this)(c.name()); }
    size_t operator()(const shared_consumer_t& c) const {
        return (*this)(c->name());
    }
};

struct consumer_eq {
    using is_transparent = void;
    bool operator()(
      const shared_consumer_t& lhs, const shared_consumer_t& rhs) const {
        return lhs->name() == rhs->name();
    }
    bool operator()(const member_id& lhs, const shared_consumer_t& rhs) const {
        return lhs == rhs->name();
    }
    bool operator()(const shared_consumer_t& lhs, const member_id& rhs) const {
        return lhs->name() == rhs;
    }
};

} // namespace detail

} // namespace kafka::client
