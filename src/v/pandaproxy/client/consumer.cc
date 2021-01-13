// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/client/consumer.h"

#include "kafka/errors.h"
#include "kafka/requests/describe_groups_request.h"
#include "kafka/requests/find_coordinator_request.h"
#include "kafka/requests/heartbeat_request.h"
#include "kafka/requests/join_group_request.h"
#include "kafka/requests/leave_group_request.h"
#include "kafka/requests/metadata_request.h"
#include "kafka/requests/sync_group_request.h"
#include "kafka/types.h"
#include "pandaproxy/client/assignment_plans.h"
#include "pandaproxy/client/configuration.h"
#include "pandaproxy/client/error.h"
#include "pandaproxy/client/logger.h"

#include <chrono>
#include <exception>

namespace pandaproxy::client {

using namespace std::chrono_literals;

namespace detail {

struct topic_comp {
    bool operator()(const model::topic& lhs, const model::topic& rhs) const {
        return lhs < rhs;
    }
    bool operator()(
      const kafka::metadata_response::topic& lhs,
      const kafka::metadata_response::topic& rhs) const {
        return lhs.name < rhs.name;
    }
    bool operator()(
      const kafka::metadata_response::topic& lhs,
      const model::topic& rhs) const {
        return lhs.name < rhs;
    }
    bool operator()(
      const model::topic& lhs,
      const kafka::metadata_response::topic& rhs) const {
        return lhs < rhs.name;
    }
};

struct partition_comp {
    bool operator()(
      const kafka::metadata_response::partition& lhs,
      const kafka::metadata_response::partition& rhs) const {
        return lhs.index < rhs.index;
    }
};

} // namespace detail

void consumer::start() {
    ppclog.info("Consumer: {}: start", *this);
    _timer.set_callback([this]() {
        ppclog.info("Consumer: {}: timer cb", *this);
        (void)heartbeat().handle_exception_type([this](consumer_error e) {
            ppclog.error("Consumer: {}: heartbeat failed: {}", *this, e.error);
        });
    });
    _timer.arm_periodic(std::chrono::duration_cast<ss::timer<>::duration>(
      shard_local_cfg().consumer_heartbeat_interval()));
}

ss::future<> consumer::stop() {
    _timer.cancel();
    return _coordinator->stop().then([this]() { return _gate.close(); });
}

ss::future<> consumer::join() {
    _timer.cancel();
    auto req_builder = [me{shared_from_this()}]() {
        const auto& cfg = shard_local_cfg();
        kafka::join_group_request req{};
        req.client_id = "test_client";
        req.data = {
          .group_id = me->_group_id,
          .session_timeout_ms = cfg.consumer_session_timeout(),
          .rebalance_timeout_ms = cfg.consumer_rebalance_timeout(),
          .member_id = me->_member_id,
          .protocol_type = kafka::protocol_type{"consumer"},
          .protocols = make_join_group_request_protocols(me->_topics)};
        return req;
    };
    return req_res(std::move(req_builder))
      .then([this](kafka::join_group_response res) {
          switch (res.data.error_code) {
          case kafka::error_code::member_id_required:
              _member_id = res.data.member_id;
              return join();
          case kafka::error_code::unknown_member_id:
              _member_id = kafka::no_member;
              return join();
          case kafka::error_code::illegal_generation:
              return join();
          case kafka::error_code::none:
              _generation_id = res.data.generation_id;
              _member_id = res.data.member_id;
              _leader_id = res.data.leader;

              _plan = make_assignment_plan(res.data.protocol_name);
              if (!_plan) {
                  return ss::make_exception_future<>(consumer_error(
                    group_id(),
                    member_id(),
                    kafka::error_code::inconsistent_group_protocol));
              }

              if (is_leader()) {
                  on_leader_join(res);
              }

              start();
              return sync();
          default:
              return ss::make_exception_future<>(
                consumer_error(_group_id, _member_id, res.data.error_code));
          }
      });
}

ss::future<> consumer::subscribe(std::vector<model::topic> topics) {
    _topics = std::move(topics);
    return join();
}

void consumer::on_leader_join(const kafka::join_group_response& res) {
    _members.clear();
    _members.reserve(res.data.members.size());
    for (auto const& m : res.data.members) {
        _members.push_back(m.member_id);
    }
    std::sort(_members.begin(), _members.end());
    _members.erase(
      std::unique(_members.begin(), _members.end()), _members.end());

    _subscribed_topics.clear();
    for (auto const& m : res.data.members) {
        kafka::request_reader r(bytes_to_iobuf(m.metadata));
        auto topics = r.read_array([](kafka::request_reader& reader) {
            return model::topic(reader.read_string());
        });
        std::copy(
          topics.begin(), topics.end(), std::back_inserter(_subscribed_topics));
    }
    std::sort(_subscribed_topics.begin(), _subscribed_topics.end());
    _subscribed_topics.erase(
      std::unique(_subscribed_topics.begin(), _subscribed_topics.end()),
      _subscribed_topics.end());

    ppclog.info(
      "Consumer: {}: join: members: {}, topics: {}",
      *this,
      _members,
      _subscribed_topics);
}

ss::future<kafka::leave_group_response> consumer::leave() {
    auto req_builder = [me{shared_from_this()}] {
        return kafka::leave_group_request{
          .data{.group_id = me->_group_id, .member_id = me->_member_id}};
    };
    return req_res(std::move(req_builder)).finally([this]() { return stop(); });
}

ss::future<std::vector<kafka::metadata_response::topic>>
consumer::get_subscribed_topic_metadata() {
    return req_res(
             []() { return kafka::metadata_request{.list_all_topics = true}; })
      .then([this](kafka::metadata_response res) {
          std::vector<kafka::sync_group_request_assignment> assignments;

          std::sort(res.topics.begin(), res.topics.end(), detail::topic_comp{});
          std::vector<kafka::metadata_response::topic> topics;
          topics.reserve(_subscribed_topics.size());
          std::set_intersection(
            res.topics.begin(),
            res.topics.end(),
            _subscribed_topics.begin(),
            _subscribed_topics.end(),
            std::back_inserter(topics),
            detail::topic_comp{});
          for (auto& t : topics) {
              std::sort(
                t.partitions.begin(),
                t.partitions.end(),
                detail::partition_comp{});
          }
          return topics;
      });
}

ss::future<> consumer::sync() {
    return (is_leader() ? get_subscribed_topic_metadata()
                        : ss::make_ready_future<
                          std::vector<kafka::metadata_response::topic>>())
      .then([this](std::vector<kafka::metadata_response::topic> topics) {
          auto req_builder = [me{shared_from_this()},
                              topics{std::move(topics)}]() {
              auto assignments
                = me->is_leader()
                    ? me->_plan->encode(me->_plan->plan(me->_members, topics))
                    : std::vector<kafka::sync_group_request_assignment>{};
              return kafka::sync_group_request{.data{
                .group_id = me->_group_id,
                .generation_id = me->_generation_id,
                .member_id = me->_member_id,
                .group_instance_id = std::nullopt,
                .assignments = assignments}};
          };

          return req_res(std::move(req_builder))
            .then([this](kafka::sync_group_response res) {
                switch (res.data.error_code) {
                case kafka::error_code::rebalance_in_progress:
                    return sync();
                case kafka::error_code::illegal_generation:
                    return join();
                case kafka::error_code::unknown_member_id:
                    _member_id = kafka::no_member;
                    return join();
                case kafka::error_code::none:
                    _assignment = _plan->decode(res.data.assignment);
                    return ss::now();
                default:
                    return ss::make_exception_future<>(consumer_error(
                      _group_id, _member_id, res.data.error_code));
                }
            });
      });
}

ss::future<> consumer::heartbeat() {
    auto req_builder = [me{shared_from_this()}] {
        return kafka::heartbeat_request{.data{
          .group_id = me->_group_id,
          .generation_id = me->_generation_id,
          .member_id = me->_member_id,
          .group_instance_id = std::nullopt}};
    };
    return req_res(std::move(req_builder))
      .then([this](kafka::heartbeat_response res) {
          switch (res.data.error_code) {
          case kafka::error_code::illegal_generation:
              return join();
          case kafka::error_code::unknown_member_id:
              _member_id = kafka::no_member;
              return join();
          case kafka::error_code::rebalance_in_progress:
              return join();
          case kafka::error_code::none:
              return ss::now();
          default:
              return ss::make_exception_future<>(
                consumer_error(_group_id, _member_id, res.data.error_code));
          }
      });
}

ss::future<kafka::describe_groups_response> consumer::describe_group() {
    auto req_builder = [this]() {
        return kafka::describe_groups_request{.data{.groups = {{_group_id}}}};
    };
    return req_res(req_builder);
}

ss::future<kafka::offset_fetch_response>
consumer::offset_fetch(std::vector<kafka::offset_fetch_request_topic> topics) {
    auto req_builder = [topics{std::move(topics)}, group_id{_group_id}] {
        return kafka::offset_fetch_request{
          .data{.group_id = group_id, .topics = topics}};
    };
    return req_res(std::move(req_builder))
      .then([this](kafka::offset_fetch_response res) {
          return res.data.error_code == kafka::error_code::none
                   ? ss::make_ready_future<kafka::offset_fetch_response>(
                     std::move(res))
                   : ss::make_exception_future<kafka::offset_fetch_response>(
                     consumer_error(
                       _group_id, _member_id, res.data.error_code));
      });
}

ss::future<kafka::offset_commit_response> consumer::offset_commit(
  std::vector<kafka::offset_commit_request_topic> topics) {
    auto req_builder = [me{shared_from_this()}, topics{std::move(topics)}]() {
        return kafka::offset_commit_request{.data{
          .group_id = me->_group_id,
          .generation_id = me->_generation_id,
          .member_id = me->_member_id,
          .topics = topics}};
    };

    return req_res(std::move(req_builder));
}

ss::future<shared_consumer_t>
make_consumer(shared_broker_t coordinator, kafka::group_id group_id) {
    auto c = ss::make_lw_shared<consumer>(
      std::move(coordinator), std::move(group_id));
    return c->join().then([c]() mutable { return std::move(c); });
}

} // namespace pandaproxy::client
