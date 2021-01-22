// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/consumer.h"

#include "bytes/iobuf_parser.h"
#include "kafka/client/assignment_plans.h"
#include "kafka/client/broker.h"
#include "kafka/client/configuration.h"
#include "kafka/client/error.h"
#include "kafka/client/logger.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/find_coordinator.h"
#include "kafka/protocol/heartbeat.h"
#include "kafka/protocol/join_group.h"
#include "kafka/protocol/leave_group.h"
#include "kafka/protocol/metadata.h"
#include "kafka/protocol/sync_group.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record_utils.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>
#include <exception>

namespace kafka::client {

using namespace std::chrono_literals;

namespace detail {

struct topic_comp {
    bool operator()(const model::topic& lhs, const model::topic& rhs) const {
        return lhs < rhs;
    }
    bool operator()(
      const metadata_response::topic& lhs,
      const metadata_response::topic& rhs) const {
        return lhs.name < rhs.name;
    }
    bool operator()(
      const metadata_response::topic& lhs, const model::topic& rhs) const {
        return lhs.name < rhs;
    }
    bool operator()(
      const model::topic& lhs, const metadata_response::topic& rhs) const {
        return lhs < rhs.name;
    }
};

struct partition_comp {
    bool operator()(
      const metadata_response::partition& lhs,
      const metadata_response::partition& rhs) const {
        return lhs.index < rhs.index;
    }
};

} // namespace detail

void consumer::start() {
    kclog.info("Consumer: {}: start", *this);
    _timer.set_callback([this]() {
        kclog.info("Consumer: {}: timer cb", *this);
        (void)heartbeat().handle_exception_type([this](consumer_error e) {
            kclog.error("Consumer: {}: heartbeat failed: {}", *this, e.error);
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
        join_group_request req{};
        req.client_id = "test_client";
        req.data = {
          .group_id = me->_group_id,
          .session_timeout_ms = cfg.consumer_session_timeout(),
          .rebalance_timeout_ms = cfg.consumer_rebalance_timeout(),
          .member_id = me->_member_id,
          .protocol_type = protocol_type{"consumer"},
          .protocols = make_join_group_request_protocols(me->_topics)};
        return req;
    };
    return req_res(std::move(req_builder))
      .then([this](join_group_response res) {
          switch (res.data.error_code) {
          case error_code::member_id_required:
              _member_id = res.data.member_id;
              return join();
          case error_code::unknown_member_id:
              _member_id = no_member;
              return join();
          case error_code::illegal_generation:
              return join();
          case error_code::none:
              _generation_id = res.data.generation_id;
              _member_id = res.data.member_id;
              _leader_id = res.data.leader;

              _plan = make_assignment_plan(res.data.protocol_name);
              if (!_plan) {
                  return ss::make_exception_future<>(consumer_error(
                    group_id(),
                    member_id(),
                    error_code::inconsistent_group_protocol));
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

void consumer::on_leader_join(const join_group_response& res) {
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
        request_reader r(bytes_to_iobuf(m.metadata));
        auto topics = r.read_array([](request_reader& reader) {
            return model::topic(reader.read_string());
        });
        std::copy(
          topics.begin(), topics.end(), std::back_inserter(_subscribed_topics));
    }
    std::sort(_subscribed_topics.begin(), _subscribed_topics.end());
    _subscribed_topics.erase(
      std::unique(_subscribed_topics.begin(), _subscribed_topics.end()),
      _subscribed_topics.end());

    kclog.info(
      "Consumer: {}: join: members: {}, topics: {}",
      *this,
      _members,
      _subscribed_topics);
}

ss::future<leave_group_response> consumer::leave() {
    auto req_builder = [me{shared_from_this()}] {
        return leave_group_request{
          .data{.group_id = me->_group_id, .member_id = me->_member_id}};
    };
    return req_res(std::move(req_builder)).finally([this]() { return stop(); });
}

ss::future<std::vector<metadata_response::topic>>
consumer::get_subscribed_topic_metadata() {
    return req_res([]() { return metadata_request{.list_all_topics = true}; })
      .then([this](metadata_response res) {
          std::vector<sync_group_request_assignment> assignments;

          std::sort(res.topics.begin(), res.topics.end(), detail::topic_comp{});
          std::vector<metadata_response::topic> topics;
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
    return (is_leader()
              ? get_subscribed_topic_metadata()
              : ss::make_ready_future<std::vector<metadata_response::topic>>())
      .then([this](std::vector<metadata_response::topic> topics) {
          auto req_builder = [me{shared_from_this()},
                              topics{std::move(topics)}]() {
              auto assignments
                = me->is_leader()
                    ? me->_plan->encode(me->_plan->plan(me->_members, topics))
                    : std::vector<sync_group_request_assignment>{};
              return sync_group_request{.data{
                .group_id = me->_group_id,
                .generation_id = me->_generation_id,
                .member_id = me->_member_id,
                .group_instance_id = std::nullopt,
                .assignments = assignments}};
          };

          return req_res(std::move(req_builder))
            .then([this](sync_group_response res) {
                switch (res.data.error_code) {
                case error_code::rebalance_in_progress:
                    return sync();
                case error_code::illegal_generation:
                    return join();
                case error_code::unknown_member_id:
                    _member_id = no_member;
                    return join();
                case error_code::none:
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
        return heartbeat_request{.data{
          .group_id = me->_group_id,
          .generation_id = me->_generation_id,
          .member_id = me->_member_id,
          .group_instance_id = std::nullopt}};
    };
    return req_res(std::move(req_builder)).then([this](heartbeat_response res) {
        switch (res.data.error_code) {
        case error_code::illegal_generation:
            return join();
        case error_code::unknown_member_id:
            _member_id = no_member;
            return join();
        case error_code::rebalance_in_progress:
            return join();
        case error_code::none:
            return ss::now();
        default:
            return ss::make_exception_future<>(
              consumer_error(_group_id, _member_id, res.data.error_code));
        }
    });
}

ss::future<describe_groups_response> consumer::describe_group() {
    auto req_builder = [this]() {
        return describe_groups_request{.data{.groups = {{_group_id}}}};
    };
    return req_res(req_builder);
}

ss::future<offset_fetch_response>
consumer::offset_fetch(std::vector<offset_fetch_request_topic> topics) {
    auto req_builder = [topics{std::move(topics)}, group_id{_group_id}] {
        return offset_fetch_request{
          .data{.group_id = group_id, .topics = topics}};
    };
    return req_res(std::move(req_builder))
      .then([this](offset_fetch_response res) {
          if (res.data.error_code != error_code::none) {
              return ss::make_exception_future<offset_fetch_response>(
                consumer_error(_group_id, _member_id, res.data.error_code));
          }
          return ss::make_ready_future<offset_fetch_response>(std::move(res));
      });
}

ss::future<offset_commit_response>
consumer::offset_commit(std::vector<offset_commit_request_topic> topics) {
    auto req_builder = [me{shared_from_this()}, topics{std::move(topics)}]() {
        return offset_commit_request{.data{
          .group_id = me->_group_id,
          .generation_id = me->_generation_id,
          .member_id = me->_member_id,
          .topics = topics}};
    };

    return req_res(std::move(req_builder));
}

ss::future<fetch_response>
consumer::consume(std::chrono::milliseconds timeout, int32_t max_bytes) {
    using broker_reqs_t = absl::flat_hash_map<shared_broker_t, fetch_request>;

    // Split requests by broker
    broker_reqs_t broker_reqs;
    for (auto const& [t, ps] : _assignment) {
        for (const auto& p : ps) {
            auto broker = co_await _brokers.find(model::topic_partition{t, p});
            auto& session = _fetch_sessions[broker];

            auto& req = broker_reqs
                          .try_emplace(
                            broker,
                            fetch_request{
                              .replica_id = model::node_id{-1}, // consumer
                              .max_wait_time = timeout,
                              .min_bytes = 0,
                              .max_bytes = max_bytes,
                              .isolation_level = 0, // READ_UNCOMMITTED
                              .session_id = session.id,
                              .session_epoch = session.epoch,
                            })
                          .first->second;
            auto offset = session.offsets[t]
                            .try_emplace(p, model::offset{0})
                            .first->second;

            if (req.topics.empty() || req.topics.back().name != t) {
                req.topics.push_back(fetch_request::topic{.name{t}});
            }

            req.topics.back().partitions.push_back(fetch_request::partition{
              .id = p,
              .fetch_offset = offset,
              .partition_max_bytes = max_bytes});
        }
    }

    // TODO Ben:
    // * Size & time limit - round-robin
    co_return co_await ss::map_reduce(
      std::make_move_iterator(broker_reqs.begin()),
      std::make_move_iterator(broker_reqs.end()),
      [this](broker_reqs_t::value_type br) -> ss::future<fetch_response> {
          auto b = br.first;
          auto& req = br.second;
          kclog.info("Consumer: {}, fetch_req: {}", *this, req);
          auto res = co_await b->dispatch(std::move(req));
          kclog.info("Consumer: {}, fetch_res: {}", *this, res);

          if (res.error != error_code::none) {
              co_return res;
          }

          auto& session = _fetch_sessions[b];
          if (session.id == invalid_fetch_session_id) {
              session.id = fetch_session_id{res.session_id};
          }

          if (res.session_id != session.id()) {
              vassert(false, "session mismatch");
              session.id = invalid_fetch_session_id;
              co_return res;
          }
          ++session.epoch;

          for (auto& part : res) {
              if (part.partition_response->has_error()) {
                  continue;
              }
              const auto& topic = part.partition->name;
              const auto p_id = part.partition_response->id;
              auto& record_set = part.partition_response->record_set;
              if (!record_set) {
                  continue;
              }

              session.offsets[topic][p_id] = ++record_set->last_offset();
          }
          co_return res;
      },
      fetch_response{},
      [](fetch_response result, fetch_response val) {
          result.throttle_time += val.throttle_time;
          result.error = val.error;           // TODO Ben: This could be better
          result.session_id = val.session_id; // TODO Ben: vassert
          result.partitions.insert(
            result.partitions.end(),
            std::make_move_iterator(val.partitions.begin()),
            std::make_move_iterator(val.partitions.end()));

          return result;
      });
}

ss::future<shared_consumer_t> make_consumer(
  brokers& brokers, shared_broker_t coordinator, group_id group_id) {
    auto c = ss::make_lw_shared<consumer>(
      brokers, std::move(coordinator), std::move(group_id));
    return c->join().then([c]() mutable { return std::move(c); });
}

} // namespace kafka::client
