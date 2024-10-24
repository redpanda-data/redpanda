// Copyright 2020 Redpanda Data, Inc.
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
#include "kafka/client/exceptions.h"
#include "kafka/client/logger.h"
#include "kafka/client/utils.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/find_coordinator.h"
#include "kafka/protocol/heartbeat.h"
#include "kafka/protocol/join_group.h"
#include "kafka/protocol/leave_group.h"
#include "kafka/protocol/metadata.h"
#include "kafka/protocol/sync_group.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record_utils.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sleep.hh>

#include <chrono>
#include <exception>
#include <iterator>
#include <utility>

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
        return lhs.partition_index < rhs.partition_index;
    }
};

fetch_response
reduce_fetch_response(fetch_response result, fetch_response val) {
    result.data.throttle_time_ms += val.data.throttle_time_ms;
    std::move(
      val.data.topics.begin(),
      val.data.topics.end(),
      std::back_inserter(result.data.topics));
    return result;
};

} // namespace detail

consumer::consumer(
  const configuration& config,
  topic_cache& topic_cache,
  brokers& brokers,
  shared_broker_t coordinator,
  kafka::group_id group_id,
  kafka::member_id name,
  ss::noncopyable_function<void(const kafka::member_id&)> on_stopped,
  ss::noncopyable_function<ss::future<>(std::exception_ptr)> mitigater)
  : _config(config)
  , _topic_cache(topic_cache)
  , _brokers(brokers)
  , _coordinator(std::move(coordinator))
  , _inactive_timer([me{shared_from_this()}]() {
      vlog(kclog.info, "Consumer: {}: inactive", *me);
      ssx::background = me->leave().discard_result().finally([me]() {});
  })
  , _group_id(std::move(group_id))
  , _name(std::move(name))
  , _topics()
  , _on_stopped(std::move(on_stopped))
  , _external_mitigate(std::move(mitigater)) {}

void consumer::start() {
    vlog(kclog.info, "Consumer: {}: start", *this);
    _heartbeat_timer.set_callback([me{shared_from_this()}]() {
        vlog(kclog.trace, "Consumer: {}: timer cb", *me);
        (void)me->heartbeat()
          .handle_exception_type([me](const exception_base& e) {
              vlog(
                kclog.info, "Consumer: {}: heartbeat failed: {}", *me, e.error);
          })
          .handle_exception_type([me](const ss::gate_closed_exception& e) {
              vlog(kclog.trace, "Consumer: {}: heartbeat failed: {}", *me, e);
          })
          .handle_exception([me](const std::exception_ptr& e) {
              vlog(kclog.error, "Consumer: {}: heartbeat failed: {}", *me, e);
          });
    });
    _heartbeat_timer.rearm_periodic(_config.consumer_heartbeat_interval());
}

ss::future<> consumer::stop() {
    vlog(kclog.info, "Consumer: {}: stop", *this);
    // Clear the timer callbacks as they may hold a shared_from_this().
    _heartbeat_timer.cancel();
    _heartbeat_timer.set_callback([]() {});
    _inactive_timer.cancel();
    _inactive_timer.set_callback([]() {});

    _on_stopped(name());
    if (_as.abort_requested()) {
        return ss::now();
    }
    _as.request_abort();
    return _coordinator->stop()
      .then([this]() { return _gate.close(); })
      .finally([me{shared_from_this()}] {});
}

ss::future<> consumer::initialize() {
    vlog(kclog.info, "Consumer: {}: initialize", *this);
    refresh_inactivity_timer();
    return join();
}

ss::future<> consumer::join() {
    _heartbeat_timer.cancel();
    auto req_builder = [me{shared_from_this()}]() {
        const auto& cfg = me->_config;
        join_group_request req{};
        req.client_id = kafka::client_id("test_client");
        req.data = {
          .group_id = me->_group_id,
          .session_timeout_ms = cfg.consumer_session_timeout(),
          .rebalance_timeout_ms = cfg.consumer_rebalance_timeout(),
          .member_id = me->_member_id,
          .protocol_type = consumer_group_protocol_type,
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
          case error_code::not_coordinator:
              return ss::sleep_abortable(_config.retry_base_backoff(), _as)
                .then([this]() { return join(); });
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

ss::future<> consumer::subscribe(chunked_vector<model::topic> topics) {
    refresh_inactivity_timer();
    _topics = std::move(topics);
    return join();
}

void consumer::on_leader_join(const join_group_response& res) {
    _members.clear();
    _members.reserve(res.data.members.size());
    for (const auto& m : res.data.members) {
        _members.push_back(m.member_id);
    }
    std::sort(_members.begin(), _members.end());
    _members.erase_to_end(std::unique(_members.begin(), _members.end()));

    _subscribed_topics.clear();
    for (const auto& m : res.data.members) {
        protocol::decoder r(bytes_to_iobuf(m.metadata));
        auto topics = r.read_array([](protocol::decoder& reader) {
            return model::topic(reader.read_string());
        });
        std::copy(
          topics.begin(), topics.end(), std::back_inserter(_subscribed_topics));
    }
    std::sort(_subscribed_topics.begin(), _subscribed_topics.end());
    _subscribed_topics.erase_to_end(
      std::unique(_subscribed_topics.begin(), _subscribed_topics.end()));

    vlog(
      kclog.info,
      "Consumer: {}: join: members: {}, topics: {}",
      *this,
      _members,
      _subscribed_topics);
}

ss::future<leave_group_response> consumer::leave() {
    auto req_builder = [this] {
        return leave_group_request{
          .data{.group_id = _group_id, .member_id = _member_id}};
    };
    return req_res(std::move(req_builder)).finally([me{shared_from_this()}]() {
        return me->stop();
    });
}

ss::future<chunked_vector<metadata_response::topic>>
consumer::get_subscribed_topic_metadata() {
    return req_res([]() { return metadata_request{.list_all_topics = true}; })
      .then([this](metadata_response res) {
          std::sort(
            res.data.topics.begin(),
            res.data.topics.end(),
            detail::topic_comp{});
          chunked_vector<metadata_response::topic> topics;
          topics.reserve(_subscribed_topics.size());
          std::set_intersection(
            std::make_move_iterator(res.data.topics.begin()),
            std::make_move_iterator(res.data.topics.end()),
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
                            chunked_vector<metadata_response::topic>>())
      .then([this](chunked_vector<metadata_response::topic> topics) {
          auto req_builder = [me{shared_from_this()},
                              topics{std::move(topics)}]() {
              auto assignments
                = me->is_leader()
                    ? me->_plan->encode(me->_plan->plan(me->_members, topics))
                    : chunked_vector<sync_group_request_assignment>{};
              return sync_group_request{.data{
                .group_id = me->_group_id,
                .generation_id = me->_generation_id,
                .member_id = me->_member_id,
                .group_instance_id = std::nullopt,
                .assignments = std::move(assignments)}};
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

void consumer::refresh_inactivity_timer() {
    _inactive_timer.rearm(
      ss::timer<>::clock::now() + _config.consumer_session_timeout());
}

ss::future<describe_groups_response> consumer::describe_group() {
    auto req_builder = [this]() {
        return describe_groups_request{.data{.groups = {{_group_id}}}};
    };
    return req_res(req_builder);
}

ss::future<offset_fetch_response>
consumer::offset_fetch(std::vector<offset_fetch_request_topic> topics) {
    refresh_inactivity_timer();
    auto req_builder = [topics{std::move(topics)}, group_id{_group_id}]() {
        return offset_fetch_request{
          .data{.group_id = group_id, .topics = topics}};
    };
    return req_res(std::move(req_builder))
      .then([this](offset_fetch_response res) {
          return res.data.error_code == error_code::none
                   ? ss::make_ready_future<offset_fetch_response>(
                       std::move(res))
                   : ss::make_exception_future<offset_fetch_response>(
                       consumer_error(
                         _group_id, _member_id, res.data.error_code));
      });
}

ss::future<offset_commit_response>
consumer::offset_commit(std::vector<offset_commit_request_topic> topics) {
    refresh_inactivity_timer();
    if (topics.empty()) { // commit all offsets
        for (const auto& s : _fetch_sessions) {
            auto res = s.second.make_offset_commit_request();
            std::move(res.begin(), res.end(), std::back_inserter(topics));
        }
    } else { // set epoch for requests tps
        for (auto& t : topics) {
            for (auto& p : t.partitions) {
                p.committed_leader_epoch = kafka::invalid_leader_epoch;
            }
        }
    }
    auto req_builder = [me{shared_from_this()}, topics{std::move(topics)}]() {
        return offset_commit_request{.data{
          .group_id = me->_group_id,
          .generation_id = me->_generation_id,
          .member_id = me->_member_id,
          .topics = topics}};
    };

    co_return co_await req_res(std::move(req_builder));
}

ss::future<fetch_response>
consumer::dispatch_fetch(broker_reqs_t::value_type br) {
    auto& [broker, req] = br;
    vlog(kclog.trace, "Consumer: {}, fetch_req: {}", *this, req);
    auto res = co_await broker->dispatch(std::move(req));
    vlog(kclog.trace, "Consumer: {}, fetch_res: {}", *this, res);

    if (res.data.error_code != error_code::none) {
        throw broker_error(broker->id(), res.data.error_code);
    }

    _fetch_sessions[broker].apply(res);
    co_return res;
}

ss::future<fetch_response> consumer::fetch(
  std::chrono::milliseconds timeout, std::optional<int32_t> max_bytes) {
    refresh_inactivity_timer();
    // Split requests by broker
    broker_reqs_t broker_reqs;
    for (const auto& [t, ps] : _assignment) {
        for (const auto& p : ps) {
            auto tp = model::topic_partition{t, p};
            auto leader = co_await _topic_cache.leader(tp);
            auto broker = co_await _brokers.find(leader);
            auto& session = _fetch_sessions[broker];

            auto& req = broker_reqs
                          .try_emplace(
                            broker,
                            fetch_request{
                              .data = {
                              .replica_id = consumer_replica_id,
                              .max_wait_ms = timeout,
                              .min_bytes = _config.consumer_request_min_bytes,
                              .max_bytes = max_bytes.value_or(
                                _config.consumer_request_max_bytes),
                              .isolation_level = model::isolation_level::
                                read_uncommitted, // READ_UNCOMMITTED
                              .session_id = session.id(),
                              .session_epoch = session.epoch(),
                            }})
                          .first->second;

            if (req.data.topics.empty() || req.data.topics.back().name != t) {
                req.data.topics.push_back(fetch_request::topic{.name{t}});
            }

            req.data.topics.back().fetch_partitions.push_back(
              fetch_request::partition{
                .partition_index = p,
                .fetch_offset = session.offset(tp),
                .max_bytes = max_bytes.value_or(
                  _config.consumer_request_max_bytes)});
        }
    }

    co_return co_await ss::map_reduce(
      std::make_move_iterator(broker_reqs.begin()),
      std::make_move_iterator(broker_reqs.end()),
      [this](broker_reqs_t::value_type br) {
          return dispatch_fetch(std::move(br));
      },
      fetch_response{
        .data
        = {.throttle_time_ms{}, .error_code = error_code::none, .session_id = kafka::invalid_fetch_session_id}},
      detail::reduce_fetch_response);
}

template<typename request_factory>
ss::future<
  typename std::invoke_result_t<request_factory>::api_type::response_type>
consumer::reset_coordinator_and_retry_request(request_factory req) {
    return find_coordinator_with_retry_and_mitigation(
             _gate,
             _config,
             _brokers,
             group_id(),
             name(),
             [this](std::exception_ptr ex) { return _external_mitigate(ex); })
      .then(
        [this, req{std::move(req)}](shared_broker_t new_coordinator) mutable {
            _coordinator = new_coordinator;
            // Calling req_res here will re-issue the request on the
            // new coordinator
            return req_res(std::move(req));
        });
}

template<typename request_factory, typename response_t>
ss::future<response_t>
consumer::maybe_process_response_errors(request_factory req, response_t res) {
    auto me = shared_from_this();
    // By default, look at the top-level for errors
    switch (res.data.error_code) {
    case error_code::not_coordinator:
        vlog(
          kclog.debug,
          "Wrong coordinator on consumer {}, getting new coordinator "
          "before retry",
          *me);
        return reset_coordinator_and_retry_request(std::move(req));
    default:
        // Return the whole response otherwise
        return ss::make_ready_future<response_t>(std::move(res));
    }
}

template<typename request_factory>
ss::future<metadata_response> consumer::maybe_process_response_errors(
  request_factory req, metadata_response res) {
    auto me = shared_from_this();
    for (auto& topic : res.data.topics) {
        switch (topic.error_code) {
        case error_code::not_coordinator:
            vlog(
              kclog.debug,
              "Wrong coordinator on consumer {}, topic {}, getting new "
              "coordinator before retry",
              *me,
              topic.name);
            return reset_coordinator_and_retry_request(std::move(req));
        default:
            continue;
        }
    }
    // Return the whole response otherwise
    return ss::make_ready_future<metadata_response>(std::move(res));
}

template<typename request_factory>
ss::future<offset_commit_response> consumer::maybe_process_response_errors(
  request_factory req, offset_commit_response res) {
    auto me = shared_from_this();
    for (auto& topic : res.data.topics) {
        for (auto& partition : topic.partitions) {
            switch (partition.error_code) {
            case error_code::not_coordinator:
                vlog(
                  kclog.debug,
                  "Wrong coordinator on consumer {}, tp {}, getting new "
                  "coordinator before retry",
                  *me,
                  model::topic_partition{
                    topic.name,
                    model::partition_id{partition.partition_index}});
                return reset_coordinator_and_retry_request(std::move(req));
            default:
                continue;
            }
        }
    }
    // Return the whole response otherwise
    return ss::make_ready_future<offset_commit_response>(std::move(res));
}

template<typename request_factory>
ss::future<describe_groups_response> consumer::maybe_process_response_errors(
  request_factory req, describe_groups_response res) {
    auto me = shared_from_this();
    for (auto& group : res.data.groups) {
        switch (group.error_code) {
        case error_code::not_coordinator:
            vlog(
              kclog.debug,
              "Wrong coordinator on consumer {}, group {}, getting new "
              "coordinator before retry",
              *me,
              group);
            return reset_coordinator_and_retry_request(std::move(req));
        default:
            continue;
        }
    }
    // Return the whole response otherwise
    return ss::make_ready_future<describe_groups_response>(std::move(res));
}

ss::future<shared_consumer_t> make_consumer(
  const configuration& config,
  topic_cache& topic_cache,
  brokers& brokers,
  shared_broker_t coordinator,
  group_id group_id,
  member_id name,
  ss::noncopyable_function<void(const member_id&)> on_stopped,
  ss::noncopyable_function<ss::future<>(std::exception_ptr)> mitigater) {
    auto c = ss::make_lw_shared<consumer>(
      config,
      topic_cache,
      brokers,
      std::move(coordinator),
      std::move(group_id),
      std::move(name),
      std::move(on_stopped),
      std::move(mitigater));
    return c->initialize().then([c]() mutable { return std::move(c); });
}

} // namespace kafka::client
