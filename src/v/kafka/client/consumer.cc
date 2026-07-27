// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/consumer.h"

#include "base/vassert.h"
#include "kafka/client/assignment_plans.h"
#include "kafka/client/broker.h"
#include "kafka/client/configuration.h"
#include "kafka/client/exceptions.h"
#include "kafka/client/types.h"
#include "kafka/client/utils.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/heartbeat.h"
#include "kafka/protocol/join_group.h"
#include "kafka/protocol/leave_group.h"
#include "kafka/protocol/metadata.h"
#include "kafka/protocol/sync_group.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "ssx/future-util.h"

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/exception.hh>

#include <absl/container/flat_hash_map.h>

#include <algorithm>
#include <chrono>
#include <exception>
#include <iterator>
#include <optional>
#include <ranges>
#include <utility>
#include <vector>

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
      val.data.responses.begin(),
      val.data.responses.end(),
      std::back_inserter(result.data.responses));
    return result;
};

/// \brief The outcome of dispatching a fetch to every assigned broker: the
/// first dispatch-level failure (if any), and the response of every broker
/// that did not fail.
struct fetch_results {
    std::exception_ptr dispatch_failure;
    std::vector<std::pair<shared_broker_t, fetch_response>> responses;
};

fetch_results collect_fetch_results(
  const std::vector<shared_broker_t>& req_brokers,
  std::vector<ss::future<fetch_response>> results) {
    fetch_results out;
    out.responses.reserve(results.size());
    for (auto&& [broker, fut] : std::views::zip(req_brokers, results)) {
        if (fut.failed()) {
            auto ex = fut.get_exception();
            if (!out.dispatch_failure) {
                out.dispatch_failure = std::move(ex);
            }
            continue;
        }
        out.responses.emplace_back(broker, fut.get());
    }
    return out;
}

/// \brief Reseed every offset_out_of_range partition across every broker's
/// response to the log_start_offset the broker reported (pandaproxy only
/// allows auto.offset.reset=earliest, which is exactly the log start).
///
/// \return true if at least one partition was reseeded.
bool reseed_out_of_range(
  absl::node_hash_map<shared_broker_t, fetch_session>& fetch_sessions,
  std::vector<std::pair<shared_broker_t, fetch_response>>& responses) {
    bool reseeded = false;
    for (auto& [broker, res] : responses) {
        for (auto& part : res) {
            if (
              part.partition_response->error_code
              == error_code::offset_out_of_range) {
                model::topic_partition tp{
                  part.partition->topic,
                  part.partition_response->partition_index};
                const auto log_start
                  = part.partition_response->log_start_offset;
                // The broker sets log_start_offset alongside
                // offset_out_of_range (see kafka::do_read_from_ntp), so a
                // negative value here is a broker-contract violation: assert it
                // in debug, and in release skip the reseed rather than set the
                // fetch position to a negative offset and loop.
                dassert(
                  log_start >= model::offset{0},
                  "offset_out_of_range with invalid log_start_offset {} for {}",
                  log_start,
                  tp);
                if (log_start < model::offset{0}) {
                    vlog(
                      kclog.warn,
                      "offset_out_of_range with invalid log_start_offset for "
                      "{}; skipping reseed",
                      tp);
                    continue;
                }
                fetch_sessions[broker].reseed(tp, log_start);
                reseeded = true;
            }
        }
    }
    return reseeded;
}

/// \brief Drop offset_out_of_range partitions from a fetch response.
///
/// They were reseeded (see reseed_out_of_range) and carry no records, so the
/// caller delivers the remaining healthy partitions now and the reseeded ones
/// resume from the corrected offset on the client's next fetch. Stripping them
/// also keeps the pandaproxy serializer -- which rejects any partition error --
/// from turning a recoverable round into an HTTP error.
void strip_out_of_range(fetch_response& res) {
    chunked_vector<fetch_response::partition> topics;
    for (auto& topic : res.data.responses) {
        chunked_vector<fetch_response::partition_response> partitions;
        for (auto& part : topic.partitions) {
            if (part.error_code != error_code::offset_out_of_range) {
                partitions.push_back(std::move(part));
            }
        }
        if (!partitions.empty()) {
            topic.partitions = std::move(partitions);
            topics.push_back(std::move(topic));
        }
    }
    res.data.responses = std::move(topics);
}

/// \brief Whether a fetch response carries any records to deliver.
///
/// empty() is non-destructive, so this does not consume the batch reader the
/// caller later serializes.
bool has_records(fetch_response& res) {
    return std::any_of(res.begin(), res.end(), [](const auto& part) {
        const auto& record_set = part.partition_response->records;
        return record_set && !record_set->empty();
    });
}

} // namespace detail

consumer::consumer(
  consumer_configuration config,
  retries_configuration& retries_cfg,
  topic_cache& topic_cache,
  brokers& brokers,
  shared_broker_t coordinator,
  kafka::group_id group_id,
  kafka::member_id name,
  ss::noncopyable_function<void(const kafka::member_id&)> on_stopped,
  ss::noncopyable_function<ss::future<>(std::exception_ptr)> mitigater,
  prefix_logger& logger)
  : _config(std::move(config))
  , _retries_cfg(retries_cfg)
  , _topic_cache(topic_cache)
  , _brokers(brokers)
  , _coordinator(std::move(coordinator))
  , _inactive_timer([me{shared_from_this()}]() {
      vlog(me->_logger->info, "Consumer: {}: inactive", *me);
      ssx::background = me->leave().discard_result().finally([me]() {});
  })
  , _group_id(std::move(group_id))
  , _name(std::move(name))
  , _topics()
  , _on_stopped(std::move(on_stopped))
  , _external_mitigate(std::move(mitigater))
  , _logger(&logger) {}

void consumer::start() {
    vlog(_logger->info, "Consumer: {}: start", *this);
    _heartbeat_timer.set_callback([me{shared_from_this()}]() {
        vlog(me->_logger->trace, "Consumer: {}: timer cb", *me);
        return ssx::spawn_with_gate(me->_gate, [me]() {
            return me->heartbeat()
              .handle_exception_type([me](const exception_base& e) {
                  vlog(
                    me->_logger->info,
                    "Consumer: {}: heartbeat failed: {}",
                    *me,
                    e.error);
              })
              .handle_exception_type([me](const ss::gate_closed_exception& e) {
                  vlog(
                    me->_logger->trace,
                    "Consumer: {}: heartbeat failed: {}",
                    *me,
                    e);
              })
              .handle_exception([me](const std::exception_ptr& e) {
                  vlog(
                    me->_logger->error,
                    "Consumer: {}: heartbeat failed: {}",
                    *me,
                    e);
              });
        });
    });
    _heartbeat_timer.rearm_periodic(_config.heartbeat_interval);
}

ss::future<> consumer::stop() {
    vlog(_logger->info, "Consumer: {}: stop", *this);
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
    vlog(_logger->info, "Consumer: {}: initialize", *this);
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
          .session_timeout_ms = cfg.session_timeout,
          .rebalance_timeout_ms = cfg.rebalance_timeout,
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
              return ss::sleep_abortable(_retries_cfg.retry_base_backoff, _as)
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
      _logger->info,
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
    _inactive_timer.rearm(ss::timer<>::clock::now() + _config.session_timeout);
}

ss::future<describe_groups_response> consumer::describe_group() {
    auto req_builder = [this]() {
        return describe_groups_request{.data{.groups = {_group_id}}};
    };
    return req_res(req_builder);
}

ss::future<offset_fetch_response>
consumer::offset_fetch(chunked_vector<offset_fetch_request_topic> topics) {
    refresh_inactivity_timer();
    auto req_builder = [topics{std::move(topics)}, group_id{_group_id}]() {
        return offset_fetch_request{
          .data{.group_id = group_id, .topics = topics.copy()}};
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
consumer::offset_commit(chunked_vector<offset_commit_request_topic> topics) {
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
          .topics = make_copy(topics)}};
    };

    co_return co_await req_res(std::move(req_builder));
}

ss::future<fetch_response>
consumer::dispatch_fetch(broker_reqs_t::value_type br) {
    auto& [broker, req] = br;
    vlog(_logger->trace, "Consumer: {}, fetch_req: {}", *this, req);
    auto res_v = co_await broker->dispatch(
      std::move(req), api_version_for(fetch_api::key), _as);
    auto res = std::get<fetch_response>(std::move(res_v));
    vlog(_logger->trace, "Consumer: {}, fetch_res: {}", *this, res);

    if (res.data.error_code != error_code::none) {
        throw broker_error(broker->id(), res.data.error_code);
    }

    // Session state is deliberately not applied here: whether the tracked
    // offsets may advance depends on the responses of ALL brokers, which
    // only consumer::fetch() has in hand.
    co_return res;
}

ss::future<> consumer::seed_positions_from_committed() {
    // Collect the assigned partitions that have no fetch position yet --
    // freshly (re)assigned ones. A partition that already carries an in-RAM
    // position (e.g. a same-instance rebalance) is left untouched: that
    // position is at least as recent as the committed offset, so re-seeding
    // would re-read.
    chunked_vector<offset_fetch_request_topic> req;
    absl::flat_hash_map<model::topic_partition, shared_broker_t>
      broker_by_partition;
    // Each topic is a unique key in _assignment, so one request entry per
    // iteration collects all of that topic's initializing partitions.
    for (const auto& [t, ps] : _assignment) {
        offset_fetch_request_topic topic_req{.name = t};
        for (const auto& p : ps) {
            model::topic_partition tp{t, p};
            auto leader = _topic_cache.leader(tp);
            if (!leader) {
                throw partition_error(
                  tp, error_code::unknown_topic_or_partition);
            }
            auto broker = _brokers.find(*leader);
            if (_fetch_sessions[broker].has_offset(tp)) {
                continue;
            }
            broker_by_partition.emplace(tp, std::move(broker));
            topic_req.partition_indexes.push_back(p);
        }
        if (!topic_req.partition_indexes.empty()) {
            req.push_back(std::move(topic_req));
        }
    }
    if (broker_by_partition.empty()) {
        // Every assigned partition already has a position
        co_return;
    }

    auto res = co_await offset_fetch(std::move(req));
    for (const auto& t : res.data.topics) {
        for (const auto& part : t.partitions) {
            model::topic_partition tp{t.name, part.partition_index};
            // A per-partition error must not be swallowed: skipping it would
            // leave the partition uninitialized, so the fetch below would start
            // it at earliest and silently re-read the whole log instead of
            // resuming. Throw so the retry mitigates (update_metadata for a
            // transient unknown_topic_or_partition, surfaces the rest).
            if (part.error_code != error_code::none) {
                throw partition_error(tp, part.error_code);
            }
            auto it = broker_by_partition.find(tp);
            if (it == broker_by_partition.end()) {
                continue;
            }
            // A committed offset of -1 means the group has none: seed earliest
            // (log start, the only reset policy pandaproxy allows). Seeding a
            // concrete value also marks the partition initialized, so committed
            // is not re-fetched on every poll. Otherwise the fetch position is
            // committed+1: our commit stores the last consumed offset
            // (commit-all stores position-1; an explicit REST commit stores the
            // client's value, so posting X resumes at X+1). If committed+1 is
            // below the log start after retention, the fetch returns
            // offset_out_of_range and the reseed to log_start recovers it.
            const auto pos = part.committed_offset < model::offset{0}
                               ? model::offset{0}
                               : part.committed_offset + model::offset{1};
            vlog(
              _logger->debug,
              "Consumer: {}: seeding {} from committed offset {} -> position "
              "{}",
              *this,
              tp,
              part.committed_offset,
              pos);
            _fetch_sessions[it->second].reseed(tp, pos);
        }
    }
}

consumer::broker_reqs_t consumer::build_fetch_requests(
  std::chrono::milliseconds timeout, std::optional<int32_t> max_bytes) {
    broker_reqs_t broker_reqs;
    for (const auto& [t, ps] : _assignment) {
        for (const auto& p : ps) {
            auto tp = model::topic_partition{t, p};
            auto leader = _topic_cache.leader(tp);
            if (!leader) {
                throw partition_error(
                  tp, error_code::unknown_topic_or_partition);
            }
            auto broker = _brokers.find(*leader);
            auto& session = _fetch_sessions[broker];

            auto& req = broker_reqs
                          .try_emplace(
                            broker,
                            fetch_request{
                              .data = {
                              .replica_id = consumer_replica_id,
                              .max_wait_ms = timeout,
                              .min_bytes = _config.fetch_min_bytes,
                              .max_bytes = max_bytes.value_or(
                                _config.fetch_max_bytes),
                              .isolation_level = model::isolation_level::
                                read_uncommitted, // READ_UNCOMMITTED
                              .session_id = session.id(),
                              .session_epoch = session.epoch(),
                            }})
                          .first->second;

            if (req.data.topics.empty() || req.data.topics.back().topic != t) {
                req.data.topics.push_back(fetch_request::topic{.topic{t}});
            }

            req.data.topics.back().partitions.push_back(
              fetch_request::partition{
                .partition = p,
                .fetch_offset = session.offset(tp),
                .partition_max_bytes = max_bytes.value_or(
                  _config.fetch_max_bytes)});
        }
    }
    return broker_reqs;
}

ss::future<std::pair<fetch_response, bool>> consumer::fetch_round(
  std::chrono::milliseconds timeout, std::optional<int32_t> max_bytes) {
    auto broker_reqs = build_fetch_requests(timeout, max_bytes);

    // Dispatch to all brokers concurrently, then wait for every result
    // before touching any session state.
    std::vector<shared_broker_t> req_brokers;
    std::vector<ss::future<fetch_response>> dispatched;
    req_brokers.reserve(broker_reqs.size());
    dispatched.reserve(broker_reqs.size());
    for (auto& br : broker_reqs) {
        req_brokers.push_back(br.first);
        dispatched.push_back(dispatch_fetch(std::move(br)));
    }
    auto results = co_await ss::when_all(dispatched.begin(), dispatched.end());

    auto [dispatch_failure, responses] = detail::collect_fetch_results(
      req_brokers, std::move(results));

    // Out-of-range partitions are reseeded to the broker-reported
    // log_start_offset (pandaproxy only allows auto.offset.reset=earliest,
    // i.e. the log start). They carry no records and are stripped below.
    const bool reseeded = detail::reseed_out_of_range(
      _fetch_sessions, responses);

    // A dispatch failure means a whole broker's response never arrived and the
    // topology may be stale, so discard the round and throw: the retry in
    // client::consumer_fetch() re-fetches and refreshes metadata. No offset
    // may advance here -- those records were never delivered, and advancing
    // would make the retry skip them (silent data loss). discard() still
    // advances each session's epoch to stay in step with the broker.
    if (dispatch_failure) {
        for (auto& [broker, res] : responses) {
            _fetch_sessions[broker].discard(res);
        }
        co_await ss::coroutine::return_exception_ptr(
          std::move(dispatch_failure));
    }

    // Deliver the healthy partitions: apply() advances the offsets of
    // partitions that returned records and skips the out-of-range ones, which
    // were reseeded above and are stripped below.
    for (auto& [broker, res] : responses) {
        _fetch_sessions[broker].apply(res);
    }

    fetch_response result{
      .data = {
        .throttle_time_ms{},
        .error_code = error_code::none,
        .session_id = kafka::invalid_fetch_session_id}};
    for (auto& [broker, res] : responses) {
        result = detail::reduce_fetch_response(
          std::move(result), std::move(res));
    }
    detail::strip_out_of_range(result);
    co_return std::pair{std::move(result), reseeded};
}

ss::future<fetch_response> consumer::fetch(
  std::chrono::milliseconds timeout, std::optional<int32_t> max_bytes) {
    refresh_inactivity_timer();

    // Before fetching, seed any freshly (re)assigned partition from the group's
    // committed offset so a fresh consumer instance resumes where the group
    // left off, mirroring the Java consumer's updateFetchPositions at the start
    // of poll(). Cheap when nothing is initializing (no OffsetFetch is issued).
    co_await seed_positions_from_committed();

    // An out-of-range partition is reseeded to the log start and stripped from
    // the response (the pandaproxy serializer rejects any partition error), so
    // a round whose only outcome was the reseed carries no records. Rather than
    // return that empty round -- forcing the client to poll again for data that
    // already exists at the reseeded offset -- repeat the round within the
    // caller's timeout budget, exactly what the client's next poll would do.
    // This mirrors the timer-bounded do/while in the Java consumer's poll()
    // (Confluent REST proxy): a round that delivered records returns at once,
    // and if the budget is spent mid-recovery the empty round is returned and
    // the reseeded partition resumes on the next poll.
    // Always run at least one round: a zero or already-elapsed budget -- a
    // non-blocking timeout=0 poll, or a retry after the budget was spent --
    // must still issue one (non-blocking, max_wait_ms=0) fetch rather than give
    // up with an empty response. The deadline is therefore checked after the
    // round, not before.
    const auto deadline = ss::lowres_clock::now() + timeout;
    for (;;) {
        const auto remaining = std::max(
          deadline - ss::lowres_clock::now(),
          ss::lowres_clock::duration::zero());
        auto [result, reseeded] = co_await fetch_round(
          std::chrono::duration_cast<std::chrono::milliseconds>(remaining),
          max_bytes);

        // Deliver as soon as there is data or when nothing was reseeded (a
        // genuine empty long-poll). Otherwise repeat to resolve a round that
        // reseeded but delivered nothing, until the timeout budget is spent.
        if (
          detail::has_records(result) || !reseeded
          || ss::lowres_clock::now() >= deadline) {
            co_return std::move(result);
        }
    }
}

template<typename request_factory>
ss::future<
  typename std::invoke_result_t<request_factory>::api_type::response_type>
consumer::reset_coordinator_and_retry_request(request_factory req) {
    return find_coordinator_with_retry_and_mitigation(
             _gate,
             _retries_cfg.max_retries,
             _retries_cfg.retry_base_backoff,
             _brokers,
             group_id(),
             name(),
             [this](std::exception_ptr ex) { return _external_mitigate(ex); })
      .then(
        [this, req{std::move(req)}](shared_broker_t new_coordinator) mutable {
            auto old_coordinator = std::move(_coordinator);
            _coordinator = std::move(new_coordinator);
            auto f = ss::now();
            if (old_coordinator) {
                f = old_coordinator->stop().finally([old_coordinator] {});
            }

            // Calling req_res here will re-issue the request on the
            // new coordinator
            return f.then([this, req = std::move(req)]() mutable {
                return req_res(std::move(req));
            });
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
          _logger->debug,
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
              _logger->debug,
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
                  _logger->debug,
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
              _logger->debug,
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
  const consumer_configuration& config,
  retries_configuration& retries_config,
  topic_cache& topic_cache,
  brokers& brokers,
  shared_broker_t coordinator,
  group_id group_id,
  member_id name,
  ss::noncopyable_function<void(const member_id&)> on_stopped,
  ss::noncopyable_function<ss::future<>(std::exception_ptr)> mitigater,
  prefix_logger& logger) {
    auto c = ss::make_lw_shared<consumer>(
      config,
      retries_config,
      topic_cache,
      brokers,
      std::move(coordinator),
      std::move(group_id),
      std::move(name),
      std::move(on_stopped),
      std::move(mitigater),
      logger);
    return c->initialize().then([c]() mutable { return std::move(c); });
}

fmt::iterator consumer::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "type={}, member_id={}, name={}",
      is_leader() ? "leader" : "member",
      _member_id,
      _name);
}

} // namespace kafka::client
