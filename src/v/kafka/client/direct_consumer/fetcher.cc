/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/client/direct_consumer/fetcher.h"

#include "absl/container/flat_hash_set.h"
#include "base/format_to.h"
#include "kafka/client/direct_consumer/data_queue.h"
#include "kafka/client/direct_consumer/direct_consumer.h"
#include "kafka/client/errors.h"
#include "kafka/protocol/types.h"
#include "ssx/async_algorithm.h"
#include "ssx/future-util.h"

#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>

#include <fmt/format.h>

namespace kafka::client {
static constexpr model::node_id client_replica_id{-1};
static constexpr std::chrono::milliseconds error_backoff(200);

fetch_session_state::fetch_session_state(
  model::node_id id,
  prefix_logger& logger,
  fetch_sessions_enabled sessions_enabled)
  : _id(id)
  , _logger(&logger)
  , _fetch_sessions_enabled(sessions_enabled) {
    reset();
}

void fetch_session_state::update_fetch_session(kafka::fetch_session_id id) {
    auto deferred_log = ss::defer(
      [this, prev_ss = session_state, prev_id = session_id] {
          if (prev_id == session_id && prev_ss == session_state) {
              return;
          }
          vlog(
            _logger->trace,
            "[broker: {}] {{session {}: {}}} -> {{session {}: {}}}",
            _id,
            prev_id,
            prev_ss,
            session_id,
            session_state);
      });
    switch (session_state) {
    case state::none:
        reset();
        return;
    case state::need_full_fetch:
        reset();
        if (id != kafka::invalid_fetch_session_id) {
            session_id = id;
            session_state = state::incremental_fetch;
            advance_epoch();
        }
        return;
    case state::incremental_fetch:
        if (id == kafka::invalid_fetch_session_id) {
            reset();
        } else if (id == session_id) {
            session_state = state::incremental_fetch;
            advance_epoch();
        } else {
            session_state = state::needs_close;
            session_epoch = kafka::final_fetch_session_epoch;
        }
        return;
    case state::needs_close:
        if (id == kafka::invalid_fetch_session_id) {
            reset();
        } else {
            session_state = state::needs_close;
            session_epoch = kafka::final_fetch_session_epoch;
        }
        return;
    }
}

void fetch_session_state::reset() {
    session_id = kafka::invalid_fetch_session_id;
    if (_fetch_sessions_enabled) {
        session_epoch = kafka::initial_fetch_session_epoch;
        session_state = state::need_full_fetch;
    } else {
        session_epoch = kafka::final_fetch_session_epoch;
        session_state = state::none;
    }
}

void fetch_session_state::toggle(fetch_sessions_enabled enable) {
    if (enable == _fetch_sessions_enabled) {
        return;
    }
    if (enable) {
        session_state = state::need_full_fetch;
    } else if (session_id != kafka::invalid_fetch_session_id) {
        session_state = state::needs_close;
    } else {
        session_state = state::none;
    }
    _fetch_sessions_enabled = enable;
    update_fetch_session(session_id);
}

data_queue& fetcher::queue() { return *_parent->_fetched_data_queue; }
prefix_logger& fetcher::logger() { return _parent->_cluster->logger(); }

fetcher::fetcher(
  direct_consumer* parent, model::node_id id, fetch_sessions_enabled sessions)
  : _parent(parent)
  , _id(id)
  , _session_state(_id, logger(), sessions)
  , _state_lock("fetcher/state") {}

void fetcher::start() {
    ssx::repeat_until_gate_closed(_gate, [this] { return do_fetch(); });
}

ss::future<> fetcher::stop() {
    vlog(logger().debug, "[broker: {}] Stopping fetcher", _id);
    _as.request_abort();
    _state_lock.broken();
    _partitions_updated.broken();
    return _gate.close();
}

ss::future<fetcher::partitions_with_epoch> fetcher::collect_partitions() {
    auto lock = co_await _state_lock.get_units();
    fetcher::partitions_with_epoch ret;
    ret.partitions.reserve(_partitions.size());
    ssx::async_counter cnt;
    for (auto& [topic, partitions] : _partitions) {
        partitions_to_process to_process;
        to_process.topic = topic;

        co_await ssx::async_for_each_counter(
          cnt,
          partitions,
          [&to_process, &ret, inc = _session_state.incremental()](auto& p_fs) {
              partition_fetch_state& fetch_state = p_fs.second;
              ret.assignment_epochs[to_process.topic][fetch_state.partition_id]
                = fetch_state.assignment_epoch;
              if (!fetch_state.fetch_offset.has_value()) {
                  to_process.to_list_offsets.push_back(fetch_state);
                  return;
              }

              constexpr auto should_skip =
                [](const partition_fetch_state& fetch_state) {
                    return !fetch_state.incremental_include
                           && fetch_state.fetch_offset.value()
                                >= fetch_state.high_watermark.value_or(
                                  kafka::offset::min());
                };

              if (inc && should_skip(fetch_state)) {
                  // session should be up to date, so we can omit this
                  // partition from the request
                  return;
              }
              to_process.to_include_in_fetch.push_back(fetch_state);
          });

        if (!to_process.empty()) {
            ret.partitions.push_back(std::move(to_process));
        }
    }

    for (auto& [topic, partitions] : _partitions_to_forget) {
        partitions_to_process to_process;
        to_process.topic = topic;
        ssx::async_counter cnt;
        // TODO(oren): maybe the async isn't so necessary here and we can
        // just copy straight across
        co_await ssx::async_for_each_counter(
          cnt, partitions, [&to_process](auto& p_fs) {
              to_process.to_forget.push_back(p_fs.second);
          });
        if (!to_process.empty()) {
            ret.partitions.push_back(std::move(to_process));
        }
    }

    co_return ret;
}

ss::future<fetch_request> fetcher::make_fetch_request(
  const chunked_vector<partitions_to_process>& to_process) {
    // TODO: handle api versions here
    // f.e. customize fetch request based on the version
    fetch_request req;

    req.data.replica_id = client_replica_id;
    req.data.isolation_level = _parent->_config.isolation_level;
    req.data.max_wait_ms = _parent->_config.max_wait_time;
    req.data.max_bytes = _parent->_config.max_fetch_size;
    req.data.min_bytes = _parent->_config.min_bytes;

    req.data.session_id = _session_state.session_id;
    req.data.session_epoch = _session_state.session_epoch;

    ssx::async_counter cnt;

    for (const auto& topic_partitions : to_process) {
        fetch_topic f_topic;
        forgotten_topic r_topic;

        const auto& to_include = topic_partitions.to_include_in_fetch;
        const auto& to_forget = topic_partitions.to_forget;

        vassert(
          to_include.empty() || to_forget.empty(),
          "Entry should have either included or forgotten partitions");

        if (!to_include.empty()) {
            f_topic.topic = topic_partitions.topic;
            f_topic.partitions.reserve(
              topic_partitions.to_include_in_fetch.size());
            co_await ssx::async_for_each_counter(
              cnt,
              topic_partitions.to_include_in_fetch,
              [this, &f_topic](const partition_fetch_state& f_state) {
                  fetch_partition f_partition;
                  f_partition.partition = f_state.partition_id;
                  f_partition.fetch_offset = kafka::offset_cast(
                    *f_state.fetch_offset);
                  f_partition.last_fetched_epoch = f_state.current_leader_epoch;
                  f_partition.partition_max_bytes
                    = _parent->_config.partition_max_bytes;
                  f_topic.partitions.push_back(std::move(f_partition));
              });
            req.data.topics.push_back(std::move(f_topic));
        }

        if (!to_forget.empty()) {
            r_topic.topic = topic_partitions.topic;
            r_topic.partitions.reserve(to_forget.size());
            ssx::async_counter cnt;
            co_await ssx::async_for_each_counter(
              cnt, to_forget, [&r_topic](model::partition_id pid) {
                  r_topic.partitions.push_back(pid);
              });
            if (_session_state.incremental()) {
                req.data.forgotten_topics_data.push_back(std::move(r_topic));
            }
        }
    }

    co_return req;
}

namespace {

ss::future<chunked_vector<model::record_batch>>
reader_to_chunked_vector(kafka::batch_reader reader) {
    return model::consume_reader_to_chunked_vector(
      model::make_record_batch_reader<kafka::batch_reader>(std::move(reader)),
      model::no_timeout);
}
} // namespace

bool fetcher::maybe_update_fetch_offset(
  const model::topic& topic,
  model::partition_id partition_id,
  kafka::offset last_received,
  kafka::offset high_watermark,
  std::optional<assignment_epoch> maybe_response_epoch) {
    auto t_it = _partitions.find(topic);
    if (t_it == _partitions.end()) {
        return false;
    }
    auto p_it = t_it->second.find(partition_id);
    if (p_it == t_it->second.end()) {
        return false;
    }

    // possible in a tight race
    // 1. unassign from A
    // 2. begin fetch
    // 3. assign to A
    // 4. broker receives update
    // 5. the fetch will receive a response for which it did not have an epoch
    // this is logically the same as epoch mismatch
    if (!maybe_response_epoch) {
        vlog(
          logger().info,
          "[broker: {}] Ignoring unbidden {}/{} current epoch: {}",
          _id,
          topic,
          partition_id,
          p_it->second.assignment_epoch);
        return false;
    }

    const auto response_epoch = maybe_response_epoch.value();

    if (p_it->second.assignment_epoch != response_epoch) {
        vlog(
          logger().trace,
          "[broker: {}] Ignoring {}/{} reply, assignment epoch changed, "
          "request epoch: {}, current epoch: {}",
          _id,
          topic,
          partition_id,
          response_epoch,
          p_it->second.assignment_epoch);
        return false;
    }

    vlog(
      logger().trace,
      "[broker: {}] Updating {}/{} fetch offset from {} to {} {{hwm: {}}}",
      _id,
      topic,
      partition_id,
      p_it->second.fetch_offset,
      kafka::next_offset(last_received),
      high_watermark);
    p_it->second.high_watermark = high_watermark;
    p_it->second.fetch_offset = kafka::next_offset(last_received);
    // we updated the fetch offset, so we should sync to with the broker's
    // fetch session on the next request
    p_it->second.incremental_include = true;

    return true;
}

ss::future<> fetcher::do_fetch() {
    bool needs_backoff = false;
    try {
        co_await _partitions_updated.wait([this] { return !is_idle(); });

        /**
         * Iterate once over all partitions that are assigned to the fetcher and
         * collect necessary actions. f.e. list offsets or include in fetch
         * request.
         */
        auto partitions_with_epochs = co_await collect_partitions();
        auto assignment_epochs = std::move(
          partitions_with_epochs.assignment_epochs);

        auto list_offsets_err = co_await maybe_initialise_fetch_offsets(
          partitions_with_epochs.partitions, assignment_epochs);
        /**
         */
        if (list_offsets_err != kafka::error_code::none) {
            vlog(
              logger().debug,
              "[broker: {}] list offsets error: {}",
              _id,
              list_offsets_err);
            if (is_retriable_error(list_offsets_err)) {
                needs_backoff = true;
            } else {
                // propagate not retriable error to the queue
                co_await queue().push_error(list_offsets_err);
            }
        }

        auto req = co_await make_fetch_request(
          partitions_with_epochs.partitions);

        if (_as.abort_requested()) {
            // if the abort was requested, we should not dispatch the request
            // and just return
            co_return;
        }
        // TODO: cache the version of the fetch request
        auto version = co_await get_fetch_request_version();
        auto response = co_await _parent->_cluster->dispatch_to(
          _id, std::move(req), version);
        auto fetch_result = co_await process_fetch_response(
          std::move(response),
          assignment_epochs,
          partitions_with_epochs.partitions);

        if (fetch_result.has_error()) {
            if (is_retriable_error(fetch_result.error())) {
                needs_backoff = true;
            } else {
                // propagate non retriable error to the queue
                co_await queue().push_error(fetch_result.error());
                co_return;
            }
        }

        auto fetch_result_value = std::move(fetch_result.value());
        _session_state.update_fetch_session(fetch_result_value.session_id);
        if (fetch_result_value.needs_metadata_update) {
            // if we need to update metadata, we should do it
            // so that we can retry fetching the partitions later
            needs_backoff = true;
        }

        if (!fetch_result_value.topics.empty()) {
            co_await queue().push(
              std::move(fetch_result_value.topics),
              fetch_result_value.total_bytes);
        }

    } catch (...) {
        if (ssx::is_shutdown_exception(std::current_exception())) {
            // if the exception is a shutdown exception, we should not log it
            // and just return
            co_return;
        }
        vlog(
          logger().warn,
          "error encountered while fetching from broker with id: {} - {}",
          _id,
          std::current_exception());
        needs_backoff = true;
    }
    if (needs_backoff) {
        co_await _parent->_cluster->request_metadata_update();
        co_await ss::sleep_abortable(error_backoff, _as);
    }
}

std::optional<fetcher::assignment_epoch> fetcher::find_assignment_epoch(
  const model::topic& topic,
  model::partition_id partition,
  const topic_partition_map<assignment_epoch>& epochs) {
    auto topic_iterator = epochs.find(topic);

    if (topic_iterator == epochs.end()) {
        return std::nullopt;
    }

    const auto& partition_map = topic_iterator->second;
    auto partition_iterator = partition_map.find(partition);

    if (partition_iterator == partition_map.end()) {
        return std::nullopt;
    }

    return partition_iterator->second;
}

ss::future<kafka_result<fetcher::fetch_response_content>>
fetcher::process_fetch_response(
  fetch_response resp,
  const topic_partition_map<assignment_epoch>& epochs,
  const chunked_vector<partitions_to_process>& partitions) {
    if (resp.data.error_code != kafka::error_code::none) {
        co_return resp.data.error_code;
    }

    // Hold the lock here as the fetch response processing updates the fetch
    // offsets, we do not want this to interfere with assignment updates.

    auto lock = co_await _state_lock.get_units();

    // For fetch session maintenance, the goal is to omit partitions from each
    // fetch request whenever possible. The incremental_include flag controls
    // whether a certain partition appears in the next fetch request, after
    // which that partition should be omitted, assuming the fetch succeeds and
    // no new data is returned for that partition.
    //
    // In general, a partition may be omitted from subsequent fetch requests iff
    // it did not appear in the topic data in the current response. For this
    // reason we flip the incremental_include for a partition based on whether
    // that partition was included in the last request. This map tracks such
    // partitions allowing us to leave the incremental_include flag ON in these
    // cases.
    chunked_hash_map<model::topic, absl::flat_hash_set<model::partition_id>>
      dirty_partitions;

    fetch_response_content result;
    result.session_id = kafka::fetch_session_id{resp.data.session_id};
    result.topics.reserve(resp.data.responses.size());
    for (auto& topic_response : resp.data.responses) {
        if (topic_response.partitions.empty()) {
            // no partitions in the response, skip it
            continue;
        }
        fetched_topic_data topic_data;
        topic_data.topic = std::move(topic_response.topic);
        topic_data.partitions.reserve(topic_response.partitions.size());

        for (auto& part_response : topic_response.partitions) {
            fetched_partition_data part_data;
            part_data.error = part_response.error_code;
            part_data.partition_id = part_response.partition_index;

            if (part_response.error_code != kafka::error_code::none) {
                if (is_retriable_error(part_response.error_code)) {
                    vlog(
                      logger().debug,
                      "[broker: {}] {}/{} retriable fetch error: {}",
                      _id,
                      topic_data.topic,
                      part_data.partition_id,
                      part_response.error_code);

                    result.needs_metadata_update = true;
                    // skip partition in the result, but mark that we
                    // need to update metadata
                    // so that we can retry fetching it later
                    continue;
                }
                vlog(
                  logger().warn,
                  "[broker: {}] {}/{} fetch error: {}",
                  _id,
                  topic_data.topic,
                  part_data.partition_id,
                  part_response.error_code);

                // this partition errored, so any pending incremental fetches
                // should be retried
                dirty_partitions[topic_data.topic].insert(
                  part_data.partition_id);
            } else {
                part_data.high_watermark = model::offset_cast(
                  part_response.high_watermark);
                part_data.last_stable_offset = model::offset_cast(
                  part_response.last_stable_offset);
                part_data.leader_epoch
                  = part_response.current_leader.leader_epoch;
                part_data.aborted_transactions = std::move(
                  part_response.aborted_transactions);

                vlog(
                  logger().trace,
                  "[broker: {}] topic: {}, partition fetch response: {}",
                  _id,
                  topic_data.topic,
                  part_response);

                if (
                  !part_response.records.has_value()
                  || part_response.records->is_end_of_stream()) {
                    continue;
                }
                topic_data.total_bytes += part_response.records->size_bytes();
                part_data.data = co_await reader_to_chunked_vector(
                  std::move(part_response.records.value()));

                auto maybe_response_epoch = find_assignment_epoch(
                  topic_data.topic, part_data.partition_id, epochs);

                bool updated_offset = maybe_update_fetch_offset(
                  topic_data.topic,
                  part_data.partition_id,
                  model::offset_cast(part_data.data.back().last_offset()),
                  part_data.high_watermark,
                  maybe_response_epoch);
                if (!updated_offset) {
                    // case when partition is either
                    // 1. partition is not assigned to this fetcher
                    // 2. assignment epoch changed
                    // for either skip
                    continue;
                }
                dirty_partitions[topic_data.topic].insert(
                  part_data.partition_id);
            }
            topic_data.partitions.push_back(std::move(part_data));
        }
        result.total_bytes += topic_data.total_bytes;
        if (topic_data.partitions.empty()) {
            continue;
        }
        result.topics.push_back(std::move(topic_data));
    }

    // Clear incremental fetch state, skipping partitions that errored
    // or just returned new data.
    for (const auto& to_process : partitions) {
        const auto& included = to_process.to_include_in_fetch;
        const auto& forgotten = to_process.to_forget;
        const auto& topic = to_process.topic;

        // vassert to enforce invariant by construction. this is an
        // implementation detail. if this assert fires, that means a change
        // to collect_partitions will require a corresponding change to this
        // loop body.
        vassert(
          included.empty() || forgotten.empty(),
          "partitions_to_process should have either included or forgotten "
          "partitions, not both");

        if (!included.empty()) {
            // _partitions maps topic -> parition map
            // partition map maps partition -> subscription info
            // get an iterator to the topic map
            auto topic_iterator = _partitions.find(topic);
            if (topic_iterator == _partitions.end()) {
                continue;
            }

            auto errs_it = dirty_partitions.find(topic);
            bool topic_err = errs_it != dirty_partitions.end();

            auto& partition_map = topic_iterator->second;

            for (const auto& p : included) {
                bool partition_err = topic_err
                                     && errs_it->second.contains(
                                       p.partition_id);
                auto partition_iterator = partition_map.find(p.partition_id);
                if (
                  partition_iterator != partition_map.end() && !partition_err) {
                    // compare the epochs before we make an edit
                    const auto assigned_epoch
                      = partition_iterator->second.assignment_epoch;

                    auto maybe_request_epoch = find_assignment_epoch(
                      topic, p.partition_id, epochs);

                    if (!maybe_request_epoch) {
                        // see maybe_update_fetch_offset for explanation
                        vlog(
                          logger().info,
                          "unbidden response on ntp: {}/{}",
                          topic,
                          p.partition_id);
                        continue;
                    }

                    const auto request_epoch = maybe_request_epoch.value();

                    if (assigned_epoch == request_epoch) {
                        partition_iterator->second.incremental_include = false;
                    } else {
                        vlog(
                          logger().trace,
                          "disclusion epoch mismatch on ntp: {}, "
                          "fetched_epoch, {}, current_epoch: {}",
                          topic,
                          p.partition_id,
                          request_epoch,
                          assigned_epoch);
                    }
                }
            }
        }

        if (!forgotten.empty()) {
            auto fgt_it = _partitions_to_forget.find(topic);
            if (fgt_it == _partitions_to_forget.end()) {
                continue;
            }
            auto& ps = fgt_it->second;
            for (auto p_id : forgotten) {
                if (auto p_it = ps.find(p_id); p_it != ps.end()) {
                    vlog(
                      logger().trace,
                      "[broker: {}] Requested to forget {} in session {}",
                      _id,
                      model::topic_partition_view{topic, p_id},
                      _session_state.session_id);
                    ps.erase(p_it);
                }
            }
            if (ps.empty()) {
                _partitions_to_forget.erase(fgt_it);
            }
        }
    }

    co_return result;
}

namespace {
model::timestamp timestamp_for_offset_reset_policy(offset_reset_policy policy) {
    switch (policy) {
    case offset_reset_policy::earliest:
        return list_offsets_request::earliest_timestamp;
    case offset_reset_policy::latest:
        return list_offsets_request::latest_timestamp;
    }
}
} // namespace

ss::future<kafka::error_code> fetcher::maybe_initialise_fetch_offsets(
  const chunked_vector<partitions_to_process>& partitions,
  const topic_partition_map<assignment_epoch>& epochs) {
    const auto timestamp = timestamp_for_offset_reset_policy(
      _parent->_config.reset_policy);

    list_offsets_request req;
    req.data.topics.reserve(partitions.size());
    for (auto& topic_partitions : partitions) {
        if (topic_partitions.to_list_offsets.empty()) {
            // no partitions to fetch offsets for
            continue;
        }
        list_offset_topic l_topic;
        l_topic.name = topic_partitions.topic;
        l_topic.partitions.reserve(topic_partitions.to_list_offsets.size());
        for (auto& fetch_state : topic_partitions.to_list_offsets) {
            // we need to fetch the offset for this partition
            list_offset_partition l_partition;
            l_partition.partition_index = fetch_state.partition_id;
            l_partition.timestamp = timestamp;
            l_partition.current_leader_epoch = fetch_state.current_leader_epoch;

            l_topic.partitions.push_back(std::move(l_partition));
        }

        req.data.topics.push_back(std::move(l_topic));
    }

    if (req.data.topics.empty()) {
        // no partitions to fetch offsets for
        co_return error_code::none;
    }

    auto list_offsets_response = co_await do_list_offsets(std::move(req));
    if (list_offsets_response.has_error()) {
        if (is_retriable_error(list_offsets_response.error())) {
            vlog(
              logger().info,
              "list_offsets request failed with retriable error: {}",
              list_offsets_response.error());
        } else {
            vlog(
              logger().warn,
              "list_offsets request failed with an error: {}",
              list_offsets_response.error());
        }
        co_return list_offsets_response.error();
    }
    // TODO: what should we do with the error code in the response?
    kafka::error_code error = kafka::error_code::none;
    for (auto& response_topic : list_offsets_response.value()) {
        for (auto& response_partition : response_topic.offsets) {
            if (response_partition.error_code != kafka::error_code::none) {
                if (is_retriable_error(response_partition.error_code)) {
                    vlog(
                      logger().debug,
                      "[broker: {}] {}/{} retriable list_offsets error: {}",
                      _id,
                      response_topic.topic,
                      response_partition.partition_id,
                      response_partition.error_code);
                }
                // Only overwrite if we don't have an error, or if current error
                // is retriable but new error is not retriable
                if (error == kafka::error_code::none ||
                  (is_retriable_error(error) && !is_retriable_error(response_partition.error_code))) {
                    error = response_partition.error_code;
                }
                continue;
            }

            // global state may have changed, we're not locked
            auto t_it = _partitions.find(response_topic.topic);
            if (t_it == _partitions.end()) {
                continue;
            }
            auto p_it = t_it->second.find(response_partition.partition_id);
            if (p_it == t_it->second.end()) {
                continue;
            }

            const auto assigned_epoch = p_it->second.assignment_epoch;

            auto maybe_response_epoch = find_assignment_epoch(
              response_topic.topic, response_partition.partition_id, epochs);

            if (!maybe_response_epoch) {
                vlog(
                  logger().warn,
                  "[broker: {} received a list topics response which was not "
                  "requested on ntp: {}/{}",
                  _id,
                  response_topic.topic,
                  response_partition.partition_id);
                continue;
            }

            const auto request_epoch = maybe_response_epoch.value();

            if (request_epoch != assigned_epoch) {
                vlog(
                  logger().trace,
                  "[broker: {}] Skipping partition {}/{} list offset response "
                  "as assignment epoch has changed. request_epoch: {}, "
                  "current_epoch: {}",
                  _id,
                  response_topic.topic,
                  response_partition.partition_id,
                  response_partition.offset,
                  request_epoch,
                  p_it->second.assignment_epoch);
                continue;
            }
            vlog(
              logger().info,
              "[broker: {}] Resetting partition {}/{} fetch offset to: {}",
              _id,
              response_topic.topic,
              response_partition.partition_id,
              response_partition.offset);
            p_it->second.fetch_offset = response_partition.offset;
            p_it->second.high_watermark.reset();
            p_it->second.incremental_include = true;
        }
    }

    co_return error;
}

ss::future<api_version> fetcher::get_fetch_request_version() const {
    constexpr auto max_client_version = kafka::api_version{12};
    auto version = co_await _parent->_cluster->supported_api_versions(
      _id, kafka::fetch_api::key);
    if (version) {
        co_return std::min(version->max, max_client_version);
    }
    // if the version is not supported, we fallback to the minimum version
    // which is 1, this is the first version of the Fetch API that use the new
    // batch format
    co_return api_version{1};
}

ss::future<api_version> fetcher::get_list_offsets_request_version() const {
    auto version = co_await _parent->_cluster->supported_api_versions(
      _id, kafka::list_offsets_api::key);
    if (version) {
        co_return std::min(version->max, kafka::list_offsets_api::max_valid);
    }
    co_return kafka::list_offsets_api::min_valid;
}

ss::future<> fetcher::assign_partition(
  model::topic_partition_view tp, std::optional<kafka::offset> offset) {
    auto lock = co_await _state_lock.get_units();
    vlog(
      logger().debug,
      "[broker: {}] Assigned partition: {} with offset: {}",
      _id,
      tp,
      offset);

    _partitions[tp.topic][tp.partition] = partition_fetch_state{
      .partition_id = tp.partition,
      .fetch_offset = offset,
      .assignment_epoch = next_epoch(),
      .incremental_include = true};

    // in the case of fast leadership transfers, we may have a partition both
    // being added and forgotten, in which case, make sure that it is only being
    // added
    auto forget_topic_iterator = _partitions_to_forget.find(tp.topic);
    if (forget_topic_iterator != _partitions_to_forget.end()) {
        // topic is in the 'to forget map', now is the partition in the topic's
        // map
        auto& partitions_to_forget = forget_topic_iterator->second;
        auto forget_partition_iterator = partitions_to_forget.find(
          tp.partition);
        if (forget_partition_iterator != partitions_to_forget.end()) {
            // the ntp is both being added and forgotten, make sure its only
            // added
            partitions_to_forget.erase(forget_partition_iterator);
            if (partitions_to_forget.empty()) {
                // cleanup if this was the last partition
                _partitions_to_forget.erase(forget_topic_iterator);
            }
        }
    }

    _partitions_updated.signal();
    co_return;
}
ss::future<std::optional<kafka::offset>>
fetcher::unassign_partition(model::topic_partition_view tp_v) {
    auto lock = co_await _state_lock.get_units();
    auto it = _partitions.find(tp_v.topic);
    if (it == _partitions.end()) {
        // partition not found, nothing to unassign
        co_return std::nullopt;
    }
    auto& partitions = it->second;
    auto p_it = partitions.find(tp_v.partition);
    if (p_it == partitions.end()) {
        // partition not found, nothing to unassign
        co_return std::nullopt;
    }
    vlog(
      logger().debug,
      "[broker: {}] Removing partition: {} assignment",
      _id,
      tp_v);
    _partitions_to_forget[tp_v.topic][tp_v.partition] = tp_v.partition;
    auto fetch_offset = p_it->second.fetch_offset;
    partitions.erase(p_it);
    if (partitions.empty()) {
        // if there are no partitions left for this topic, remove the topic
        _partitions.erase(it);
    }

    _partitions_updated.signal();
    co_return fetch_offset;
}

void fetcher::toggle_sessions(fetch_sessions_enabled v) {
    _session_state.toggle(v);
}

ss::future<kafka_result<chunked_vector<topic_partition_offsets>>>
fetcher::do_list_offsets(list_offsets_request req) {
    try {
        auto version = co_await get_list_offsets_request_version();
        auto reply = co_await _parent->_cluster->dispatch_to(
          _id, std::move(req), version);
        chunked_vector<topic_partition_offsets> offsets;
        offsets.reserve(reply.data.topics.size());

        for (auto& topic : reply.data.topics) {
            topic_partition_offsets topics;
            topics.topic = std::move(topic.name);
            topics.offsets.reserve(topic.partitions.size());
            for (auto& partition : topic.partitions) {
                topics.offsets.push_back(
                  partition_offset{
                    .partition_id = partition.partition_index,
                    .error_code = partition.error_code,
                    .leader_epoch = partition.leader_epoch,
                    .offset = model::offset_cast(partition.offset),
                  });
            }
            offsets.push_back(std::move(topics));
        }
        co_return offsets;
    } catch (const broker_error& e) {
        vlog(
          logger().warn,
          "list_offsets request to broker {} failed with broker error: {}",
          _id,
          e);
        co_return e.error;
    } catch (...) {
        vlog(
          logger().warn,
          "list_offsets request to broker {} failed with an error: {}",
          _id,
          std::current_exception());
        co_return kafka::error_code::unknown_server_error;
    }
}

fmt::iterator fetch_session_state::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{id: {}, epoch: {}, state: {}}}",
      session_id,
      session_epoch,
      session_state);
}

} // namespace kafka::client

namespace fmt {
auto fmt::formatter<kafka::client::fetch_session_state::state>::format(
  kafka::client::fetch_session_state::state s, format_context& ctx) const
  -> format_context::iterator {
    std::string_view result = "unknown";
    switch (s) {
        using enum kafka::client::fetch_session_state::state;
    case none:
        result = "fetch_session_state::state::none";
        break;
    case need_full_fetch:
        result = "fetch_session_state::state::need_full_fetch";
        break;
    case incremental_fetch:
        result = "fetch_session_state::state::incremental_fetch";
        break;
    case needs_close:
        result = "fetch_session_state::state::needs_close";
        break;
    }
    return formatter<std::string_view>::format(result, ctx);
}
} // namespace fmt
