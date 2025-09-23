/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster_link/replication/mux_remote_consumer.h"

#include "cluster_link/logger.h"
#include "ssx/future-util.h"

namespace cluster_link::replication {

ss::future<> mux_remote_consumer::start() {
    co_await _consumer->start();
    ssx::repeat_until_gate_closed(_gate, [this]() {
        return fetch_loop().handle_exception([](const std::exception_ptr& e) {
            if (!ssx::is_shutdown_exception(e)) {
                vlog(cllog.error, "Fetch loop failed: {}", e);
            }
        });
    });
}

ss::future<> mux_remote_consumer::stop() noexcept {
    _cv.broken();
    auto f = _gate.close();
    co_await _consumer->stop();
    co_await std::move(f);
    static constexpr auto concurrent_stops = 5;
    co_await ss::max_concurrent_for_each(
      _partitions, concurrent_stops, [](auto& p) { return p.second->stop(); });
    _partitions.clear();
}

mux_remote_consumer::result mux_remote_consumer::add(
  const model::topic_partition& tp, kafka::offset offset) {
    vlog(
      cllog.trace, "Adding partition {}, with initial offset: {}", tp, offset);
    _gate.check();
    if (_partitions.contains(tp)) {
        return std::unexpected(errc::partition_already_exists);
    }
    auto [it, success] = _partitions.emplace(
      tp, std::make_unique<partition_data_queue>(_partition_max_buffered));
    it->second->reset(offset);
    _pending_assignment[tp.topic].insert(tp.partition);
    _cv.signal();
    return {};
}

ss::future<mux_remote_consumer::result>
mux_remote_consumer::remove(const model::topic_partition& tp) {
    vlog(cllog.trace, "Removing partition {}", tp);
    auto holder = _gate.hold();
    auto it = _partitions.find(tp);
    if (it == _partitions.end()) {
        co_return std::unexpected(errc::partition_not_found);
    }
    auto queue = std::move(it->second);
    _partitions.erase(it);
    auto pending_it = _pending_assignment.find(tp.topic);
    if (pending_it != _pending_assignment.end()) {
        pending_it->second.erase(tp.partition);
        if (pending_it->second.empty()) {
            _pending_assignment.erase(pending_it);
        }
    }
    _cv.signal();
    co_await _consumer->unassign_partitions({tp});
    co_await queue->stop();
    co_return result{};
}

mux_remote_consumer::result mux_remote_consumer::reset(
  const model::topic_partition& tp, kafka::offset offset) {
    vlog(cllog.trace, "Resetting partition {}, to offset: {}", tp, offset);
    _gate.check();
    auto it = _partitions.find(tp);
    if (it == _partitions.end()) {
        return std::unexpected(errc::partition_not_found);
    }
    it->second->reset(offset);
    _pending_assignment[tp.topic].insert(tp.partition);
    return {};
}

ss::future<
  std::expected<partition_data_queue::fetch_data, mux_remote_consumer::errc>>
mux_remote_consumer::fetch(
  const model::topic_partition& tp, ss::abort_source& as) {
    auto holder = _gate.hold();
    auto it = _partitions.find(tp);
    if (it == _partitions.end()) {
        co_return std::unexpected(errc::partition_not_found);
    }
    co_return co_await it->second->fetch(as);
}

ss::future<> mux_remote_consumer::assign_pending_partitions() {
    if (_pending_assignment.empty()) {
        co_return;
    }
    chunked_vector<kafka::client::topic_assignment> assignments;
    // Assign pending partitions
    for (auto& [topic, partitions] : _pending_assignment) {
        kafka::client::topic_assignment assignment;
        assignment.topic = topic;
        for (auto partition_id : partitions) {
            model::topic_partition tp(topic, partition_id);
            auto it = _partitions.find(tp);
            vassert(it != _partitions.end(), "Partition {} not found", tp);
            const auto& queue = it->second;
            if (queue->full()) {
                // The queue is still full which indicates it was not drained,
                // so we do not assign it in this round. This usually indicates
                // there is an issue with the data sink.
                vlog(
                  cllog.debug,
                  "[{}] Skipping assignment due to full queue",
                  tp);
                continue;
            }
            vlog(
              cllog.trace,
              "Assigning partition {} at offset: {}",
              tp,
              queue->next());
            assignment.partitions.emplace_back(partition_id, queue->next());
        }
        if (assignment.partitions.empty()) {
            continue;
        }
        assignments.push_back(std::move(assignment));
    }
    if (assignments.empty()) {
        co_return;
    }
    // make a copy of assignments
    chunked_vector<kafka::client::topic_assignment> assignments_copy;
    for (auto& t_assign : assignments) {
        assignments_copy.push_back(
          {.topic = t_assign.topic, .partitions = t_assign.partitions.copy()});
    }
    co_await _consumer->assign_partitions(std::move(assignments_copy));
    // remove them from pending assignments
    for (auto& t_assign : assignments) {
        auto it = _pending_assignment.find(t_assign.topic);
        if (it == _pending_assignment.end()) {
            continue;
        }
        for (auto& partition : t_assign.partitions) {
            it->second.erase(partition.partition_id);
        }
        if (it->second.empty()) {
            _pending_assignment.erase(it);
        }
    }
}

bool mux_remote_consumer::can_ignore_partition_data(
  const model::topic_partition& tp) {
    auto partition_inactive = _partitions.find(tp) == _partitions.end();
    auto it = _pending_assignment.find(tp.topic);
    // If a partition is in pending_assignment, it got reseeked to a new offset
    // ignore data for now and reassign to the new offset in the next iteration.
    auto partition_got_reset = (it != _pending_assignment.end())
                               && it->second.contains(tp.partition);
    return partition_inactive || partition_got_reset;
}

ss::future<> mux_remote_consumer::process_fetched_data(
  chunked_vector<kafka::client::fetched_topic_data> fetches) {
    chunked_vector<model::topic_partition> to_unassign;
    for (auto& tp_fetch : fetches) {
        const auto& topic = tp_fetch.topic;
        for (auto& partition : tp_fetch.partitions) {
            auto tp = model::topic_partition(topic, partition.partition_id);
            if (can_ignore_partition_data(tp)) {
                vlog(cllog.trace, "skipping data for partition: {}", tp);
                continue;
            }
            auto it = _partitions.find(tp);
            vassert(it != _partitions.end(), "Partition {} not found", tp);
            // Note: enqueue _always_ succeeds and may oversubscribe memory by
            // a tiny amount since the consumed batches (per partition) are
            // usually not that large and it is very unlikely that the sink is
            // not able to catch up. This keeps the design simple. Here we try
            // to be good citizen and not overwhelm the queue further and
            // unassign the partition temporarily if the queue is full and then
            // retry at a later time.
            auto can_enqueue_more = it->second->enqueue(
              std::move(partition.data));
            if (!can_enqueue_more) {
                // the queue is full, this is usually a rare case indicating the
                // sink is not able to catchup with the rate of incoming data.
                // We put it to the pending_assignments map and retry in the
                // next pass once the queue has some space.
                to_unassign.push_back(tp);
            }
        }
    }
    if (!to_unassign.empty()) {
        co_await _consumer->unassign_partitions(to_unassign.copy());
        vlog(cllog.trace, "Unassigned partitions: {}", to_unassign.size());
        // add the partitions back to _pending_assignments
        for (const auto& tp : to_unassign) {
            _pending_assignment[tp.topic].insert(tp.partition);
        }
    }
}

ss::future<> mux_remote_consumer::fetch_loop() {
    while (!_gate.is_closed()) {
        co_await _cv.wait([this] { return !_partitions.empty(); });
        vlog(
          cllog.trace,
          "fetch loop iteration with {} partitions, unassigned: {}",
          _partitions.size(),
          _pending_assignment.size());
        co_await assign_pending_partitions();
        auto fetch_result = co_await _consumer->fetch_next(_fetch_max_wait);
        if (fetch_result.has_error()) {
            vlog(cllog.warn, "Fetch failed: {}", fetch_result.error());
            continue;
        }
        co_await process_fetched_data(std::move(fetch_result.value()));
    }
}
} // namespace cluster_link::replication

auto fmt::formatter<cluster_link::replication::mux_remote_consumer::errc>::
  format(
    const cluster_link::replication::mux_remote_consumer::errc& e,
    fmt::format_context& ctx) const -> decltype(ctx.out()) {
    switch (e) {
    case cluster_link::replication::mux_remote_consumer::errc::
      partition_not_found:
        return fmt::formatter<std::string_view>::format(
          "partition_not_found", ctx);
    case cluster_link::replication::mux_remote_consumer::errc::
      partition_already_exists:
        return fmt::formatter<std::string_view>::format(
          "partition_already_exists", ctx);
    }
}

std::ostream& operator<<(
  std::ostream& os,
  const cluster_link::replication::mux_remote_consumer::errc& e) {
    return os << fmt::format("{}", e);
}
