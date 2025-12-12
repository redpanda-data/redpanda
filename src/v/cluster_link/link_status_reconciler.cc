/**
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/dev/licenses/rcl.md
 *
 */

#include "cluster_link/link_status_reconciler.h"

#include "cluster_link/deps.h"
#include "cluster_link/logger.h"
#include "ssx/future-util.h"

static constexpr auto reconciliation_interval = std::chrono::seconds{1};
static constexpr auto mutation_timeout = std::chrono::seconds{5};

namespace cluster_link {

ss::future<> link_status_reconciler::start() noexcept {
    vlog(
      cllog.trace,
      "Starting link status reconciler in term {}.",
      _controller_term);
    if (_gate.is_closed()) {
        co_return;
    }
    reconcile();
}

ss::future<> link_status_reconciler::stop() noexcept {
    _as.request_abort();
    auto f = _gate.close();
    co_await ss::max_concurrent_for_each(
      _reconcilers, 8, [](auto& p) { return p.second->stop(); });
    _reconcilers.clear();
    co_await std::move(f);
    vlog(
      cllog.trace,
      "Stopped link status reconciler in term {}.",
      _controller_term);
}

void link_status_reconciler::reconcile() {
    if (_as.abort_requested() || _gate.is_closed()) {
        return;
    }
    // Note: the number of links is small (almost always 1), so we can afford
    // to run these loops every time.
    const auto links = _link_registry->get_all_link_ids();
    for (auto& link_id : links) {
        if (_reconcilers.find(link_id) == _reconcilers.end()) {
            _reconcilers.emplace(
              link_id,
              std::make_unique<per_link_reconciler>(
                *_link_registry, link_id, _controller_term, _as));
        }
    }
    // find all links that no longer exist and remove their reconcilers
    for (auto it = _reconcilers.begin(); it != _reconcilers.end();) {
        if (std::ranges::find(links, it->first) == links.end()) {
            auto reconciler = std::move(it->second);
            // Note the gate is open at this point, so this never fails.
            ssx::spawn_with_gate(
              _gate, [reconciler = std::move(reconciler)]() mutable {
                  return ss::do_with(
                    std::move(reconciler),
                    [](auto& reconciler) { return reconciler->stop(); });
              });
            it = _reconcilers.erase(it);
        } else {
            ++it;
        }
    }
    // notify all reconcilers
    for (auto& [_, reconciler] : _reconcilers) {
        reconciler->notify_changes();
    }
}

link_status_reconciler::per_link_reconciler::per_link_reconciler(
  link_registry& registry,
  model::id_t link_id,
  ::model::term_id term,
  ss::abort_source& as)
  : _registry(registry)
  , _link_id(link_id)
  , _term(term) {
    _as_sub = as.subscribe([this] noexcept { _as.request_abort(); });
    ssx::repeat_until_gate_closed(
      _gate, [this] { return reconcile_status_changes(); });
}

void link_status_reconciler::per_link_reconciler::notify_changes() {
    _cv.signal();
}

ss::future<> link_status_reconciler::per_link_reconciler::stop() noexcept {
    _as.request_abort();
    _cv.broken();
    co_await _gate.close();
}

ss::future<> link_status_reconciler::per_link_reconciler::try_finish_failover(
  const ::model::topic& topic) noexcept {
    if (_as.abort_requested()) {
        co_return;
    }
    vlog(
      cllog.trace,
      "[{}] Checking if topic {} can be failed over",
      _link_id,
      topic);
    auto topic_report = co_await _registry.shadow_topic_report(_link_id, topic);
    if (!topic_report.has_value()) {
        vlog(
          cllog.warn,
          "[{}] Failed to get shadow topic report for topic {}, error: {}",
          _link_id,
          topic,
          topic_report.error());
        co_return;
    }
    // A topic can be promoted if all the partitions leaders have been reported
    // in the health report
    // and each such leader has seen the link update revision that the
    // controller has seen for the link. The link revision check guarantees that
    // the partition leader has unblocked the kafka API for the mirror topic
    // after the failing_over state. This is not a fool proof check but is a
    // reasonable heuristic.
    auto maybe_rev = _registry.get_last_update_revision(_link_id);
    if (!maybe_rev.has_value()) {
        vlog(
          cllog.warn,
          "[{}] Inconsistent state detected, link revision does not exist",
          _link_id);
        co_return;
    }
    chunked_hash_set<::model::partition_id> valid_partitions;
    auto local_update_revision = maybe_rev.value();
    for (const auto& broker : topic_report->brokers) {
        if (broker.link_update_revision < local_update_revision) {
            // this broker has not yet seen the link update revision
            continue;
        }
        for (auto& leader : broker.leaders) {
            valid_partitions.insert(leader.partition);
        }
    }
    if (
      valid_partitions.size()
      != static_cast<size_t>(topic_report->total_partitions)) {
        vlog(
          cllog.debug,
          "[{}] Topic {} cannot be promoted yet, only {}/{} partitions are "
          "reported as valid.",
          _link_id,
          topic,
          valid_partitions.size(),
          topic_report->total_partitions);
        co_return;
    }

    auto result = co_await _registry.update_mirror_topic_state(
      _link_id,
      {.topic = topic, .status = model::mirror_topic_status::failed_over},
      ::model::timeout_clock::now() + mutation_timeout);
    if (result != cluster::cluster_link::errc::success) {
        vlog(
          cllog.warn,
          "[{}] Failed to transition topic {} to  state {}, error: {}",
          _link_id,
          topic,
          model::mirror_topic_status::failed_over,
          result);
    }
    vlog(
      cllog.debug,
      "[{}] Transitioned topic {} to {}",
      _link_id,
      topic,
      model::mirror_topic_status::failed_over);
}

ss::future<>
link_status_reconciler::per_link_reconciler::reconcile_status_changes() {
    auto holder = _gate.hold();
    while (!_as.abort_requested()) {
        co_await _cv.wait([this] { return has_pending_reconciliations(); });
        vlog(cllog.trace, "[{}] Starting reconciliation iteration", _link_id);
        const auto& md = _registry.find_link_by_id(_link_id);
        if (!md) {
            continue;
        }
        // check if there are any topics still not failing over
        chunked_vector<::model::topic> pending_failover_topics;
        const auto& mirror_topics = md->state.mirror_topics;
        for (const auto& [topic, mt] : mirror_topics) {
            if (mt.status == model::mirror_topic_status::failing_over) {
                pending_failover_topics.push_back(topic);
            }
        }
        co_await ss::max_concurrent_for_each(
          pending_failover_topics, 8, [this](const auto& topic) {
              return try_finish_failover(topic);
          });
        co_await ss::sleep_abortable(reconciliation_interval, _as);
    }
}

bool link_status_reconciler::per_link_reconciler::has_pending_reconciliations()
  const {
    const auto& md = _registry.find_link_by_id(_link_id);
    if (!md) {
        // link no longer exists, will be cleaned up via notification
        return false;
    }
    const auto& mirror_topics = md->state.mirror_topics;
    for (const auto& [_, mt] : mirror_topics) {
        switch (mt.status) {
        case model::mirror_topic_status::active:
        case model::mirror_topic_status::paused:
        case model::mirror_topic_status::failed_over:
        case model::mirror_topic_status::failed:
        case model::mirror_topic_status::promoted:
            // non transitional
            break;
        case model::mirror_topic_status::failing_over:
        case model::mirror_topic_status::promoting:
            return true;
        }
    }
    return false;
}

} // namespace cluster_link
