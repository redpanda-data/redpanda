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

#include "cluster/types.h"
#include "cluster_link/deps.h"
#include "cluster_link/logger.h"
#include "kafka/data/rpc/deps.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

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
                *_link_registry,
                _topic_creator,
                _topic_metadata_cache,
                link_id,
                _controller_term,
                _as));
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
  kafka::data::rpc::topic_creator* topic_creator,
  kafka::data::rpc::topic_metadata_cache* topic_metadata_cache,
  model::id_t link_id,
  ::model::term_id term,
  ss::abort_source& as)
  : _registry(registry)
  , _topic_creator(topic_creator)
  , _topic_metadata_cache(topic_metadata_cache)
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
        // The transition did not land, so the topic is not actually failed
        // over. Do not fall through to promotion (which keys off the topic's
        // storage mode, not its mirror status); the next reconcile retries.
        co_return;
    }
    vlog(
      cllog.debug,
      "[{}] Transitioned topic {} to {}",
      _link_id,
      topic,
      model::mirror_topic_status::failed_over);

    // maybe_promote_storage_mode never throws (it swallows update_topic
    // failures internally), so no outer guard is needed here even though
    // try_finish_failover is noexcept.
    co_await maybe_promote_storage_mode(topic);
}

bool link_status_reconciler::per_link_reconciler::topic_needs_promotion(
  const ::model::topic& topic) const {
    const auto& md = _registry.find_link_by_id(_link_id);
    if (!md) {
        return false;
    }
    const auto& cfg = md->configuration.topic_metadata_mirroring_cfg;
    if (!cfg.promote_to_tiered_cloud_on_failover) {
        return false;
    }
    if (
      !cfg.storage_mode_override.has_value()
      || *cfg.storage_mode_override != ::model::redpanda_storage_mode::cloud) {
        return false;
    }
    auto topic_cfg = _topic_metadata_cache->find_topic_cfg(
      {::model::kafka_namespace, topic});
    if (!topic_cfg.has_value()) {
        // Topic has been deleted locally; nothing to promote.
        return false;
    }
    // Promote iff the topic is currently in cloud mode. For a topic whose
    // source is non-cloud this is exactly the set the override created as
    // cloud, honoring the create-time scope without re-reading the (mutable)
    // storage_mode_override_filters.
    //
    // WARNING / KNOWN LIMITATION: if the source topic is itself cloud-mode,
    // its shadow inherits cloud even when the override filters EXCLUDE it, so
    // it is promoted here too -- promotion is not strictly scoped to the
    // override. Strict per-topic scoping needs a persisted "override applied"
    // marker per topic; deferred for now (see the RFC "known limitations").
    return topic_cfg->properties.storage_mode
           == ::model::redpanda_storage_mode::cloud;
}

ss::future<>
link_status_reconciler::per_link_reconciler::maybe_promote_storage_mode(
  const ::model::topic& topic) {
    // After failover, promote cloud shadow topics to tiered_cloud so the
    // now-primary cluster has low-latency local reads/writes. Safe to call
    // repeatedly — early returns if the topic no longer needs promotion.
    if (!topic_needs_promotion(topic)) {
        co_return;
    }
    // No tiered_cloud_topics feature-active guard here: this feature cannot be
    // exercised before the cluster is upgraded past the release that ships
    // tiered_cloud, so the feature is active by construction whenever we reach
    // promotion.
    cluster::topic_properties_update update({::model::kafka_namespace, topic});
    update.properties.storage_mode.op
      = cluster::incremental_update_operation::set;
    update.properties.storage_mode.value
      = ::model::redpanda_storage_mode::tiered_cloud;
    // Use as_future so this method is safe to call from any context, including
    // noexcept callers like try_finish_failover, without an explicit throw. The
    // reconciler loop retries on the next iteration.
    auto fut = co_await ss::coroutine::as_future(
      _topic_creator->update_topic(std::move(update)));
    if (fut.failed()) {
        auto eptr = fut.get_exception();
        vlog(
          cllog.warn,
          "[{}] Exception while promoting storage mode for topic {}: {}",
          _link_id,
          topic,
          eptr);
        co_return;
    }
    auto result = fut.get();
    if (result != cluster::errc::success) {
        // The reconciliation loop retries on each tick, so this is not
        // permanently stuck. Log at warn — if it persists the operator will
        // see repeated warnings.
        vlog(
          cllog.warn,
          "[{}] Failed to promote storage mode for topic {} to tiered_v2: "
          "{}. Will retry.",
          _link_id,
          topic,
          result);
    } else {
        vlog(
          cllog.info,
          "[{}] Promoted topic {} storage mode from cloud to tiered_v2",
          _link_id,
          topic);
    }
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
        // Separate per-status buckets: failing_over topics need the finish-
        // failover workflow, failed_over topics may still need post-failover
        // storage mode promotion (retry path for cases where the immediate
        // promotion after the state transition failed transiently).
        chunked_vector<::model::topic> pending_failover_topics;
        chunked_vector<::model::topic> pending_promotion_topics;
        const auto& mirror_topics = md->state.mirror_topics;
        for (const auto& [topic, mt] : mirror_topics) {
            switch (mt.status) {
            case model::mirror_topic_status::failing_over:
                pending_failover_topics.push_back(topic);
                break;
            case model::mirror_topic_status::failed_over:
                if (topic_needs_promotion(topic)) {
                    pending_promotion_topics.push_back(topic);
                }
                break;
            default:
                break;
            }
        }
        co_await ss::max_concurrent_for_each(
          pending_failover_topics, 8, [this](const auto& topic) {
              return try_finish_failover(topic);
          });
        co_await ss::max_concurrent_for_each(
          pending_promotion_topics, 8, [this](const auto& topic) {
              return maybe_promote_storage_mode(topic);
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
    for (const auto& [topic, mt] : mirror_topics) {
        switch (mt.status) {
        case model::mirror_topic_status::active:
        case model::mirror_topic_status::paused:
        case model::mirror_topic_status::failed:
        case model::mirror_topic_status::promoted:
            // non transitional
            break;
        case model::mirror_topic_status::failed_over:
            if (topic_needs_promotion(topic)) {
                return true;
            }
            break;
        case model::mirror_topic_status::failing_over:
        case model::mirror_topic_status::promoting:
            return true;
        }
    }
    return false;
}

} // namespace cluster_link
