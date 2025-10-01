/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster_link/replication/deps_impl.h"

#include "cluster/partition.h"
#include "cluster/partition_manager.h"
#include "cluster_link/logger.h"
#include "cluster_link/replication/mux_remote_consumer.h"
#include "kafka/server/write_at_offset_stm.h"
#include "ssx/future-util.h"

static constexpr auto sync_timeout = 10s;

namespace cluster_link::replication {

remote_data_source_factory::remote_data_source_factory(
  std::unique_ptr<mux_remote_consumer> consumer)
  : _consumer(std::move(consumer)) {}

// to keep the unique_ptr<mux_remote_consumer> fwd declaration happy
remote_data_source_factory::~remote_data_source_factory() = default;

ss::future<> remote_data_source_factory::start() { return _consumer->start(); }

ss::future<> remote_data_source_factory::stop() noexcept {
    return _consumer->stop();
}

std::unique_ptr<data_source> remote_data_source_factory::make_source(
  const ::model::ntp& ntp, ::model::timestamp starting_offset) {
    return std::make_unique<remote_partition_source>(
      ntp.tp, *_consumer, starting_offset);
}

ss::future<> remote_partition_source::start(kafka::offset offset) {
    vlog(
      cllog.trace,
      "[{}] Starting remote partition source at offset {}",
      _tp,
      offset);
    if (offset == kafka::offset{0}) {
        vlog(
          cllog.debug,
          "[{}] Starting with empty partition, issuing ListOffsets with "
          "timestamp {}",
          _tp,
          _starting_offset);
        offset = co_await fetch_starting_offset();
        vlog(cllog.debug, "[{}] ListOffsets returned offset {}", _tp, offset);
    }
    auto result = _consumer.add(_tp, offset);
    if (!result.has_value()) [[unlikely]] {
        // this is usually indicative of a bug in the manager where
        // a previous source is not deregistered, bubble it up.
        auto err = result.error();
        vlog(
          cllog.error,
          "[{}] Failed to add remote partition source: {}",
          _tp,
          err);
        throw std::runtime_error(
          fmt::format(
            "[{}] Failed to add remote partition source: {}", _tp, err));
    }
}

ss::future<> remote_partition_source::stop() noexcept {
    vlog(cllog.trace, "[{}] Stopping remote partition source", _tp);
    _as.abort_requested();
    auto f = _gate.close();
    co_await _consumer.remove(_tp);
    co_await std::move(f);
}

ss::future<> remote_partition_source::reset(kafka::offset offset) {
    _gate.check();
    auto result = _consumer.reset(_tp, offset);
    if (!result.has_value()) [[unlikely]] {
        auto err = result.error();
        vlog(
          cllog.error,
          "[{}] Failed to reset remote partition source: {}",
          _tp,
          err);
        return ss::make_exception_future<>(err);
    }
    return ss::now();
}

ss::future<data_source::data>
remote_partition_source::fetch_next(ss::abort_source& as) {
    auto holder = _gate.hold();
    auto result = co_await _consumer.fetch(_tp, as);
    if (!result.has_value()) [[unlikely]] {
        auto err = result.error();
        vlog(
          cllog.error,
          "[{}] Failed to fetch from remote partition source: {}",
          _tp,
          result.error());
        throw std::runtime_error(
          fmt::format(
            "[{}] Failed to fetch from remote partition source: {}", _tp, err));
    }
    auto [batches, units] = std::move(*result);
    co_return data_source::data{
      .batches = std::move(batches), .units = std::move(units)};
}

kafka::offset remote_partition_source::last_seen_log_start_offset() {
    auto lso = _consumer.last_seen_log_start_offset(_tp);
    vassert(lso.has_value(), "Partition {} must exist", _tp);
    return *lso;
}

ss::future<kafka::offset> remote_partition_source::fetch_starting_offset() {
    // This loop will run forever until the _gate is closed
    // If a user has specified a specific starting offset, then we will wait
    // until that offset is available on the remote cluster and then return it
    static constexpr auto backoff_delay = 250ms;
    while (!_gate.is_closed()) {
        try {
            auto resp = co_await do_list_offset();
            if (resp.error_code != kafka::error_code::none) {
                throw std::runtime_error(
                  fmt::format(
                    "[{}] ListOffsets returned error: {}",
                    _tp,
                    resp.error_code));
            }
            vlog(
              cllog.debug, "[{}] Fetched starting offset {}", _tp, resp.offset);
            co_return ::model::offset_cast(resp.offset);
        } catch (const std::exception& e) {
            vlog(cllog.warn, "[{}] ListOffsets attempt failed: {}", _tp, e);
        }
        co_await ss::sleep_abortable(backoff_delay, _as);
    }
    // Perform a gate check here, in case it closed then it will throw an
    // exception
    _gate.check();
    // We should have only exited the above while loop if the gate has closed,
    // meaning _gate.check() should have thrown a gate closed exception
    __builtin_unreachable();
}

ss::future<kafka::list_offset_partition_response>
remote_partition_source::do_list_offset() {
    auto version = co_await get_list_offset_api_version();
    if (!version) {
        throw std::runtime_error(
          fmt::format("[{}] ListOffsets not supported by remote cluster", _tp));
    }
    const auto leader_and_epoch = co_await get_leader_and_epoch_for_ntp();
    if (!leader_and_epoch) {
        throw std::runtime_error(
          fmt::format(
            "[{}] No leader found.  Unable to fetch starting offset", _tp));
    }
    auto [leader_id, leader_epoch] = *leader_and_epoch;
    kafka::list_offsets_request req;
    req.data.replica_id = ::model::node_id{-1}; // normal consumer
    req.data.isolation_level = 1;               // read committed only
    req.data.topics.emplace_back(
      kafka::list_offset_topic{
        .name = _tp.topic,
        .partitions = {{
          .partition_index = _tp.partition,
          .current_leader_epoch = leader_epoch,
          .timestamp = _starting_offset,
        }},
      });

    auto resp = co_await _consumer.cluster().dispatch_to(
      leader_id, std::move(req), *version);

    if (resp.data.topics.empty()) {
        throw std::runtime_error(
          fmt::format("[{}] No topics in ListOffsets response", _tp));
    }

    auto& topic = resp.data.topics[0];
    if (topic.name != _tp.topic) {
        throw std::runtime_error(
          fmt::format(
            "[{}] Topic name mismatch in ListOffsets response: {}",
            _tp,
            topic.name));
    }
    if (topic.partitions.empty()) {
        throw std::runtime_error(
          fmt::format("[{}] No partitions in ListOffsets response", _tp));
    }
    auto& partition = topic.partitions[0];
    if (partition.partition_index != _tp.partition) {
        throw std::runtime_error(
          fmt::format(
            "[{}] Partition index mismatch in ListOffsets response: {}",
            _tp,
            partition.partition_index));
    }

    co_return std::move(partition);
}

ss::future<std::optional<std::tuple<::model::node_id, kafka::leader_epoch>>>
remote_partition_source::get_leader_and_epoch_for_ntp() {
    auto leader_and_epoch = do_get_leader_and_epoch_for_ntp();
    if (leader_and_epoch) {
        co_return leader_and_epoch;
    }
    vlog(cllog.debug, "[{}] NTP has no leader, refreshing metadata", _tp);
    // refresh metadata and try again
    co_await _consumer.cluster().request_metadata_update();
    co_return do_get_leader_and_epoch_for_ntp();
}

std::optional<std::tuple<::model::node_id, kafka::leader_epoch>>
remote_partition_source::do_get_leader_and_epoch_for_ntp() {
    const auto& topics = _consumer.cluster().get_topics().cache();
    auto it = topics.find(_tp.topic);
    if (it == topics.end()) {
        vlog(cllog.debug, "[{}] Topic not found in metadata", _tp);
        return std::nullopt;
    }
    auto pit = it->second.partitions.find(_tp.partition);
    if (pit == it->second.partitions.end()) {
        vlog(cllog.debug, "[{}] Partition not found in metadata", _tp);
        return std::nullopt;
    }
    return std::make_tuple(pit->second.leader, pit->second.leader_epoch);
}

ss::future<std::optional<kafka::api_version>>
remote_partition_source::get_list_offset_api_version() {
    auto supported_versions
      = co_await _consumer.cluster().supported_api_versions(
        kafka::list_offsets_api::key);
    if (!supported_versions) {
        co_return std::nullopt;
    }

    if (supported_versions.value().min > kafka::list_offsets_api::max_valid) {
        co_return std::nullopt;
    }

    co_return std::min(
      supported_versions.value().max, kafka::list_offsets_api::max_valid);
}

std::unique_ptr<data_sink>
local_partition_data_sink_factory::make_sink(const ::model::ntp& ntp) {
    auto partition = _partition_manager.local().get(ntp);
    if (!partition) {
        throw std::runtime_error(
          fmt::format("Partition not found: {} on this shard", ntp));
    }
    return std::make_unique<local_partition_sink>(std::move(partition));
}

local_partition_sink::local_partition_sink(
  ss::lw_shared_ptr<cluster::partition> partition)
  : _partition(std::move(partition))
  , _stm(_partition->raft()->stm_manager()->get<kafka::write_at_offset_stm>()) {
    vassert(
      _stm,
      "write_at_offset_stm not attached to partition {}",
      _partition->ntp());
}
ss::future<> local_partition_sink::start() {
    auto holder = _gate.hold();
    auto sync_offset = co_await _stm->get_expected_last_offset(sync_timeout);
    if (sync_offset.has_error()) {
        throw std::runtime_error(
          fmt::format(
            "Failed to sync write_at_offset_stm for partition {}: {}",
            _partition->ntp(),
            sync_offset.error().message()));
    }
    vlog(
      cllog.trace,
      "[{}] Starting local partition sink at offset {}",
      _partition->ntp(),
      sync_offset.value());
    _last_replicated_offset = sync_offset.value();
}

ss::future<> local_partition_sink::stop() noexcept {
    vlog(cllog.trace, "[{}] Stopping local partition sink", _partition->ntp());
    co_await _gate.close();
}

kafka::offset local_partition_sink::last_replicated_offset() const {
    vassert(_last_replicated_offset, "Sink has not been started");
    return _last_replicated_offset.value();
}

raft::replicate_stages local_partition_sink::replicate(
  chunked_vector<::model::record_batch> batches,
  ::model::timeout_clock::duration timeout,
  ss::abort_source& as) {
    _gate.check();
    vassert(_last_replicated_offset, "Sink has not been started");
    vassert(
      !batches.empty(),
      "Cannot replicate empty batch vector {}",
      _partition->ntp());
    chunked_vector<kafka::offset> expected_offsets;
    expected_offsets.reserve(batches.size());
    for (const auto& batch : batches) {
        expected_offsets.push_back(::model::offset_cast(batch.base_offset()));
    }
    auto new_last_replicated_begin = ::model::offset_cast(
      batches.front().base_offset());
    auto new_last_replicated_end = ::model::offset_cast(
      batches.back().last_offset());
    vassert(
      new_last_replicated_begin > _last_replicated_offset
        && new_last_replicated_end > _last_replicated_offset,
      "[{}] Replicating offsets must be monotonically increasing last "
      "replicated: {}, attempting to replicate: [{}, {}]",
      _partition->ntp(),
      _last_replicated_offset,
      new_last_replicated_begin,
      new_last_replicated_end);
    vlog(
      cllog.trace,
      "[{}] Replicating batches in range [{} - {}], last_replicated: {}, "
      "new_last_replicated: {}",
      _partition->ntp(),
      batches.front().header(),
      batches.back().header(),
      _last_replicated_offset,
      new_last_replicated_end);
    auto stages = _stm->replicate(
      std::move(batches),
      std::move(expected_offsets),
      _last_replicated_offset,
      timeout,
      as);
    _last_replicated_offset = new_last_replicated_end;
    return stages;
}

void local_partition_sink::notify_replicator_failure(::model::term_id term) {
    if (_gate.is_closed()) {
        return;
    }
    // If the replicator failed to start _and_ the partition is still the
    // leader in the same term we are effectively stuck without a replicator.
    // Here we step down to ensure a new leader comes up and a replicator start
    // is triggered again on the new leader.
    if (_partition->term() == term) {
        ssx::spawn_with_gate(_gate, [this, term] {
            return _partition->raft()->step_down(
              fmt::format("Unable to start replicator in term: {}", term));
        });
    }
}
} // namespace cluster_link::replication
