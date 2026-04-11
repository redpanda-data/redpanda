/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/housekeeper/housekeeper.h"

#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/logger.h"
#include "model/timestamp.h"
#include "random/simple_time_jitter.h"
#include "ssx/future-util.h"

#include <chrono>
#include <exception>

using namespace std::chrono_literals;

namespace cloud_topics {

housekeeper::housekeeper(
  model::topic_id_partition tidp,
  l0_metadata_storage* l0_metastore,
  l1::metastore* l1_metastore,
  retention_configuration* config,
  config::binding<std::chrono::milliseconds> loop_interval)
  : _tidp(tidp)
  , _l0_metastore(l0_metastore)
  , _l1_metastore(l1_metastore)
  , _config(config)
  , _loop_interval(std::move(loop_interval)) {}

ss::future<> housekeeper::start() {
    _gate = {};
    _as = {};
    ssx::repeat_until_gate_closed_or_aborted(
      _gate, _as, [this]() { return do_loop(); });
    return ss::now();
}

ss::future<> housekeeper::stop() {
    _as.request_abort();
    co_await _gate.close();
}

ss::future<> housekeeper::do_housekeeping() {
    kafka::offset new_start_offset = kafka::offset::min();
    if (auto retention_bytes = _config->retention_bytes(_tidp)) {
        new_start_offset = co_await do_bytes_retention(*retention_bytes);
    }
    if (auto retention_duration = _config->retention_duration(_tidp)) {
        auto offset = co_await do_time_retention(*retention_duration);
        new_start_offset = std::max(new_start_offset, offset);
    }
    auto max_allowed_start_offset = _l0_metastore->get_max_allowed_start_offset(
      _tidp);
    if (max_allowed_start_offset < new_start_offset) {
        vlog(
          cd_log.trace,
          "{} - Pinning requested new start offset {} by max allowed start "
          "offset {}",
          _tidp,
          new_start_offset,
          max_allowed_start_offset);
        new_start_offset = max_allowed_start_offset;
    }
    if (new_start_offset != kafka::offset::min()) {
        co_await _l0_metastore->set_start_offset(_tidp, new_start_offset, &_as);
    }
    // Sync the start offset back to the L1 metastore.
    // DeleteRecords may advance the L0 start offset past the L1 start offset.
    co_await sync_start_offset();
}

ss::future<> housekeeper::do_bump_epoch() {
    auto curr_partition_epoch = _l0_metastore->estimate_inactive_epoch(_tidp);

    if (curr_partition_epoch != _last_epoch) {
        vlog(
          cd_log.debug,
          "{}: Epoch made progress ({} -> {}), nothing to do.",
          _tidp,
          _last_epoch,
          curr_partition_epoch);
        _last_epoch = curr_partition_epoch;
        co_return;
    }

    vlog(
      cd_log.debug,
      "{}: Partition idle since last housekeeping interval, "
      "force the epoch to advance.",
      _tidp);

    auto new_epoch = co_await _l0_metastore->get_current_cluster_epoch(
      _tidp, &_as);
    if (!new_epoch.has_value()) {
        co_return;
    }
    vlog(
      cd_log.debug,
      "{}: Advance epoch: {} -> {}",
      _tidp,
      curr_partition_epoch,
      new_epoch);
    co_await _l0_metastore->advance_epoch(_tidp, new_epoch.value(), &_as);
    co_await _l0_metastore->sync_to_next_placeholder(_tidp, &_as);
}

ss::future<> housekeeper::do_loop() {
    simple_time_jitter<ss::lowres_clock> jitter(_loop_interval());
    co_await ss::sleep_abortable<ss::lowres_clock>(jitter.next_duration(), _as);
    try {
        co_await do_housekeeping();
        co_await do_bump_epoch();
    } catch (...) {
        auto ex = std::current_exception();
        vlogl(
          cd_log,
          ssx::is_shutdown_exception(ex) ? ss::log_level::debug
                                         : ss::log_level::error,
          "error running housekeeping loop: {}",
          ex);
    }
}

namespace {
void handle_error(l1::metastore::errc ec) {
    switch (ec) {
    case l1::metastore::errc::missing_ntp:
        // Likely no data in L1 yet
    case l1::metastore::errc::out_of_range:
        // Less than `size` or `duration` data in L1
        break;
    case l1::metastore::errc::invalid_request:
    case l1::metastore::errc::transport_error:
        // Warn but retry next loop.
        vlog(cd_log.warn, "unable to reach the metastore: {}", ec);
        break;
    }
}

} // namespace

ss::future<kafka::offset> housekeeper::do_bytes_retention(size_t size) {
    auto result = co_await _l1_metastore->get_first_offset_for_bytes(
      _tidp, size);
    if (!result.has_value()) {
        handle_error(result.error());
        co_return kafka::offset::min();
    }
    co_return result.value();
}

ss::future<kafka::offset>
housekeeper::do_time_retention(std::chrono::milliseconds duration) {
    // It's important that we get the offsets before the timequery, as the
    // data could change after the timequery, and this way we ensure we
    // don't delete all the data.
    auto offsets_result = co_await _l1_metastore->get_offsets(_tidp);
    if (!offsets_result.has_value()) {
        handle_error(offsets_result.error());
        co_return kafka::offset::min();
    }
    auto retention_point = model::timestamp_clock::now() - duration;
    auto result = co_await _l1_metastore->get_first_ge(
      _tidp,
      offsets_result->start_offset,
      model::to_timestamp(retention_point));
    if (result.has_value()) {
        auto object = result.value();
        co_return object.first_offset;
    }
    if (result.error() != l1::metastore::errc::out_of_range) {
        handle_error(result.error());
        co_return kafka::offset::min();
    }
    auto next_offset = offsets_result.value().next_offset;
    co_return next_offset;
}

ss::future<> housekeeper::sync_start_offset() {
    if (_l0_metastore->get_last_reconciled_offset(_tidp) == kafka::offset{}) {
        // Nothing reconciled, L1 is empty and there's nothing to sync.
        co_return;
    }
    auto start_offset = _l0_metastore->get_start_offset(_tidp);
    if (_last_synced_start_offset == start_offset) {
        co_return;
    }
    vlog(cd_log.debug, "Setting {} start offset to {}", _tidp, start_offset);
    auto result = co_await _l1_metastore->set_start_offset(_tidp, start_offset);
    if (!result.has_value()) {
        vlog(
          cd_log.warn,
          "Failed to sync start offset to L1 for {}: {}",
          _tidp,
          result.error());
        co_return;
    }
    _last_synced_start_offset = start_offset;
}

} // namespace cloud_topics
