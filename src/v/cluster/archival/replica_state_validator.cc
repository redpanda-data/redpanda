/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/archival/replica_state_validator.h"

#include "cloud_storage/types.h"
#include "cluster/archival/logger.h"
#include "model/fundamental.h"
#include "model/offset_interval.h"

namespace archival {

replica_state_validator::replica_state_validator(
  const storage::log& log, const cloud_storage::partition_manifest& manifest)
  : _log(&log)
  , _manifest(&manifest)
  , _anomalies(validate()) {}

bool replica_state_validator::has_anomalies() const noexcept {
    return _anomalies.size() > 0;
}

const chunked_vector<replica_state_anomaly>&
replica_state_validator::get_anomalies() const noexcept {
    return _anomalies;
}

void replica_state_validator::maybe_print_scarry_log_message() const {
    if (has_anomalies()) {
        for (const auto& anomaly : _anomalies) {
            vlog(
              archival_log.error,
              "[{}] anomaly detected: {}",
              _log->config().ntp(),
              anomaly.message);
        }
    }
}

chunked_vector<replica_state_anomaly> replica_state_validator::validate() {
    chunked_vector<replica_state_anomaly> result;
    if (_manifest->empty()) {
        // Nothing to validate
        return result;
    }

    // 1. Check local log start offset
    auto manifest_last = _manifest->get_last_offset();
    std::optional<model::bounded_offset_interval> manifest_interval
      = std::nullopt;
    if (_manifest->get_start_offset().has_value()) {
        manifest_interval = model::bounded_offset_interval::optional(
          _manifest->get_start_offset().value(), manifest_last);
    }
    auto local_interval = model::bounded_offset_interval::optional(
      _log->offsets().start_offset, _log->offsets().committed_offset);

    vlog(
      archival_log.debug,
      "[{}] checking start offset, local: {}, manifest: {}",
      _log->config().ntp(),
      local_interval,
      manifest_interval);

    // We have four valid cases:
    // 1. Partial overlap
    //    [manifest offset range]
    //                 [local offset range]
    // 2. Adjacent offset ranges
    //    [manifest offset range][local offset range]
    // 3. Manifest is empty (nothing is uploaded yet)
    // 4. Local storage is empty (removed by retention)
    auto can_continue_offset_range =
      [](
        std::optional<model::bounded_offset_interval> manifest_range,
        std::optional<model::bounded_offset_interval> local_offset_range) {
          if (!manifest_range.has_value() || !local_offset_range.has_value()) {
              // cases 3 and 4
              return true;
          }
          if (manifest_range.value().overlaps(local_offset_range.value())) {
              // case 1.
              return true;
          }
          if (
            model::next_offset(manifest_range.value().max())
            == local_offset_range.value().min()) {
              // case 2.
              return true;
          }
          return false;
      };
    if (!can_continue_offset_range(manifest_interval, local_interval)) {
        // There is a gap between last uploaded offset and first available
        // local offset. Progress is impossible if the metadata consistency
        // checks are on.
        result.push_back(
          replica_state_anomaly{
            .type = replica_state_anomaly_type::offsets_gap,
            .message = ssx::sformat(
              "There is a gap between the manifest {} and local storage {}",
              manifest_last,
              local_interval)});
    }

    // 2. Check offset translator state
    // Get the last uploaded segment and try to translate one of its offsets
    // and compare the results.
    auto last_segment = _manifest->last_segment();
    if (
      last_segment.has_value() && local_interval.has_value()
      && local_interval.value().contains(last_segment.value().base_offset)) {
        // Last segment exists in the manifest and can be translated using
        // local offset translation state.

        auto expected_delta = last_segment.value().delta_offset;
        auto log_delta = _log->offset_delta(last_segment.value().base_offset);

        vlog(
          archival_log.debug,
          "[{}] checking offset {} deltas, expected/log delta: {}/{}",
          _log->config().ntp(),
          last_segment->base_offset,
          expected_delta,
          log_delta);

        // Offset translation state diverged on two different replicas.
        // Previous leader translated offset differently
        if (expected_delta != log_delta) {
            result.push_back(
              replica_state_anomaly{
                .type = replica_state_anomaly_type::ot_state,
                .message = ssx::sformat(
                  "Offset translation anomaly detected for offset {}, expected "
                  "delta {}, actual delta {}, segment_meta: {}",
                  last_segment.value().base_offset,
                  expected_delta,
                  log_delta,
                  last_segment)});
        }
    } else {
        vlog(
          archival_log.debug,
          "[{}] bypassing offset delta check, last segment: {}, local "
          "interval: {} ",
          _log->config().ntp(),
          last_segment.value_or(cloud_storage::segment_meta{}),
          local_interval);
    }

    return result;
}

} // namespace archival
