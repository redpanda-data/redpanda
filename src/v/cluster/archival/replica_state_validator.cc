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

#include "cluster/archival/logger.h"

namespace archival {

replica_state_validator::replica_state_validator(const cluster::partition& p)
  : _partition(p)
  , _anomalies(validate()) {}

bool replica_state_validator::has_anomalies() const noexcept {
    return _anomalies.size() > 0;
}

const std::deque<replica_state_anomaly>
replica_state_validator::get_anomalies() const noexcept {
    return _anomalies;
}

void replica_state_validator::maybe_print_scarry_log_message() const {
    if (has_anomalies()) {
        for (const auto& anomalie : _anomalies) {
            vlog(
              archival_log.error,
              "[{}] anomaly detected: {}",
              _partition.get_ntp_config().ntp(),
              anomalie.message);
        }
    }
}

std::deque<replica_state_anomaly> replica_state_validator::validate() {
    std::deque<replica_state_anomaly> result;
    // Before we start uploading we need to make sure that some invariants
    // are met.
    // We're validating the replica state over previously uploaded metadata.
    // The idea is that we can gradually build confidence by making incremental
    // checks. This code also defines manual intervention point. Once the
    // problem is detected the NTP archiver shouldn't do anything other than
    // reporting the problem periodically and asking for intervention. After
    // examining the anomaly the operator might decide to disable the checks and
    // allow the uploads to continue. This is an improvement over the situation
    // when the uploads are getting stuck because we're not doing any extra
    // work. It may also be easier to diagnose the problem if the archiver is
    // not running. Potentially, we can do some automatic mitigations
    // (transferring leadership or blocking writes).
    const auto& manifest = _partition.archival_meta_stm()->manifest();
    if (manifest.empty()) {
        // Nothing to validate
        return result;
    }

    // 1. Check local log start offset
    auto manifest_next = model::next_offset(manifest.get_last_offset());
    auto local_so = _partition.log()->offsets().start_offset;
    if (manifest_next < local_so) {
        // There is a gap between last uploaded offset and first available
        // local offset. Progress is impossible if the metadata consistency
        // checks are on.
        result.push_back(replica_state_anomaly{
          .type = replica_state_anomaly_type::offsets_gap,
          .message = ssx::sformat(
            "There is a {} to {} gap between the manifest and local storage",
            manifest_next,
            local_so)});
    }

    // 2. Check offset translator state
    // Get the last uploaded segment and try to translate one of its offsets
    // and compare the results.
    auto last_segment = manifest.last_segment();
    if (last_segment.has_value() && local_so < last_segment->base_offset) {
        // Last segment exists in the manifest and can be translated using
        // local offset translation state.

        auto expected_delta = last_segment->delta_offset;
        auto actual_delta = _partition.log()->offset_delta(
          last_segment->base_offset);

        // Offset translation state diverged on two different replicas.
        // Previous leader translated offset differently
        if (expected_delta != actual_delta) {
            result.push_back(replica_state_anomaly{
              .type = replica_state_anomaly_type::ot_state,
              .message = ssx::sformat(
                "Offset translation anomaly detected for offset {}, expected "
                "delta {}, actual delta {}, segment_meta",
                last_segment->base_offset,
                expected_delta,
                actual_delta,
                last_segment)});
        }
    }

    return result;
}

} // namespace archival
