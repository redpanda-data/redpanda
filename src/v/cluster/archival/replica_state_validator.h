/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster/fwd.h"
#include "cluster/partition.h"

#include <seastar/core/shared_ptr.hh>

namespace archival {

enum class replica_state_anomaly_type {
    // Gap between the manifest and the local storage
    offsets_gap,

    // Inconsistent offset-translator state
    ot_state,
};

struct replica_state_anomaly {
    replica_state_anomaly_type type;
    ss::sstring message;
};

/// This class is used to validate partition replica state
/// over cluster replicas. If the archiver starts on a replica
/// with inconsistent offset translator state it should be able
/// to detect this and report the problem.
class replica_state_validator {
public:
    explicit replica_state_validator(const cluster::partition&);

    bool has_anomalies() const noexcept;

    const std::deque<replica_state_anomaly> get_anomalies() const noexcept;

    void maybe_print_scarry_log_message() const;

private:
    /// Run validations.
    /// Return set of detected anomalies.
    /// Throw if we failed to perform validation process.
    std::deque<replica_state_anomaly> validate();

    const cluster::partition& _partition;
    std::deque<replica_state_anomaly> _anomalies;
};

} // namespace archival
