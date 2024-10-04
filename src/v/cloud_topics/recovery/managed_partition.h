// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "model/fundamental.h"

namespace cluster {
class partition;
}

namespace experimental::cloud_topics::recovery {

class managed_partition {
public:
    explicit managed_partition(
      model::initial_revision_id initial_rev,
      ss::lw_shared_ptr<cluster::partition> p)
      : initial_rev(initial_rev)
      , partition(std::move(p)) {}

    managed_partition(const managed_partition&) = delete;
    managed_partition& operator=(const managed_partition&) = delete;

    managed_partition(managed_partition&&) noexcept = default;
    managed_partition& operator=(managed_partition&&) noexcept = default;
    ~managed_partition();

    void switch_to_active(model::term_id active_term);
    void switch_to_recover(model::term_id active_term);

    model::initial_revision_id initial_rev;
    ss::lw_shared_ptr<cluster::partition> partition;

    // The term when we became the leader of this partition. It is set when
    // we transition to the active state and never changes. During leadership
    // changes we re-create the managed_partition instance.
    model::term_id active_term = model::term_id{};

    enum class state {
        /// We are not the leader of this partition. Just subscribe to
        /// notifications and wait in case we become the leader.
        passive,
        /// We are the leader and are responsible for taking snapshots.
        active,
        /// We are the leader and are responsible for recovering the
        /// partition.
        recovering,
        /// Detaching and aborting any in-progress operations. In this state
        /// after losing leadership or shutting down.
        detaching,
    };

    friend std::ostream&
    operator<<(std::ostream&, const managed_partition::state);

    state current_state = state::passive;

    // The next time we should take a snapshot of this partition.
    ss::lowres_clock::time_point next_schedule_deadline;

    // Abort source for when we don't manage the partition anymore.
    ss::abort_source as;
};

}; // namespace experimental::cloud_topics::recovery
