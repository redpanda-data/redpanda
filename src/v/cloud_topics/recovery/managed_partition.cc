// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/recovery/managed_partition.h"

#include "cluster/partition.h"
#include "model/fundamental.h"

namespace experimental::cloud_topics::recovery {

managed_partition::~managed_partition() = default;

std::ostream& operator<<(std::ostream& o, managed_partition::state s) {
    switch (s) {
    case managed_partition::state::passive:
        return o << "passive";
    case managed_partition::state::active:
        return o << "active";
    case managed_partition::state::recovering:
        return o << "recovering";
    case managed_partition::state::detaching:
        return o << "detaching";
    }
}

void managed_partition::switch_to_active(model::term_id active_term) {
    vassert(
      current_state == state::passive || current_state == state::recovering,
      "Invalid state transition from {} to active",
      current_state);

    this->active_term = active_term;
    current_state = state::active;
}

void managed_partition::switch_to_recover(model::term_id active_term) {
    vassert(
      current_state == state::passive,
      "Invalid state transition from {} to recovering",
      current_state);

    this->active_term = active_term;
    current_state = state::recovering;
}

} // namespace experimental::cloud_topics::recovery
