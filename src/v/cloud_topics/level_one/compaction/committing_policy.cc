/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/committing_policy.h"

namespace cloud_topics::l1 {

committing_policy::update_response
commit_on_update_policy::on_update(const object_output_t&) {
    return update_response::preempt;
}

bool commit_on_update_policy::should_commit() const { return true; }

std::unique_ptr<committing_policy> make_default_committing_policy() {
    return std::make_unique<commit_on_update_policy>();
}

} // namespace cloud_topics::l1
