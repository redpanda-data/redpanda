/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/compaction/meta.h"
#include "ssx/semaphore.h"

namespace cloud_topics::l1 {

class committing_policy {
public:
    committing_policy() = default;
    committing_policy(const committing_policy&) = delete;
    committing_policy& operator=(const committing_policy&) = delete;
    committing_policy(committing_policy&& other) noexcept = default;
    committing_policy& operator=(committing_policy&&) noexcept = default;
    virtual ~committing_policy() = default;

    // Invoked when the `compaction_committer` recieves an update.
    virtual void on_update(const object_output_t&, ssx::semaphore&) = 0;

    // Invoked when the `compaction_committer` is ready to potentially commit
    // some updates.
    virtual bool should_commit() const = 0;
};

// Policy to commit updates as soon as they arrive (alerts on every update). No
// batching implemented.
class commit_on_update_policy : public committing_policy {
public:
    void on_update(const object_output_t&, ssx::semaphore&) final;

    bool should_commit() const final;
};

std::unique_ptr<committing_policy> make_default_committing_policy();

} // namespace cloud_topics::l1
