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

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/compaction/committer.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/source.h"

namespace cloud_topics::l1 {

class compaction_worker {
public:
    compaction_worker(io*, compaction_committer*);

    // Requests a compaction of the provided `log`.
    ss::future<> compact(log_info_and_meta, ss::abort_source&);

    // Sets `_state = compaction_job_state::stopped` if the passed
    // `expected_ntp == _ntp`. It is up to users/currently running compaction
    // jobs to respect this flag.
    void request_stop_compact(model::ntp);

    // Sets `_stopped` flag to indicate the `worker` will not perform any more
    // compaction jobs, as well as `_state = compaction_job_state::stopped` to
    // indicate to a potential inflight compaction job that it should exit
    // early.
    void set_stopped();

private:
    // Specifies which `ntp` is currently undergoing compaction on this
    // worker. Set if `_state == compaction_job_state::running`.
    std::optional<model::ntp> _ntp{std::nullopt};

    // The state of the worker (`idle`, `running`, or `stopped`). `stopped`
    // means that the inflight compaction job running on this worker has been
    // pre-empted to return early- it does not mean that the worker itself is
    // stopped from running future compaction jobs (`_stopped` is used as a flag
    // to indicate this state instead).
    compaction_job_state _state{compaction_job_state::idle};

    // If `true`, new compaction jobs are automatically rejected (shutdown has
    // likely been requested).
    bool _stopped{false};

    io* _io;
    compaction_committer* _committer;
};

} // namespace cloud_topics::l1
