/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"

#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/reactor.hh>

class priority_manager {
public:
    ss::io_priority_class raft_priority() { return _raft_priority; }
    ss::io_priority_class controller_priority() { return _controller_priority; }
    ss::io_priority_class kafka_read_priority() { return _kafka_read_priority; }
    ss::io_priority_class wasm_read_priority() { return _wasm_read_priority; }
    ss::io_priority_class compaction_priority() { return _compaction_priority; }
    ss::io_priority_class raft_learner_recovery_priority() {
        return _raft_learner_recovery_priority;
    }
    ss::io_priority_class shadow_indexing_priority() {
        return _shadow_indexing_priority;
    }
    ss::io_priority_class archival_priority() { return _archival_priority; }

    static priority_manager& local() {
        static thread_local priority_manager pm = priority_manager();
        return pm;
    }

private:
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
    priority_manager()
      : _raft_priority(ss::io_priority_class::register_one("raft", 1000))
      , _controller_priority(
          ss::io_priority_class::register_one("controller", 1000))
      , _kafka_read_priority(
          ss::io_priority_class::register_one("kafka_read", 1000))
      , _wasm_read_priority(
          ss::io_priority_class::register_one("wasm_read", 500))
      , _compaction_priority(
          ss::io_priority_class::register_one("compaction", 200))
      , _raft_learner_recovery_priority(
          ss::io_priority_class::register_one("raft-learner-recovery", 100))
      // User reads via Tiered Storage.  Lower priority than raft writes, to
      // mitigate promotion to tiered storage cache starving out producers.
      , _shadow_indexing_priority(
          ss::io_priority_class::register_one("shadow-indexing", 500))
      // Background uploads to tiered storage: not user-visible latency, lowest
      // priority.
      , _archival_priority(
          ss::io_priority_class::register_one("archival", 200)) {}
#pragma clang diagnostic pop

    ss::io_priority_class _raft_priority;
    ss::io_priority_class _controller_priority;
    ss::io_priority_class _kafka_read_priority;
    ss::io_priority_class _wasm_read_priority;
    ss::io_priority_class _compaction_priority;
    ss::io_priority_class _raft_learner_recovery_priority;
    ss::io_priority_class _shadow_indexing_priority;
    ss::io_priority_class _archival_priority;
};

inline ss::io_priority_class raft_priority() {
    return priority_manager::local().raft_priority();
}

inline ss::io_priority_class controller_priority() {
    return priority_manager::local().controller_priority();
}

inline ss::io_priority_class kafka_read_priority() {
    return priority_manager::local().kafka_read_priority();
}

inline ss::io_priority_class wasm_read_priority() {
    return priority_manager::local().wasm_read_priority();
}

inline ss::io_priority_class compaction_priority() {
    return priority_manager::local().compaction_priority();
}

inline ss::io_priority_class raft_learner_recovery_priority() {
    return priority_manager::local().raft_learner_recovery_priority();
}

inline ss::io_priority_class shadow_indexing_priority() {
    return priority_manager::local().shadow_indexing_priority();
}

inline ss::io_priority_class archival_priority() {
    return priority_manager::local().archival_priority();
}
