/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/fwd.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/state_machine.h"
#include "utils/expiring_promise.h"
#include "utils/mutex.h"

#include <absl/container/flat_hash_map.h>

namespace config {
struct configuration;
}

namespace cluster {

// id_allocator is a service to generate cluster-wide unique id (int64)
// It is based on a state machine running on top of a raft log. For each
// request (*) it adds "+1" command to the log, reads the log up
// to this command (identified by a sequence_id), sums all the read
// commands and returns a result.
//
// (*) When we do this procedure for each request it becomes costly
// in terms of latency so the id_allocator_stm allocates the ids in
// batches using "+1000" commands (the actual value comes from the
// configuration see id_allocator_batch_size) and then serves the
// requests without touching the log until the batch is exhausted.
//
// sequence_id is 3-tuple, first component is a node-unique id generated
// on the app start (we use kvstore to persist the value and guarantee
// monotonicity / uniqueness), second component is a node id and the
// last component is persisted in RAM. All the 3 components guarantees
// uniqueness even if the node is restarted (last component is reset but
// the first is incremented).
//
// Log garbage collection. If we do nothing all the "+1000" commands will
// eventually consume all the disk. To prevent this from happening
// we use the following snapshotting scheme:
//
//   1. once the log size passes a threshold a leader writes
//      "prepare_truncation" message to mark a place in the log
//      where we want to cut off the history
//
//   2. when the leader catches up with the "prepare_truncation"
//      it knows the current state (sum of all the messages before
//      the mark) and the prepare_truncation's  so it writes
//      "execute_truncation(state, offset)"
//
//   3. after the leader catches up with the execute_truncation
//      command it truncates the local log before the mark by
//      calling raft::write_snapshot
//
//   4. after a follower catches up with the execute_truncation
//      it truncates its log
//
// There may be regular "+1000" commands between the "prepare" and the
// "execute" commands. When this happen the state_machine postpone the
// execution of the "+1000" commands until it meets the execute_truncation
// and use its state as a base for the increments.
// A leader starts acting before it reads up to the current moment
// so there may be a situation when a leader issues several
// "prepare_truncation" before it issues "execute_truncation" in this
// case the first "prepare_truncation" in the log order wins. The same
// works with "execute_truncation" the first wins.

class id_allocator_stm final : public raft::state_machine {
public:
    struct stm_allocation_result {
        int64_t id;
        raft::errc raft_status{raft::errc::success};
    };

    explicit id_allocator_stm(
      ss::logger&, raft::consensus*, config::configuration&);

    ss::future<> start() final;

    ss::future<stm_allocation_result>
    allocate_id_and_wait(model::timeout_clock::time_point timeout);

private:
    struct sequence_id {
        model::run_id run_id;
        raft::vnode node_id;
        int64_t tick;

        bool operator==(const sequence_id& other) const = default;

        template<typename H>
        friend H AbslHashValue(H h, const sequence_id& seq) {
            return H::combine(std::move(h), seq.run_id, seq.node_id, seq.tick);
        }
    };

    struct log_allocation_result {
        sequence_id seq;
        int64_t base;
        int64_t range;
        raft::errc raft_status{raft::errc::success};
        model::offset offset;
    };

    struct allocation_cmd {
        static constexpr uint8_t record_key = 0;
        sequence_id seq;
        int64_t range;
    };

    struct prepare_truncation_cmd {
        static constexpr uint8_t record_key = 1;
        sequence_id seq;
    };

    struct execute_truncation_cmd {
        static constexpr uint8_t record_key = 2;
        model::offset prepare_offset;
        int64_t state;
    };

    struct cached_allocation_cmd {
        model::offset offset;
        allocation_cmd cmd;
    };

    ss::future<> process(model::record_batch&& b);
    void execute(model::offset offset, allocation_cmd c);

    ss::future<> apply(model::record_batch b) override;
    ss::future<result<raft::replicate_result>> replicate(model::record_batch&&);
    ss::future<log_allocation_result> replicate_and_wait(
      allocation_cmd, model::timeout_clock::time_point, sequence_id);
    ss::future<bool> replicate_and_wait(
      prepare_truncation_cmd, model::timeout_clock::time_point, sequence_id);

    config::configuration& _config;
    int64_t _last_seq_tick;
    std::optional<model::run_id> _run_id;

    raft::consensus* _c;
    absl::flat_hash_map<sequence_id, expiring_promise<log_allocation_result>>
      _promises;
    absl::flat_hash_map<sequence_id, expiring_promise<bool>> _prepare_promises;

    int64_t _state{0};
    int64_t _processed{0};
    int64_t _last_allocated_base{0};
    int64_t _last_allocated_range{0};
    bool _should_cache{false};
    model::offset _prepare_offset{0};
    int64_t _prepare_state{0};
    std::vector<cached_allocation_cmd> _cache;
};

} // namespace cluster
