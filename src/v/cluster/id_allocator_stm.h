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

#include "cluster/fwd.h"
#include "cluster/state_machine_registry.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "raft/errc.h"
#include "raft/persisted_stm.h"
#include "raft/state_machine.h"
#include "utils/mutex.h"

#include <absl/container/flat_hash_map.h>

namespace config {
struct configuration;
}

namespace cluster {

// id_allocator is a service to generate cluster-wide unique id (int64)

class id_allocator_stm final : public raft::persisted_stm<> {
public:
    static constexpr std::string_view name = "id_allocator_stm";

    using stm_allocation_result = result<int64_t>;

    explicit id_allocator_stm(ss::logger&, raft::consensus*);

    explicit id_allocator_stm(
      ss::logger&, raft::consensus*, config::configuration&);

    ss::future<stm_allocation_result>
    allocate_id(model::timeout_clock::duration timeout);

    ss::future<iobuf> take_snapshot(model::offset) final { co_return iobuf{}; }

    ss::future<stm_allocation_result>
    reset_next_id(int64_t, model::timeout_clock::duration timeout);

private:
    // legacy structs left for backward compatibility with the "old"
    // on-disk log format
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

    // /legacy

    bool _should_cache{false};
    bool _procesing_legacy{true};
    model::offset _prepare_offset{0};
    std::vector<allocation_cmd> _cache;

    struct state_cmd {
        static constexpr uint8_t record_key = 3;
        int64_t next_state;
    };

    ss::future<stm_allocation_result>
      do_allocate_id(model::timeout_clock::duration);
    ss::future<bool> set_state(int64_t, model::timeout_clock::duration);

    ss::future<> do_apply(const model::record_batch&) final;

    // Moves the state forward to the given value if the curent id is lower
    // than it.
    ss::future<stm_allocation_result>
      advance_state(int64_t, model::timeout_clock::duration);

    ss::future<> write_snapshot();
    ss::future<>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) override;
    ss::future<raft::stm_snapshot>
    take_local_snapshot(ssx::semaphore_units apply_units) override;
    ss::future<> apply_raft_snapshot(const iobuf&) final;
    ss::future<bool> sync(model::timeout_clock::duration);

    mutex _lock{"id_allocator"};

    // id_allocator_stm is a state machine generating unique increasing IDs.
    // When a node becomes a leader it allocates a range of IDs of size
    // `_batch_size` by saving `state_cmd {_state + _batch_size}` to the log.
    // The state machine uses `_curr_batch` to track the number of the IDs
    // left in the range. Each time `allocate_id` is called the state machine
    // increments `_curr_id` (it is initialy equal to `_state`) and decrements
    // `_curr_batch`. When the latter reaches zero the state machine allocates
    // a new batch.
    //
    // Unlike the data partitions id_allocator_stm doesn't rely on the eviction
    // stm and manages log truncations on its own. STM counts the number of
    // applied `state_cmd` commands and when it (`_processed`) surpasses
    // `_log_capacity` id_allocator_stm trucates the prefix.
    int64_t _batch_size;
    int16_t _log_capacity;

    int64_t _processed{0};
    model::offset _next_snapshot{-1};
    int64_t _curr_id{0};
    int64_t _curr_batch{0};
    int64_t _state{0};

    bool _is_writing_snapshot{false};
};

class id_allocator_stm_factory : public state_machine_factory {
public:
    id_allocator_stm_factory() = default;
    bool is_applicable_for(const storage::ntp_config& cfg) const final;

    void create(
      raft::state_machine_manager_builder& builder,
      raft::consensus* raft) final;
};

} // namespace cluster
