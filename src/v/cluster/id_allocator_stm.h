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

class id_allocator_stm final : public raft::state_machine {
public:
    struct stm_allocation_result {
        int64_t id;
        raft::errc raft_status{raft::errc::success};
    };

    explicit id_allocator_stm(
      ss::logger&, raft::consensus*, config::configuration&);

    ss::future<stm_allocation_result>
    allocate_id(model::timeout_clock::duration timeout);

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

    ss::future<> apply(model::record_batch) final;
};

} // namespace cluster
