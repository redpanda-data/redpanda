/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/outcome.h"
#include "bytes/iobuf.h"
#include "jumbo_log/metadata.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "raft/persisted_stm.h"
#include "raft/state_machine.h"
#include "utils/mutex.h"
#include "utils/uuid.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/util/log.hh>

#include <absl/container/btree_map.h>
#include <absl/strings/string_view.h>

#include <vector>

namespace config {
struct configuration;
}

namespace cluster {

class jumbo_log_stm final : public raft::persisted_stm<> {
public:
    static constexpr std::string_view name = "jumbo_log_stm";

    explicit jumbo_log_stm(ss::logger&, raft::consensus*);

    explicit jumbo_log_stm(
      ss::logger&, raft::consensus*, config::configuration&);

    ss::future<result<jumbo_log::write_intent_id_t>> create_write_intent(
      jumbo_log::segment_object, model::timeout_clock::duration);

    ss::future<result<std::vector<jumbo_log::write_intent_segment>>>
    get_write_intents(
      std::vector<jumbo_log::write_intent_id_t> ids,
      model::timeout_clock::duration);

    // ss::future<stm_allocation_result>
    // allocate_id(model::timeout_clock::duration timeout);

    ss::future<iobuf> take_snapshot(model::offset) final { co_return iobuf{}; }

    // ss::future<stm_allocation_result>
    // reset_next_id(int64_t, model::timeout_clock::duration timeout);

private:
    struct write_intent;
    struct create_write_intent_cmd;

    ss::future<> apply(const model::record_batch&) final;

    ss::future<> write_snapshot();
    ss::future<> apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) final;
    ss::future<raft::stm_snapshot> take_local_snapshot() final;
    ss::future<> apply_raft_snapshot(const iobuf&) final;
    ss::future<bool> sync(model::timeout_clock::duration);

    mutex _lock{"jumbo_log"};
    bool _is_writing_snapshot{false};

    ///////////////////////////////////////////
    /// Below are methods corresponding to the persistent state of the STM.
    ///////////////////////////////////////////
    void apply_create_write_intent(write_intent intent);

    ///////////////////////////////////////////
    /// Below are members corresponding to the persistent state of the STM.
    ///////////////////////////////////////////

    ///////////////////////////////////////////
    /// Staging region.
    ///////////////////////////////////////////

    /// Next write intent ID we will allocate.
    jumbo_log::write_intent_id_t _next_write_intent_id;

    /// Write intents.
    absl::
      btree_map<jumbo_log::write_intent_id_t, jumbo_log::write_intent_segment>
        _write_intents;
    /// An index for identifying duplicate write intents and ensuring
    /// idempotency. Deduplicate by object id of the write intent segment.
    absl::btree_map<uuid_t, jumbo_log::write_intent_id_t>
      _write_intent_id_by_object_id;
};

} // namespace cluster
