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
#include "config/configuration.h"
#include "model/fundamental.h"
#include "seastarx.h"
#include "storage/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/util/log.hh>

namespace raft {

class consensus;

/**
 * Responsible for taking snapshots triggered by underlying log segments
 * eviction.
 *
 * The process goes like this: storage layer will send a "deletion notification"
 * - a request to evict log up to a certain offset. log_eviction_stm will then
 * adjust that offset with _stm_manager->max_collectible_offset(), write the
 * raft snapshot and notify the storage layer that log eviction can safely
 * proceed up to the adjusted offset.
 */
class log_eviction_stm {
public:
    log_eviction_stm(
      consensus*,
      ss::logger&,
      ss::lw_shared_ptr<storage::stm_manager>,
      ss::abort_source&);

    ss::future<> start();

    ss::future<> stop();

    /// Return the offset up to which the storage layer would like to
    /// prefix truncate the log, if any.
    std::optional<model::offset> eviction_requested_offset() const {
        if (_requested_eviction_offset == model::offset{}) {
            return std::nullopt;
        } else {
            return _requested_eviction_offset;
        }
    }

private:
    ss::future<> handle_deletion_notification(model::offset);
    void monitor_log_eviction();

    consensus* _raft;
    ss::logger& _logger;
    ss::lw_shared_ptr<storage::stm_manager> _stm_manager;
    ss::abort_source& _as;
    ss::gate _gate;
    model::offset _previous_eviction_offset;
    model::offset _requested_eviction_offset;
};

} // namespace raft
