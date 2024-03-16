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

#include "model/record.h"
#include "raft/group_configuration.h"

#include <fmt/format.h>

namespace raft {

class configuration_bootstrap_state {
public:
    configuration_bootstrap_state() = default;
    configuration_bootstrap_state(configuration_bootstrap_state&&) noexcept
      = default;
    configuration_bootstrap_state&
    operator=(configuration_bootstrap_state&&) noexcept
      = default;

    void process_batch(model::record_batch);
    void process_configuration(model::record_batch);
    void process_data_offsets(model::record_batch);

    model::offset commit_index() const { return _commit_index; }
    model::term_id term() const { return _term; }
    model::offset prev_log_index() const { return _prev_log_index; }
    model::term_id prev_log_term() const { return _prev_log_term; }
    const std::vector<raft::offset_configuration>& configurations() const {
        return _configurations;
    }
    std::vector<raft::offset_configuration> release_configurations() && {
        return std::move(_configurations);
    }
    void set_term(model::term_id t) { _term = t; }
    void set_end_of_log() { _end_of_log = true; }
    uint32_t data_batches_seen() const { return _data_batches_seen; }
    uint32_t config_batches_seen() const { return _configurations.size(); }

    friend std::ostream&
    operator<<(std::ostream& o, const configuration_bootstrap_state& s) {
        fmt::print(
          o,
          "data_seen {} config_seen {} eol {} commit {} term {} prev_idx {} "
          "prev_term {} config_tracker {} commit_base_tracker {} "
          "configurations {}",
          s._data_batches_seen,
          s._configurations.size(),
          s._end_of_log,
          s._commit_index,
          s._term,
          s._prev_log_index,
          s._prev_log_term,
          s._log_config_offset_tracker,
          s._commit_index_base_batch_offset,
          s._configurations);
        return o;
    }

private:
    void process_offsets(model::offset base_offset, model::offset last_offset);

    uint32_t _data_batches_seen{0};
    bool _end_of_log{false};
    // This is an exclusive upper bound
    model::offset _commit_index{0};
    model::term_id _term{0};
    model::offset _prev_log_index{0};
    model::term_id _prev_log_term{0};
    std::vector<raft::offset_configuration> _configurations;

    // we need to keep track of what we have processed in case we re-reprocess a
    // multiple segments
    model::offset _log_config_offset_tracker;
    model::offset _commit_index_base_batch_offset;
};
} // namespace raft
