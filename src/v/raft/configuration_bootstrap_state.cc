// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/configuration_bootstrap_state.h"

#include "likely.h"
#include "model/record.h"
#include "raft/types.h"
#include "reflection/adl.h"

#include <fmt/format.h>

namespace raft {
void configuration_bootstrap_state::process_configuration(
  model::record_batch b) {
    if (unlikely(b.header().type != configuration_batch_type)) {
        throw std::runtime_error(fmt::format(
          "Logic error. Asked a configuration tracker to process an unknown "
          "record_batch_type: {}",
          b.header().type));
    }
    if (unlikely(b.compressed())) {
        throw std::runtime_error(
          "Compressed configuration records are unsupported");
    }
    auto last_offset = b.last_offset();
    if (_log_config_offset_tracker < last_offset) {
        _config_batches_seen++;
        _log_config_offset_tracker = last_offset;
        process_offsets(b.base_offset(), last_offset);
        b.for_each_record([this](model::record rec) {
            _config = reflection::from_iobuf<raft::group_configuration>(
              rec.release_value());
        });
    }
}
void configuration_bootstrap_state::process_data_offsets(
  model::record_batch b) {
    _data_batches_seen++;
    if (unlikely(b.header().type == configuration_batch_type)) {
        throw std::runtime_error(fmt::format(
          "Logic error. Asked a data tracker to process "
          "configuration_batch_type "
          "record_batch_type: {}",
          b.header().type));
    }
    process_offsets(b.base_offset(), b.last_offset());
}

void configuration_bootstrap_state::process_offsets(
  model::offset base_offset, model::offset last_offset) {
    // happy path
    if (last_offset > _commit_index) {
        _prev_log_index = _commit_index;
        _prev_log_term = _term;
        _commit_index = last_offset;
        _commit_index_base_batch_offset = base_offset;
        return;
    }
    // we need to test how to find prev term
    if (
      base_offset < _commit_index_base_batch_offset
      && last_offset >= _commit_index_base_batch_offset) {
        _prev_log_index = _commit_index;
        _prev_log_term = _term;
        return;
    }
}

void configuration_bootstrap_state::process_batch(model::record_batch b) {
    switch (b.header().type) {
    case configuration_batch_type:
        process_configuration(std::move(b));
        break;
    default:
        process_data_offsets(std::move(b));
        break;
    }
}
} // namespace raft
