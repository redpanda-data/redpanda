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

#include "model/fundamental.h"
#include "model/record.h"
#include "seastarx.h"
#include "storage/logger.h"

#include <seastar/core/future-util.hh>

#include <fmt/format.h>

#include <optional>

namespace storage::internal {
class offset_to_filepos_consumer {
public:
    using type = std::optional<std::pair<model::offset, size_t>>;
    offset_to_filepos_consumer(
      model::offset log_start_offset, model::offset target, size_t initial)
      : _target_last_offset(target)
      , _prev_batch_last_offset(model::prev_offset(log_start_offset))
      , _accumulator(initial)
      , _prev(initial) {}
    ss::future<ss::stop_iteration> operator()(::model::record_batch batch) {
        _prev = _accumulator;
        _accumulator += batch.size_bytes();

        if (_target_last_offset <= batch.base_offset()) {
            _filepos = {_prev_batch_last_offset, _prev};
            co_return ss::stop_iteration::yes;
        }
        if (
          _target_last_offset > batch.base_offset()
          && _target_last_offset <= batch.last_offset()) {
            throw std::runtime_error(fmt::format(
              "Offset to file position consumer isn't able to translate "
              "offsets other than batch base offset or offset being in the "
              "gap. Requested offset: {}, current batch offsets: [{},{}]",
              _target_last_offset,
              batch.base_offset(),
              batch.last_offset()));
        }

        _prev_batch_last_offset = batch.last_offset();
        co_return ss::stop_iteration::no;
    }
    type end_of_stream() { return _filepos; }

private:
    type _filepos;
    model::offset _target_last_offset;
    model::offset _prev_batch_last_offset;
    size_t _accumulator;
    size_t _prev;
};

} // namespace storage::internal
