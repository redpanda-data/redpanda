/*
 * Copyright 2022 Redpanda Data, Inc.
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
#include "base/seastarx.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "storage/fwd.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/io_priority_class.hh>

#include <fmt/format.h>

#include <optional>

namespace storage {

namespace internal {

class offset_to_filepos_consumer {
public:
    using type
      = std::optional<std::tuple<model::offset, size_t, model::timestamp>>;

    offset_to_filepos_consumer(
      model::offset log_start_offset,
      model::offset target,
      size_t initial,
      model::timestamp initial_timestamp);

    ss::future<ss::stop_iteration> operator()(::model::record_batch batch);

    // Returns the offsets corresponding to the batch end offset strictly below
    // the target offset.
    type end_of_stream();

private:
    type _filepos;
    model::offset _target_last_offset;
    model::offset _prev_batch_last_offset;
    model::timestamp _prev_batch_max_timestamp;
    size_t _prev_end_pos;
};

} // namespace internal

struct offset_to_file_pos_result {
    model::offset offset;
    size_t bytes;
    model::timestamp ts;
    bool offset_inside_batch{false};

    auto operator<=>(const offset_to_file_pos_result&) const = default;
};

using should_fail_on_missing_offset
  = ss::bool_class<struct should_fail_on_missing_offset_tag>;

ss::future<result<offset_to_file_pos_result>> convert_begin_offset_to_file_pos(
  model::offset begin_inclusive,
  ss::lw_shared_ptr<storage::segment> segment,
  model::timestamp base_timestamp,
  ss::io_priority_class io_priority,
  should_fail_on_missing_offset fail_on_missing_offset
  = should_fail_on_missing_offset::yes);

ss::future<result<offset_to_file_pos_result>> convert_end_offset_to_file_pos(
  model::offset end_inclusive,
  ss::lw_shared_ptr<storage::segment> segment,
  model::timestamp max_timestamp,
  ss::io_priority_class io_priority,
  should_fail_on_missing_offset fail_on_missing_offset
  = should_fail_on_missing_offset::yes);

} // namespace storage
