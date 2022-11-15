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

#include "model/fundamental.h"
#include "model/record.h"
#include "seastarx.h"
#include "storage/logger.h"
#include "storage/segment.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/io_priority_class.hh>

#include <fmt/format.h>

#include <optional>

namespace storage {

namespace internal {

class offset_to_filepos_consumer {
public:
    using type = std::optional<std::pair<model::offset, size_t>>;

    offset_to_filepos_consumer(
      model::offset log_start_offset, model::offset target, size_t initial);

    ss::future<ss::stop_iteration> operator()(::model::record_batch batch);

    type end_of_stream();

private:
    type _filepos;
    model::offset _target_last_offset;
    model::offset _prev_batch_last_offset;
    size_t _accumulator;
    size_t _prev;
};

} // namespace internal

struct offset_to_file_pos_result {
    model::offset offset;
    size_t bytes;
    model::timestamp ts;
};

ss::future<offset_to_file_pos_result> convert_begin_offset_to_file_pos(
  model::offset begin_inclusive,
  ss::lw_shared_ptr<storage::segment> segment,
  model::timestamp base_timestamp,
  ss::io_priority_class io_priority);

ss::future<offset_to_file_pos_result> convert_end_offset_to_file_pos(
  model::offset end_inclusive,
  ss::lw_shared_ptr<storage::segment> segment,
  model::timestamp max_timestamp,
  ss::io_priority_class io_priority);

} // namespace storage