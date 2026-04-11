/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/metastore/extent_metadata_reader.h"

#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/retry.h"

namespace cloud_topics::l1 {

extent_metadata_reader::extent_metadata_reader(
  metastore* metastore,
  model::topic_id_partition tp,
  kafka::offset min_offset,
  kafka::offset max_offset,
  iteration_direction iter_dir,
  ss::abort_source& as,
  std::optional<size_t> num_extents_per_request)
  : _metastore(metastore)
  , _tp(std::move(tp))
  , _min_offset(min_offset)
  , _max_offset(max_offset)
  , _iter_dir(iter_dir)
  , _as(as)
  , _num_extents_per_request(
      num_extents_per_request.value_or(default_num_extents_per_request)) {}

extent_metadata_reader::extent_metadata_generator
extent_metadata_reader::generator() {
    switch (_iter_dir) {
    case iteration_direction::forwards:
        return forward_generator();
    case iteration_direction::backwards:
        return backward_generator();
    }
}

extent_metadata_reader::extent_metadata_generator
extent_metadata_reader::forward_generator() {
    kafka::offset next_offset = _min_offset;
    kafka::offset end_offset = _max_offset;
    while (next_offset <= end_offset) {
        auto extent_md_res = co_await retry_metastore_op_with_default_rtc(
          [this, next_offset] {
              return _metastore->get_extent_metadata_forwards(
                _tp,
                next_offset,
                _max_offset,
                _num_extents_per_request,
                metastore::include_object_metadata::no);
          },
          _as);

        if (!extent_md_res.has_value()) {
            // Return the error to the user. Allow them to decide what
            // to do with the generator.
            co_yield std::unexpected(extent_md_res.error());
        } else {
            // Yield extents.
            auto extents = std::move(extent_md_res->extents);
            bool end_of_stream = extent_md_res->end_of_stream;

            dassert(
              end_of_stream || !extents.empty(),
              "end_of_stream=false requires non-empty extents");

            for (const auto& extent : extents) {
                co_yield extent;
            }

            if (end_of_stream) {
                break;
            }

            if (!extents.empty()) {
                next_offset = kafka::next_offset(extents.back().last_offset);
            }
        }
    }
}

extent_metadata_reader::extent_metadata_generator
extent_metadata_reader::backward_generator() {
    kafka::offset next_offset = _max_offset;
    kafka::offset end_offset = _min_offset;
    while (next_offset >= end_offset) {
        auto extent_md_res = co_await retry_metastore_op_with_default_rtc(
          [this, next_offset] {
              return _metastore->get_extent_metadata_backwards(
                _tp, _min_offset, next_offset, _num_extents_per_request);
          },
          _as);

        if (!extent_md_res.has_value()) {
            // Return the error to the user. Allow them to decide what
            // to do with the generator.
            co_yield std::unexpected(extent_md_res.error());
        } else {
            // Yield extents.
            auto extents = std::move(extent_md_res->extents);
            bool end_of_stream = extent_md_res->end_of_stream;

            dassert(
              end_of_stream || !extents.empty(),
              "end_of_stream=false requires non-empty extents");

            for (const auto& extent : extents) {
                co_yield extent;
            }

            if (end_of_stream) {
                break;
            }

            if (!extents.empty()) {
                next_offset = kafka::prev_offset(extents.back().base_offset);
            }
        }
    }
}

} // namespace cloud_topics::l1
