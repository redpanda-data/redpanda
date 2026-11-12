/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/remote_segment_index.h"

#include "raft/consensus.h"

namespace cloud_storage {

remote_segment_index_builder::remote_segment_index_builder(
  const model::ntp& ntp,
  offset_index& ix,
  model::offset_delta initial_delta,
  size_t sampling_step,
  std::optional<std::reference_wrapper<segment_record_stats>> maybe_stats)
  : _ix(ix)
  , _running_delta(initial_delta)
  , _sampling_step(sampling_step)
  , _filter(raft::offset_translator_batch_types(ntp))
  , _stats(maybe_stats) {}

remote_segment_index_builder::consume_result
remote_segment_index_builder::accept_batch_start(
  const model::record_batch_header&) const {
    return consume_result::accept_batch;
}

void remote_segment_index_builder::consume_batch_start(
  model::record_batch_header hdr,
  size_t physical_base_offset,
  size_t size_on_disk) {
    auto it = std::find(_filter.begin(), _filter.end(), hdr.type);
    const auto is_config = it != _filter.end();
    auto delta = hdr.last_offset_delta + 1;
    if (is_config) {
        _running_delta += delta;
    } else {
        _running_max_timestamp = std::max(
          _running_max_timestamp, model::batch_max_timestamp(hdr));
        if (_window >= _sampling_step) {
            _ix.add(
              hdr.base_offset,
              hdr.base_offset - _running_delta,
              static_cast<int64_t>(physical_base_offset),
              _running_max_timestamp);
            _window = 0;
        }
    }
    _window += size_on_disk;

    // Update stats
    if (_stats.has_value()) {
        if (is_config) {
            _stats->get().total_conf_records += delta;
        } else {
            _stats->get().total_data_records += delta;
        }
        if (_stats->get().base_rp_offset == model::offset{}) {
            _stats->get().base_rp_offset = hdr.base_offset;
        }
        _stats->get().last_rp_offset = hdr.last_offset();
        if (_stats->get().base_timestamp == model::timestamp{}) {
            _stats->get().base_timestamp = hdr.first_timestamp;
        }
        _stats->get().last_timestamp = hdr.max_timestamp;
        _stats->get().size_bytes += hdr.size_bytes;
    }
}

void remote_segment_index_builder::skip_batch_start(
  model::record_batch_header, size_t, size_t) {
    vunreachable("no batches should be skipped by this consumer");
}

void remote_segment_index_builder::consume_records(iobuf&&) {}

ss::future<remote_segment_index_builder::stop_parser>
remote_segment_index_builder::consume_batch_end() {
    co_return stop_parser::no;
}

fmt::iterator remote_segment_index_builder::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "remote_segment_index_builder");
}

} // namespace cloud_storage
