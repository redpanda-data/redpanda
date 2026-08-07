/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "cloud_storage/offset_index.h"
#include "model/fundamental.h"
#include "model/record_batch_types.h"
#include "storage/parser.h"

#include <seastar/util/log.hh>

namespace cloud_storage {

struct segment_record_stats {
    // Offset of the first record in the segment
    model::offset base_rp_offset;
    // Offset of the last record in the segment
    model::offset last_rp_offset;
    // Number of records in all data batches in the segment (this includes tx
    // batches and all non-data batch types which doesn't participate in offset
    // translation)
    size_t total_data_records{0};
    // Number of records in all config batches in the segment (this includes
    // raft-configuration, archival and few other batches)
    size_t total_conf_records{0};
    // Total size of the segment
    size_t size_bytes{0};
    // Base timestamp
    model::timestamp base_timestamp;
    // Last timestamp
    model::timestamp last_timestamp;

    auto operator<=>(const segment_record_stats&) const noexcept = default;
};

class remote_segment_index_builder : public storage::batch_consumer {
public:
    using consume_result = storage::batch_consumer::consume_result;
    using stop_parser = storage::batch_consumer::stop_parser;

    remote_segment_index_builder(
      const model::ntp& ntp,
      offset_index& ix,
      model::offset_delta initial_delta,
      size_t sampling_step,
      std::optional<std::reference_wrapper<segment_record_stats>> maybe_stats);

    consume_result
    accept_batch_start(const model::record_batch_header&) const override;

    void consume_batch_start(
      model::record_batch_header,
      size_t physical_base_offset,
      size_t size_on_disk) override;

    void skip_batch_start(
      model::record_batch_header,
      size_t physical_base_offset,
      size_t size_on_disk) override;

    void consume_records(iobuf&&) override;
    ss::future<stop_parser> consume_batch_end() override;
    fmt::iterator format_to(fmt::iterator it) const override;

private:
    offset_index& _ix;
    model::offset_delta _running_delta;
    size_t _window{0};
    size_t _sampling_step;
    std::vector<model::record_batch_type> _filter;
    /// Collected stats
    std::optional<std::reference_wrapper<segment_record_stats>> _stats;
};

inline ss::lw_shared_ptr<storage::continuous_batch_parser>
make_remote_segment_index_builder(
  const model::ntp& ntp,
  ss::input_stream<char> stream,
  offset_index& ix,
  model::offset_delta initial_delta,
  size_t sampling_step,
  std::optional<std::reference_wrapper<segment_record_stats>> maybe_stats
  = std::nullopt) {
    auto parser = ss::make_lw_shared<storage::continuous_batch_parser>(
      std::make_unique<remote_segment_index_builder>(
        ntp, ix, initial_delta, sampling_step, maybe_stats),
      storage::segment_reader_handle(std::move(stream)));
    return parser;
}

} // namespace cloud_storage
