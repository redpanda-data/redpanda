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

#include "bytes/iostream.h"
#include "cloud_storage/partition_manifest.h"
#include "model/tests/random_batch.h"
#include "storage/tests/utils/disk_log_builder.h"

inline ss::input_stream<char> make_manifest_stream(std::string_view json) {
    iobuf i;
    i.append(json.data(), json.size());
    return make_iobuf_input_stream(std::move(i));
}

inline storage::disk_log_builder make_log_builder(std::string_view data_path) {
    return storage::disk_log_builder{storage::log_config{
      {data_path.data(), data_path.size()},
      4_KiB,
      storage::debug_sanitize_files::yes,
    }};
}

struct segment_spec {
    size_t start_offset;
    size_t end_offset;
    size_t size_bytes;
    std::optional<model::timestamp> timestamp{std::nullopt};

    friend std::ostream&
    operator<<(std::ostream& os, const segment_spec& spec) {
        fmt::print(
          os,
          "{{ start_offset={}, end_offset={}, size_bytes={}, timestamp={} }}",
          spec.start_offset,
          spec.end_offset,
          spec.size_bytes,
          spec.timestamp);
        return os;
    }
};

inline void populate_local_log(
  storage::disk_log_builder& b, const std::vector<segment_spec>& segs) {
    for (const auto& spec : segs) {
        auto record_batch = make_random_batch(model::test::record_batch_spec{
          .offset = model::offset{spec.start_offset},
          .allow_compression = false,
          .count = 1,
          .record_sizes = std::vector<size_t>{spec.size_bytes},
          .timestamp = spec.timestamp});

        b | storage::add_segment(spec.start_offset)
          | storage::add_batch(std::move(record_batch));
    }
}

inline void populate_manifest(
  cloud_storage::partition_manifest& m, const std::vector<segment_spec>& segs) {
    for (const auto& spec : segs) {
        cloud_storage::partition_manifest::key key = model::offset{
          spec.start_offset};

        cloud_storage::partition_manifest::value value{
          .size_bytes = spec.size_bytes,
          .base_offset = model::offset{spec.start_offset},
          .committed_offset = model::offset{spec.end_offset},
          .max_timestamp = spec.timestamp ? *spec.timestamp
                                          : model::timestamp::now()};
        m.add(key, value);
    }
}
