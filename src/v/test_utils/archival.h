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
#include "cluster/archival/archival_policy.h"
#include "model/tests/random_batch.h"
#include "storage/tests/utils/disk_log_builder.h"

#include <boost/test/unit_test.hpp>

inline ss::input_stream<char> make_manifest_stream(std::string_view json) {
    iobuf i;
    i.append(json.data(), json.size());
    return make_iobuf_input_stream(std::move(i));
}

inline storage::disk_log_builder make_log_builder(std::string_view data_path) {
    return storage::disk_log_builder{storage::log_config{
      {data_path.data(), data_path.size()},
      4_KiB,
      ss::default_priority_class(),
      storage::make_sanitized_file_config()}};
}

struct segment_spec {
    model::offset start_offset;
    model::offset end_offset;
    size_t size_bytes;
    std::optional<model::timestamp> timestamp{std::nullopt};
    model::offset start_kafka_offset;
    model::offset end_kafka_offset;

    segment_spec(
      size_t start,
      size_t end,
      size_t size,
      std::optional<model::timestamp> ts = std::nullopt,
      int start_kafka = -1,
      int end_kafka = -1)
      : start_offset(start)
      , end_offset(end)
      , size_bytes(size)
      , timestamp(ts)
      , start_kafka_offset(start_kafka >= 0 ? start_kafka : start)
      , end_kafka_offset(end_kafka >= 0 ? end_kafka : end) {}

    friend std::ostream&
    operator<<(std::ostream& os, const segment_spec& spec) {
        fmt::print(
          os,
          "{{ start_offset={}, end_offset={}, size_bytes={}, timestamp={}, "
          "start_kafka_offset={}, end_kafka_offset={} }}",
          spec.start_offset,
          spec.end_offset,
          spec.size_bytes,
          spec.timestamp,
          spec.start_kafka_offset,
          spec.end_kafka_offset);
        return os;
    }
};

inline void populate_local_log(
  storage::disk_log_builder& b, const std::vector<segment_spec>& segs) {
    for (const auto& spec : segs) {
        auto record_batch = make_random_batch(model::test::record_batch_spec{
          .offset = spec.start_offset,
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
        m.add(
          {.size_bytes = spec.size_bytes,
           .base_offset = spec.start_offset,
           .committed_offset = spec.end_offset,
           .max_timestamp = spec.timestamp ? *spec.timestamp
                                           : model::timestamp::now(),
           .delta_offset = model::offset_delta(
             spec.start_offset - spec.start_kafka_offset),
           .delta_offset_end = model::offset_delta(
             spec.end_offset - spec.end_kafka_offset)});
    }
}

inline archival::upload_candidate_with_locks
require_upload_candidate(archival::candidate_creation_result&& r) {
    ss::visit(
      r,
      [](std::monostate) {
          BOOST_FAIL("unexpected default candidate creation result");
      },
      [](const archival::candidate_creation_error& err) {
          BOOST_FAIL(fmt::format("unexpected creation error: {}", err));
      },
      [](const archival::skip_offset_range& r) {
          BOOST_FAIL(fmt::format("unexpected skip offset range: {}", r.reason));
      },
      [](const archival::upload_candidate_with_locks&) {});
    return std::move(std::get<archival::upload_candidate_with_locks>(r));
}

inline void require_candidate_creation_error(
  archival::candidate_creation_result&& r,
  archival::candidate_creation_error expected) {
    ss::visit(
      r,
      [](std::monostate) {
          BOOST_FAIL("unexpected default candidate creation result");
      },
      [expected](const archival::candidate_creation_error& actual) {
          BOOST_REQUIRE_EQUAL(actual, expected);
      },
      [](const archival::skip_offset_range& r) {
          BOOST_FAIL(fmt::format("unexpected skip offset range: {}", r));
      },
      [](const archival::upload_candidate_with_locks&) {
          BOOST_FAIL("unexpected candidate created");
      });
}

inline void require_skip_offset(
  archival::candidate_creation_result&& r,
  archival::candidate_creation_error expected,
  model::offset final) {
    ss::visit(
      r,
      [](std::monostate) {
          BOOST_FAIL("unexpected default candidate creation result");
      },
      [](const archival::candidate_creation_error& err) {
          BOOST_FAIL(fmt::format("unexpected creation error: {}", err));
      },
      [expected, final](const archival::skip_offset_range& r) {
          BOOST_REQUIRE_EQUAL(r.reason, expected);
          BOOST_REQUIRE_EQUAL(r.end_offset, final);
      },
      [](const archival::upload_candidate_with_locks&) {
          BOOST_FAIL("unexpected candidate created");
      });
}
