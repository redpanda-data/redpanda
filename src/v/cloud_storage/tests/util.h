/*
 * Copyright 2023 Redpanda Data, Inc.
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
#include "bytes/streambuf.h"
#include "cloud_storage/async_manifest_view.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_partition.h"
#include "cloud_storage/tests/cloud_storage_fixture.h"
#include "cloud_storage/tests/common_def.h"
#include "model/record_batch_types.h"
#include "utils/lazy_abort_source.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <ostream>
#include <random>
#include <vector>

namespace cloud_storage {

static lazy_abort_source always_continue{[]() { return std::nullopt; }};

inline ss::logger test_util_log("test_util"); // NOLINT

constexpr bool is_internal_record_batch(model::record_batch_type type) {
    return type == model::record_batch_type::raft_configuration
           || type == model::record_batch_type::archival_metadata;
}

struct segment_layout {
    std::vector<std::vector<cloud_storage::batch_t>> segments;
    size_t num_data_batches;
};

segment_layout generate_segment_layout(
  int num_segments, int seed, bool exclude_tx_fence = true);

class test_consumer final {
public:
    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        headers.push_back(b.header());
        co_return ss::stop_iteration::no;
    }

    std::vector<model::record_batch_header> end_of_stream() {
        return std::move(headers);
    }

    std::vector<model::record_batch_header> headers;
};

struct in_memory_segment {
    ss::sstring bytes;
    std::vector<model::record_batch_header> headers;
    std::vector<iobuf> records;
    std::vector<uint64_t> file_offsets;
    model::offset base_offset, max_offset;
    segment_name sname;
    int num_config_batches{0};
    int num_config_records{0};
    bool do_not_reupload{false};
    // (Optional) If the segment overlaps with the prevoius segment this field
    // should contain number of config records inside the overlapping area. This
    // is needed to compute the offset_delta field in the manifest correctly.
    //
    // Only used to emulate an older version of Redpanda that had offset
    // overlap between segments.
    int delta_offset_overlap{0};
    std::optional<model::timestamp> base_timestamp;
    std::optional<model::timestamp> last_timestamp;
};

std::ostream& operator<<(std::ostream& o, const in_memory_segment& ims);

std::unique_ptr<storage::continuous_batch_parser> make_recording_batch_parser(
  iobuf buf,
  std::vector<model::record_batch_header>& headers,
  std::vector<iobuf>& records,
  std::vector<uint64_t>& file_offsets);

ss::sstring linearize_iobuf(iobuf io);

in_memory_segment
make_segment(model::offset base, const std::vector<batch_t>& batches);

in_memory_segment make_segment(model::offset base, int num_batches);

std::vector<in_memory_segment> make_segments(int num_segments, int num_batches);

in_memory_segment merge_in_memory_segments(
  const in_memory_segment& lhs, const in_memory_segment& rhs);

in_memory_segment copy_in_memory_segment(const in_memory_segment& src);

in_memory_segment
copy_subsegment(const in_memory_segment& src, size_t shift, size_t length);

std::vector<in_memory_segment>
make_segments(const partition_manifest& manifest);

std::vector<in_memory_segment> make_segments(
  const std::vector<std::vector<batch_t>>& segments,
  bool produce_overlapping = false,
  bool produce_duplicate = false);

std::vector<in_memory_segment> make_segments(
  const std::vector<std::vector<batch_t>>& segments, model::offset base_offset);

enum class manifest_inconsistency {
    none,
    truncated_segments,
    overlapping_segments,
    duplicate_offset_ranges,
};

std::vector<cloud_storage_fixture::expectation> make_imposter_expectations(
  const cloud_storage::partition_manifest& m,
  const std::vector<in_memory_segment>& segments);

std::vector<cloud_storage_fixture::expectation> make_imposter_expectations(
  cloud_storage::partition_manifest& m,
  const std::vector<in_memory_segment>& segments,
  bool truncate_segments = false,
  model::offset_delta delta = model::offset_delta(0),
  segment_name_format sname_format = segment_name_format::v3);

std::vector<in_memory_segment> setup_s3_imposter(
  cloud_storage_fixture& fixture,
  int num_segments,
  int num_batches_per_segment,
  manifest_inconsistency inject = manifest_inconsistency::none,
  segment_name_format sname_format = segment_name_format::v3);

std::vector<in_memory_segment> setup_s3_imposter(
  cloud_storage_fixture& fixture,
  model::offset base_offset,
  model::offset_delta base_delta,
  const std::vector<std::vector<batch_t>>& batches);

std::vector<in_memory_segment> setup_s3_imposter(
  cloud_storage_fixture& fixture,
  std::vector<std::vector<batch_t>> batches,
  manifest_inconsistency inject = manifest_inconsistency::none);

std::vector<in_memory_segment> setup_s3_imposter(
  cloud_storage_fixture& fixture,
  const cloud_storage::partition_manifest& manifest);

partition_manifest
hydrate_manifest(remote& api, const cloud_storage_clients::bucket_name& bucket);

/// Similar to prev function but scans the range of offsets instead of
/// returning a single one
std::vector<model::record_batch_header> scan_remote_partition_incrementally(
  cloud_storage_fixture& imposter,
  model::offset base,
  model::offset max,
  size_t maybe_max_bytes = 0,
  size_t maybe_max_segments = 0,
  size_t maybe_max_readers = 0);

/// Similar to prev function but scans the range of offsets instead of
/// returning a single one
std::vector<model::record_batch_header> scan_remote_partition(
  cloud_storage_fixture& imposter,
  model::offset base,
  model::offset max = model::offset::max(),
  size_t maybe_max_segments = 0,
  size_t maybe_max_readers = 0);

struct scan_result {
    std::vector<model::record_batch_header> headers;
    // number of bytes consumed (acquired from the metrics probe)
    uint64_t bytes_read = 0;
    // number of bytes consumed (acquired from the metrics probe)
    uint64_t records_read = 0;
    // number of bytes skipped by the segment reader (acquired from the metrics
    // probe)
    uint64_t bytes_skip = 0;
    // number of bytes accepted by the segment reader (acquired from the metrics
    // probe)
    uint64_t bytes_accept = 0;
};

/// Similar to prev function but uses timequery
scan_result scan_remote_partition(
  cloud_storage_fixture& imposter,
  model::offset min,
  model::timestamp timestamp,
  model::offset max = model::offset::max(),
  size_t maybe_max_segments = 0,
  size_t maybe_max_readers = 0);

void reupload_compacted_segments(
  cloud_storage_fixture& fixture,
  cloud_storage::partition_manifest& m,
  const std::vector<in_memory_segment>& segments,
  bool truncate_segments = false);

std::vector<in_memory_segment> replace_segments(
  cloud_storage_fixture& fixture,
  cloud_storage::partition_manifest& manifest,
  model::offset base_offset,
  model::offset_delta base_delta,
  const std::vector<std::vector<batch_t>>& batches);

/// Read batches by one using max_bytes=1 and set max_offset to closes
/// value in the 'possible_lso_values' list.
std::vector<model::record_batch_header>
scan_remote_partition_incrementally_with_closest_lso(
  cloud_storage_fixture& imposter,
  model::offset base,
  model::offset max,
  size_t maybe_max_segments,
  size_t maybe_max_readers);

} // namespace cloud_storage
