/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/spillover_manifest.h"
#include "cluster/archival/ntp_archiver_service.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

#include <seastar/testing/thread_test_case.hh>

using namespace archival;

namespace {

cloud_storage::partition_manifest make_manifest(size_t num_segments) {
    cloud_storage::partition_manifest manifest(
      model::ntp(
        model::kafka_namespace, model::topic("panda"), model::partition_id(0)),
      model::initial_revision_id(1));
    int64_t base = 0;
    int64_t ts = 1780000000000;
    for (size_t i = 0; i < num_segments; ++i) {
        auto committed = base + 80;
        manifest.add(
          cloud_storage::segment_meta{
            .is_compacted = false,
            .size_bytes = 18000,
            .base_offset = model::offset(base),
            .committed_offset = model::offset(committed),
            .base_timestamp = model::timestamp(ts),
            .max_timestamp = model::timestamp(ts + 300000),
            .delta_offset = model::offset_delta(0),
            .ntp_revision = model::initial_revision_id(1),
            .archiver_term = model::term_id(1),
            .segment_term = model::term_id(1),
            .delta_offset_end = model::offset_delta(0),
            .sname_format = cloud_storage::segment_name_format::v3,
            .metadata_size_hint = 0,
          });
        base = committed + 1;
        ts += 300000;
    }
    return manifest;
}

} // namespace

SEASTAR_THREAD_TEST_CASE(test_spillover_tail_stops_at_segment_limit) {
    auto manifest = make_manifest(20);
    auto tail = make_spillover_tail(manifest, std::nullopt, 5);
    BOOST_REQUIRE_EQUAL(tail.size(), 5);
    BOOST_REQUIRE_EQUAL(
      tail.get_start_offset().value(), manifest.get_start_offset().value());
    BOOST_REQUIRE(
      manifest.safe_spillover_manifest(tail.make_manifest_metadata()));
}

SEASTAR_THREAD_TEST_CASE(test_spillover_tail_takes_one_frame) {
    // Size-based spillover is frame-aligned: even a tiny limit takes
    // exactly one sealed frame.
    auto manifest = make_manifest(2500);
    auto tail = make_spillover_tail(manifest, 1, std::nullopt);
    BOOST_REQUIRE_EQUAL(tail.size(), cloud_storage::cstore_max_frame_size);
    BOOST_REQUIRE_EQUAL(
      tail.get_start_offset().value(), manifest.get_start_offset().value());
    BOOST_REQUIRE(
      manifest.safe_spillover_manifest(tail.make_manifest_metadata()));
}

SEASTAR_THREAD_TEST_CASE(test_spillover_tail_takes_multiple_frames) {
    // A limit that the first sealed frame doesn't reach pulls in the next
    // whole frame.
    auto manifest = make_manifest(2500);
    auto frames = manifest.sealed_segment_frames();
    BOOST_REQUIRE_EQUAL(frames.size(), 2);
    auto tail = make_spillover_tail(
      manifest, frames[0].size_bytes + 1, std::nullopt);
    BOOST_REQUIRE_EQUAL(tail.size(), 2 * cloud_storage::cstore_max_frame_size);
    BOOST_REQUIRE(
      manifest.safe_spillover_manifest(tail.make_manifest_metadata()));
}

SEASTAR_THREAD_TEST_CASE(test_spillover_tail_never_takes_active_frame) {
    // The size limit is impossible to reach. The tail consumes every
    // sealed frame but the active frame always stays behind, so the
    // resulting spillover command remains applicable
    // (safe_spillover_manifest requires a segment to remain after the
    // spillover range).
    auto manifest = make_manifest(2500);
    auto tail = make_spillover_tail(manifest, 100_MiB, std::nullopt);
    BOOST_REQUIRE_EQUAL(tail.size(), 2 * cloud_storage::cstore_max_frame_size);
    BOOST_REQUIRE(
      manifest.safe_spillover_manifest(tail.make_manifest_metadata()));
}

SEASTAR_THREAD_TEST_CASE(test_spillover_tail_sub_frame_manifest) {
    // A manifest that fits entirely in the active frame has nothing to
    // spill in size mode.
    auto manifest = make_manifest(20);
    auto tail = make_spillover_tail(manifest, 100_MiB, std::nullopt);
    BOOST_REQUIRE_EQUAL(tail.size(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_spillover_tail_single_segment_manifest) {
    // Nothing can be spilled from a single-segment manifest
    auto manifest = make_manifest(1);
    auto tail = make_spillover_tail(manifest, 100_MiB, std::nullopt);
    BOOST_REQUIRE_EQUAL(tail.size(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_spillover_tail_count_mode_keeps_last_segment) {
    // The count-based limit (test-only config) keeps the legacy
    // element-wise cut and never consumes the whole manifest.
    auto manifest = make_manifest(20);
    auto tail = make_spillover_tail(manifest, std::nullopt, 100);
    BOOST_REQUIRE_EQUAL(tail.size(), 19);
    BOOST_REQUIRE(
      manifest.safe_spillover_manifest(tail.make_manifest_metadata()));
}
