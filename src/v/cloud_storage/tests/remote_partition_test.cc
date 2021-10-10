/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "cloud_storage/manifest.h"
#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_partition.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/tests/cloud_storage_fixture.h"
#include "cloud_storage/tests/common_def.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/types.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "s3/client.h"
#include "seastarx.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/segment.h"
#include "storage/types.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <exception>
#include <numeric>

using namespace std::chrono_literals;
using namespace cloud_storage;

inline ss::logger test_log("test"); // NOLINT

static std::unique_ptr<storage::continuous_batch_parser>
make_recording_batch_parser(
  iobuf buf,
  std::vector<model::record_batch_header>& headers,
  std::vector<iobuf>& records,
  std::vector<uint64_t>& file_offsets) {
    auto stream = make_iobuf_input_stream(std::move(buf));
    auto parser = std::make_unique<storage::continuous_batch_parser>(
      std::make_unique<recording_batch_consumer>(
        headers, records, file_offsets),
      std::move(stream));
    return parser;
}

ss::sstring linearize_iobuf(iobuf io) {
    ss::sstring bytes;
    for (const auto& f : io) {
        bytes.append(f.get(), f.size());
    }
    return bytes;
}

struct in_memory_segment {
    ss::sstring bytes;
    std::vector<model::record_batch_header> headers;
    std::vector<iobuf> records;
    std::vector<uint64_t> file_offsets;
    model::offset base_offset, max_offset;
    segment_name sname;
};

static in_memory_segment make_segment(model::offset base, int num_batches) {
    iobuf segment_bytes = generate_segment(base, num_batches);
    std::vector<model::record_batch_header> hdr;
    std::vector<iobuf> rec;
    std::vector<uint64_t> off;
    auto p1 = make_recording_batch_parser(
      iobuf_deep_copy(segment_bytes), hdr, rec, off);
    p1->consume().get();
    in_memory_segment s;
    s.bytes = linearize_iobuf(std::move(segment_bytes));
    s.base_offset = hdr.front().base_offset;
    s.max_offset = hdr.back().last_offset();
    s.headers = std::move(hdr);
    s.records = std::move(rec);
    s.file_offsets = std::move(off);
    s.sname = segment_name(fmt::format("{}-1-v1.log", s.base_offset()));
    return s;
}

static std::vector<in_memory_segment>
make_segments(int num_segments, int num_batches) {
    std::vector<in_memory_segment> s;
    model::offset base_offset{0};
    for (int i = 0; i < num_segments; i++) {
        s.push_back(make_segment(base_offset, num_batches));
        base_offset = s.back().max_offset + model::offset(1);
    }
    return s;
}

static std::ostream& operator<<(std::ostream& o, const in_memory_segment& ims) {
    fmt::print(
      o,
      "name {}, base-offset {}, max-offset {}\n",
      ims.sname,
      ims.base_offset,
      ims.max_offset);
    for (size_t i = 0; i < ims.headers.size(); i++) {
        fmt::print(o, "\trecord-batch {}\n", ims.headers[i]);
    }
    return o;
}

static void print_segments(const std::vector<in_memory_segment>& segments) {
    for (const auto& s : segments) {
        vlog(test_log.debug, "segment: {}", s);
    }
}

static std::vector<cloud_storage_fixture::expectation>
make_imposter_expectations(
  cloud_storage::manifest& m, const std::vector<in_memory_segment>& segments) {
    std::vector<cloud_storage_fixture::expectation> results;
    for (const auto& s : segments) {
        // assume all segments has term=1
        auto url = m.get_remote_segment_path(s.sname);
        results.push_back(cloud_storage_fixture::expectation{
          .url = "/" + url().string(), .body = s.bytes});
        cloud_storage::manifest::segment_meta meta{
          .is_compacted = false,
          .size_bytes = s.bytes.size(),
          .base_offset = s.base_offset,
          .committed_offset = s.max_offset,
          .base_timestamp = {},
          .max_timestamp = {},
          .delta_offset = model::offset(0),
        };
        m.add(s.sname, meta);
    }
    std::stringstream ostr;
    m.serialize(ostr);
    results.push_back(cloud_storage_fixture::expectation{
      .url = "/" + m.get_manifest_path()().string(),
      .body = ss::sstring(ostr.str())});
    return results;
}

/// Return vector<bool> which have a value for every recrod_batch_header in
/// 'segments' If i'th value is true then the value are present in both
/// 'headers' and 'segments' Otherwise the i'th value will be false.
static std::vector<bool> get_coverage(
  const std::vector<model::record_batch_header>& headers,
  const std::vector<in_memory_segment>& segments,
  int batches_per_segment) {
    size_t num_record_batches = segments.size() * batches_per_segment;
    std::vector<bool> result(num_record_batches, false);
    size_t hix = 0;
    for (size_t i = 0; i < num_record_batches; i++) {
        const auto& hh = headers[hix];
        const auto& sh = segments.at(i / batches_per_segment)
                           .headers.at(i % batches_per_segment);
        if (hh == sh) {
            hix++;
            result[i] = true;
        }
        if (hix == headers.size()) {
            break;
        }
    }
    return result;
}

using namespace cloud_storage;
using namespace std::chrono_literals;

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

static auto setup_s3_imposter(
  cloud_storage_fixture& fixture,
  int num_segments,
  int num_batches_per_segment) {
    // Create test data
    auto segments = make_segments(num_segments, num_batches_per_segment);
    cloud_storage::manifest manifest(manifest_ntp, manifest_revision);
    auto expectations = make_imposter_expectations(manifest, segments);
    fixture.set_expectations_and_listen(expectations);
    return segments;
}

static manifest hydrate_manifest(remote& api, const s3::bucket_name& bucket) {
    manifest m(manifest_ntp, manifest_revision);
    retry_chain_node rtc(1s, 200ms);
    auto key = m.get_manifest_path();
    auto res = api.download_manifest(bucket, key, m, rtc).get();
    BOOST_REQUIRE(res == cloud_storage::download_result::success);
    return m;
}

/// This test reads only a tip of the log
static model::record_batch_header read_single_batch_from_remote_partition(
  cloud_storage_fixture& fixture, model::offset target) {
    auto conf = fixture.get_configuration();
    static auto bucket = s3::bucket_name("bucket");
    remote api(s3_connection_limit(10), conf);
    auto action = ss::defer([&api] { api.stop().get(); });
    auto m = ss::make_lw_shared<cloud_storage::manifest>(
      manifest_ntp, manifest_revision);
    offset_translator ot;
    storage::log_reader_config reader_config(
      target, target, ss::default_priority_class());

    auto manifest = hydrate_manifest(api, bucket);

    remote_partition partition(manifest, api, *fixture.cache, bucket);

    auto reader = partition.make_reader(reader_config).get();

    auto headers_read
      = reader.consume(test_consumer(), model::no_timeout).get();

    partition.stop().get();

    vlog(test_log.debug, "num headers: {}", headers_read.size());
    BOOST_REQUIRE(headers_read.size() == 1);
    vlog(test_log.debug, "batch found: {}", headers_read.front());
    return headers_read.front();
}

/// Similar to prev function but scans the range of offsets instead of
/// returning a single one
static std::vector<model::record_batch_header> scan_remote_partition(
  cloud_storage_fixture& imposter, model::offset base, model::offset max) {
    auto conf = imposter.get_configuration();
    static auto bucket = s3::bucket_name("bucket");
    remote api(s3_connection_limit(10), conf);
    auto action = ss::defer([&api] { api.stop().get(); });
    auto m = ss::make_lw_shared<cloud_storage::manifest>(
      manifest_ntp, manifest_revision);
    offset_translator ot;
    storage::log_reader_config reader_config(
      base, max, ss::default_priority_class());

    auto manifest = hydrate_manifest(api, bucket);

    remote_partition partition(manifest, api, *imposter.cache, bucket);

    auto reader = partition.make_reader(reader_config).get();

    auto headers_read
      = reader.consume(test_consumer(), model::no_timeout).get();

    partition.stop().get();
    return headers_read;
}

FIXTURE_TEST(
  test_remote_partition_single_batch_0, cloud_storage_fixture) { // NOLINT
    auto segments = setup_s3_imposter(*this, 3, 10);
    auto hdr = read_single_batch_from_remote_partition(*this, model::offset(0));
    BOOST_REQUIRE(hdr.base_offset == model::offset(0));
}

FIXTURE_TEST(
  test_remote_partition_single_batch_1, cloud_storage_fixture) { // NOLINT
    auto segments = setup_s3_imposter(*this, 3, 10);
    auto target = segments[0].max_offset;
    vlog(test_log.debug, "target offset: {}", target);
    print_segments(segments);
    auto hdr = read_single_batch_from_remote_partition(*this, target);
    BOOST_REQUIRE(hdr.last_offset() == target);
}

FIXTURE_TEST(
  test_remote_partition_single_batch_2, cloud_storage_fixture) { // NOLINT
    auto segments = setup_s3_imposter(*this, 3, 10);
    auto target = segments[1].base_offset;
    vlog(test_log.debug, "target offset: {}", target);
    print_segments(segments);
    auto hdr = read_single_batch_from_remote_partition(*this, target);
    BOOST_REQUIRE(hdr.base_offset == target);
}

FIXTURE_TEST(
  test_remote_partition_single_batch_3, cloud_storage_fixture) { // NOLINT
    auto segments = setup_s3_imposter(*this, 3, 10);
    auto target = segments[1].max_offset;
    vlog(test_log.debug, "target offset: {}", target);
    print_segments(segments);
    auto hdr = read_single_batch_from_remote_partition(*this, target);
    BOOST_REQUIRE(hdr.last_offset() == target);
}

FIXTURE_TEST(
  test_remote_partition_single_batch_4, cloud_storage_fixture) { // NOLINT
    auto segments = setup_s3_imposter(*this, 3, 10);
    auto target = segments[2].base_offset;
    vlog(test_log.debug, "target offset: {}", target);
    print_segments(segments);
    auto hdr = read_single_batch_from_remote_partition(*this, target);
    BOOST_REQUIRE(hdr.base_offset == target);
}

FIXTURE_TEST(test_remote_partition_single_batch_5, cloud_storage_fixture) {
    auto segments = setup_s3_imposter(*this, 3, 10);
    auto target = segments[2].max_offset;
    vlog(test_log.debug, "target offset: {}", target);
    print_segments(segments);
    auto hdr = read_single_batch_from_remote_partition(*this, target);
    BOOST_REQUIRE(hdr.last_offset() == target);
}

/// This test scans the entire range of offsets
FIXTURE_TEST(test_remote_partition_scan_full, cloud_storage_fixture) {
    constexpr int batches_per_segment = 10;
    constexpr int num_segments = 3;
    constexpr int total_batches = batches_per_segment * num_segments;

    auto segments = setup_s3_imposter(*this, 3, 10);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;

    vlog(test_log.debug, "offset range: {}-{}", base, max);
    print_segments(segments);

    auto headers_read = scan_remote_partition(*this, base, max);

    BOOST_REQUIRE_EQUAL(headers_read.size(), total_batches);
    auto coverage = get_coverage(headers_read, segments, batches_per_segment);
    auto nmatches = std::count(coverage.begin(), coverage.end(), true);
    BOOST_REQUIRE_EQUAL(nmatches, coverage.size());
}

/// This test scans first half of batches
FIXTURE_TEST(test_remote_partition_scan_first_half, cloud_storage_fixture) {
    constexpr int batches_per_segment = 10;
    constexpr int num_segments = 3;
    constexpr int total_batches = batches_per_segment * num_segments;

    auto segments = setup_s3_imposter(*this, 3, 10);
    auto base = segments[0].base_offset;
    auto max = segments[1].headers[batches_per_segment / 2 - 1].last_offset();

    vlog(test_log.debug, "offset range: {}-{}", base, max);
    print_segments(segments);

    auto headers_read = scan_remote_partition(*this, base, max);

    BOOST_REQUIRE_EQUAL(headers_read.size(), total_batches / 2);
    auto coverage = get_coverage(headers_read, segments, batches_per_segment);
    auto nmatches = std::count(coverage.begin(), coverage.end(), true);
    BOOST_REQUIRE_EQUAL(nmatches, total_batches / 2);
    const std::vector<bool> expected_coverage = {
      true,  true,  true,  true,  true,  true,  true,  true,  true,  true,
      true,  true,  true,  true,  true,  false, false, false, false, false,
      false, false, false, false, false, false, false, false, false, false,
    };
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      coverage.begin(),
      coverage.end(),
      expected_coverage.begin(),
      expected_coverage.end());
}

/// This test scans last half of batches
FIXTURE_TEST(test_remote_partition_scan_second_half, cloud_storage_fixture) {
    constexpr int batches_per_segment = 10;
    constexpr int num_segments = 3;
    constexpr int total_batches = batches_per_segment * num_segments;

    auto segments = setup_s3_imposter(*this, 3, 10);
    auto base = segments[1].headers[batches_per_segment / 2].last_offset();
    auto max = segments[2].max_offset;

    vlog(test_log.debug, "offset range: {}-{}", base, max);
    print_segments(segments);

    auto headers_read = scan_remote_partition(*this, base, max);

    BOOST_REQUIRE_EQUAL(headers_read.size(), total_batches / 2);
    auto coverage = get_coverage(headers_read, segments, batches_per_segment);
    auto nmatches = std::count(coverage.begin(), coverage.end(), true);
    BOOST_REQUIRE_EQUAL(nmatches, total_batches / 2);
    std::vector<bool> expected_coverage = {
      false, false, false, false, false, false, false, false, false, false,
      false, false, false, false, false, true,  true,  true,  true,  true,
      true,  true,  true,  true,  true,  true,  true,  true,  true,  true,
    };
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      coverage.begin(),
      coverage.end(),
      expected_coverage.begin(),
      expected_coverage.end());
}

/// This test scans batches in the middle
FIXTURE_TEST(test_remote_partition_scan_middle, cloud_storage_fixture) {
    constexpr int batches_per_segment = 10;
    constexpr int num_segments = 3;
    constexpr int total_batches = batches_per_segment * num_segments;

    auto segments = setup_s3_imposter(*this, 3, 10);
    auto base = segments[0].headers[batches_per_segment / 2].last_offset();
    auto max = segments[2].headers[batches_per_segment / 2 - 1].last_offset();

    vlog(test_log.debug, "offset range: {}-{}", base, max);
    print_segments(segments);

    auto headers_read = scan_remote_partition(*this, base, max);
    BOOST_REQUIRE_EQUAL(
      headers_read.size(), total_batches - batches_per_segment);
    auto coverage = get_coverage(headers_read, segments, batches_per_segment);
    auto nmatches = std::count(coverage.begin(), coverage.end(), true);
    BOOST_REQUIRE_EQUAL(nmatches, total_batches - batches_per_segment);
    std::vector<bool> expected_coverage = {
      false, false, false, false, false, true,  true,  true,  true,  true,
      true,  true,  true,  true,  true,  true,  true,  true,  true,  true,
      true,  true,  true,  true,  true,  false, false, false, false, false,
    };
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      coverage.begin(),
      coverage.end(),
      expected_coverage.begin(),
      expected_coverage.end());
}

/// This test scans batches in the middle
FIXTURE_TEST(test_remote_partition_scan_off, cloud_storage_fixture) {
    constexpr int batches_per_segment = 10;
    constexpr int num_segments = 3;
    constexpr int total_batches = batches_per_segment * num_segments;

    auto segments = setup_s3_imposter(*this, 3, 10);
    auto base = segments[2].max_offset + model::offset(10);
    auto max = base + model::offset(10);

    vlog(test_log.debug, "offset range: {}-{}", base, max);
    print_segments(segments);

    auto headers_read = scan_remote_partition(*this, base, max);
    BOOST_REQUIRE_EQUAL(headers_read.size(), 0);
}
