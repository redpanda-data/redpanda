/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "bytes/iostream.h"
#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_partition.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/tests/cloud_storage_fixture.h"
#include "cloud_storage/tests/common_def.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/tests/util.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "seastarx.h"
#include "ssx/future-util.h"
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

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <chrono>
#include <exception>
#include <iterator>
#include <numeric>
#include <system_error>

using namespace std::chrono_literals;
using namespace cloud_storage;

inline ss::logger test_log("test"); // NOLINT

static void print_segments(const std::vector<in_memory_segment>& segments) {
    for (const auto& s : segments) {
        vlog(test_log.debug, "segment: {}", s);
    }
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
    size_t num_filtered = 0;
    for (size_t i = 0; i < num_record_batches; i++) {
        const auto& hh = headers[hix];
        auto sh = segments.at(i / batches_per_segment)
                    .headers.at(i % batches_per_segment);
        if (sh.type != model::record_batch_type::raft_data) {
            num_filtered++;
            continue;
        }
        if (num_filtered != 0) {
            // adjust base offset to compensate for removed record batches
            // fix crc so comparison would work as expected
            sh.base_offset = sh.base_offset - model::offset(num_filtered);
            sh.header_crc = model::internal_header_only_crc(sh);
        }
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

/// Consumer that accepts fixed number of record
/// batches.
class counting_batch_consumer final {
public:
    explicit counting_batch_consumer(size_t count)
      : _count(count) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        vlog(test_log.debug, "record batch #{}: {}", headers.size(), b);
        headers.push_back(b.header());
        if (headers.size() == _count) {
            co_return ss::stop_iteration::yes;
        }
        co_return ss::stop_iteration::no;
    }

    std::vector<model::record_batch_header> end_of_stream() {
        return std::move(headers);
    }

    size_t _count;
    std::vector<model::record_batch_header> headers;
};

/// This test reads only a tip of the log
static model::record_batch_header read_single_batch_from_remote_partition(
  cloud_storage_fixture& fixture,
  model::offset target,
  bool expect_exists = true) {
    auto conf = fixture.get_configuration();
    static auto bucket = cloud_storage_clients::bucket_name("bucket");
    auto m = ss::make_lw_shared<cloud_storage::partition_manifest>(
      manifest_ntp, manifest_revision);
    storage::log_reader_config reader_config(
      target, target, ss::default_priority_class());

    auto manifest = hydrate_manifest(fixture.api.local(), bucket);

    auto partition = ss::make_shared<remote_partition>(
      manifest, fixture.api.local(), fixture.cache.local(), bucket);
    auto partition_stop = ss::defer([&partition] { partition->stop().get(); });

    auto reader = partition->make_reader(reader_config).get().reader;

    auto headers_read
      = reader.consume(test_consumer(), model::no_timeout).get();

    vlog(
      test_log.debug,
      "num headers for offset {}: {}",
      target,
      headers_read.size());
    if (expect_exists) {
        BOOST_REQUIRE(headers_read.size() == 1);
        vlog(test_log.debug, "batch found: {}", headers_read.front());
        return headers_read.front();
    }
    BOOST_REQUIRE(headers_read.empty());
    return model::record_batch_header{};
}

// Returns true if a kafka::offset scan returns the expected existence of the
// record.
bool check_fetch(
  cloud_storage_fixture& fixture, kafka::offset ko, bool expect_exists) {
    auto hdr = read_single_batch_from_remote_partition(
      fixture, kafka::offset_cast(ko), expect_exists);
    bool exists = hdr.record_count > 0;
    bool ret = expect_exists == exists;
    if (!ret) {
        test_log.error("Unexpected result", hdr);
    }
    return ret;
}

// Returns true if a kafka::offset scan returns the expected number of records.
bool check_scan(
  cloud_storage_fixture& fixture, kafka::offset ko, int expected_num_records) {
    auto seg_hdrs = scan_remote_partition(fixture, kafka::offset_cast(ko));
    int num_data_records = 0;
    for (const auto& hdr : seg_hdrs) {
        num_data_records += hdr.record_count;
    }
    auto ret = expected_num_records == num_data_records;
    if (!ret) {
        test_log.error(
          "Expected {} records, got {}: {}",
          expected_num_records,
          num_data_records,
          seg_hdrs);
    }
    return ret;
}

FIXTURE_TEST(test_scan_by_kafka_offset, cloud_storage_fixture) {
    // mo: 0     5 6    11 12   17 18
    //     [a    ] [b    ] [c    ] end
    // ko: 0     2 3     5 6     8 9
    batch_t data = {
      .num_records = 1, .type = model::record_batch_type::raft_data};
    batch_t conf = {
      .num_records = 1, .type = model::record_batch_type::raft_configuration};
    const std::vector<std::vector<batch_t>> batch_types = {
      {conf, conf, conf, data, data, data},
      {conf, conf, conf, data, data, data},
      {conf, conf, conf, data, data, data},
    };
    auto segments = setup_s3_imposter(
      *this, model::offset(0), model::offset_delta(0), batch_types);
    print_segments(segments);
    for (int i = 0; i <= 8; i++) {
        BOOST_REQUIRE(check_scan(*this, kafka::offset(i), 9 - i));
        BOOST_REQUIRE(check_fetch(*this, kafka::offset(i), true));
    }
    BOOST_REQUIRE(check_scan(*this, kafka::offset(9), 0));
    BOOST_REQUIRE(check_fetch(*this, kafka::offset(9), false));
    BOOST_REQUIRE(check_scan(*this, kafka::offset(10), 0));
    BOOST_REQUIRE(check_fetch(*this, kafka::offset(10), false));
}

FIXTURE_TEST(test_scan_by_kafka_offset_truncated, cloud_storage_fixture) {
    // mo: 6    11 12   17
    //     [b    ] [c    ] end
    // ko: 3     5 6     8
    batch_t data = {
      .num_records = 1, .type = model::record_batch_type::raft_data};
    batch_t conf = {
      .num_records = 1, .type = model::record_batch_type::raft_configuration};
    const std::vector<std::vector<batch_t>> batch_types = {
      {conf, conf, conf, data, data, data},
      {conf, conf, conf, data, data, data},
    };
    auto segments = setup_s3_imposter(
      *this, model::offset(6), model::offset_delta(3), batch_types);
    print_segments(segments);
    for (int i = 0; i <= 2; i++) {
        BOOST_REQUIRE(check_scan(*this, kafka::offset(i), 6));
        BOOST_REQUIRE(check_fetch(*this, kafka::offset(i), false));
    }
    for (int i = 3; i <= 8; i++) {
        BOOST_REQUIRE(check_scan(*this, kafka::offset(i), 9 - i));
        BOOST_REQUIRE(check_fetch(*this, kafka::offset(i), true));
    }
    BOOST_REQUIRE(check_scan(*this, kafka::offset(9), 0));
    BOOST_REQUIRE(check_fetch(*this, kafka::offset(9), false));
    BOOST_REQUIRE(check_scan(*this, kafka::offset(10), 0));
    BOOST_REQUIRE(check_fetch(*this, kafka::offset(10), false));
}

FIXTURE_TEST(test_scan_by_kafka_offset_repeats, cloud_storage_fixture) {
    // mo: 0     5 6    11 12   17
    //     [a    ] [b    ] [c    ] end
    // ko: 0     2 3     3 3     3
    batch_t data = {
      .num_records = 1, .type = model::record_batch_type::raft_data};
    batch_t conf = {
      .num_records = 1, .type = model::record_batch_type::raft_configuration};
    const std::vector<std::vector<batch_t>> batch_types = {
      {conf, conf, conf, data, data, data},
      {conf, conf, conf, conf, conf, conf},
      {data, conf, conf, conf, conf, conf},
    };
    auto segments = setup_s3_imposter(
      *this, model::offset(0), model::offset_delta(0), batch_types);
    print_segments(segments);
    for (int i = 0; i <= 3; i++) {
        BOOST_REQUIRE(check_scan(*this, kafka::offset(i), 4 - i));
        BOOST_REQUIRE(check_fetch(*this, kafka::offset(i), true));
    }
    BOOST_REQUIRE(check_scan(*this, kafka::offset(4), 0));
    BOOST_REQUIRE(check_fetch(*this, kafka::offset(4), false));
}
FIXTURE_TEST(
  test_scan_by_kafka_offset_repeats_truncated, cloud_storage_fixture) {
    // mo: 6    11 12   17
    //     [b    ] [c    ] end
    // ko: 3     3 3     3
    batch_t data = {
      .num_records = 1, .type = model::record_batch_type::raft_data};
    batch_t conf = {
      .num_records = 1, .type = model::record_batch_type::raft_configuration};
    const std::vector<std::vector<batch_t>> batch_types = {
      {conf, conf, conf, conf, conf, conf},
      {data, conf, conf, conf, conf, conf},
    };
    auto segments = setup_s3_imposter(
      *this, model::offset(6), model::offset_delta(3), batch_types);
    print_segments(segments);
    BOOST_REQUIRE(check_scan(*this, kafka::offset(2), 1));
    BOOST_REQUIRE(check_fetch(*this, kafka::offset(2), false));
    BOOST_REQUIRE(check_scan(*this, kafka::offset(3), 1));
    BOOST_REQUIRE(check_fetch(*this, kafka::offset(3), true));
    BOOST_REQUIRE(check_scan(*this, kafka::offset(4), 0));
    BOOST_REQUIRE(check_fetch(*this, kafka::offset(4), false));
}

FIXTURE_TEST(test_scan_by_kafka_offset_same_bounds, cloud_storage_fixture) {
    // mo: 0     5 6    11 12   17 18
    //     [a    ] [b    ] [c    ] end
    // ko: 0     5 6     6 6     6 7
    batch_t data = {
      .num_records = 1, .type = model::record_batch_type::raft_data};
    batch_t conf = {
      .num_records = 1, .type = model::record_batch_type::raft_configuration};
    const std::vector<std::vector<batch_t>> batch_types = {
      {data, data, data, data, data, data},
      {conf, conf, conf, conf, conf, conf},
      {data, conf, conf, conf, conf, conf},
    };
    auto segments = setup_s3_imposter(
      *this, model::offset(0), model::offset_delta(0), batch_types);
    print_segments(segments);
    for (int i = 0; i <= 6; i++) {
        BOOST_REQUIRE(check_scan(*this, kafka::offset(i), 7 - i));
        BOOST_REQUIRE(check_fetch(*this, kafka::offset(i), true));
    }
    BOOST_REQUIRE(check_scan(*this, kafka::offset(7), 0));
    BOOST_REQUIRE(check_fetch(*this, kafka::offset(7), false));
}

FIXTURE_TEST(
  test_scan_by_kafka_offset_same_bounds_truncated, cloud_storage_fixture) {
    // mo: 6    11 12   17
    //     [b    ] [c    ] end
    // ko: 6     6 6     6
    batch_t data = {
      .num_records = 1, .type = model::record_batch_type::raft_data};
    batch_t conf = {
      .num_records = 1, .type = model::record_batch_type::raft_configuration};
    const std::vector<std::vector<batch_t>> batch_types = {
      {conf, conf, conf, conf, conf, conf},
      {data, conf, conf, conf, conf, conf},
    };
    auto segments = setup_s3_imposter(
      *this, model::offset(6), model::offset_delta(0), batch_types);
    print_segments(segments);
    for (int i = 0; i < 6; i++) {
        BOOST_REQUIRE(check_scan(*this, kafka::offset(i), 1));
        BOOST_REQUIRE(check_fetch(*this, kafka::offset(i), false));
    }
    BOOST_REQUIRE(check_scan(*this, kafka::offset(6), 1));
    BOOST_REQUIRE(check_fetch(*this, kafka::offset(6), true));
    BOOST_REQUIRE(check_scan(*this, kafka::offset(7), 0));
    BOOST_REQUIRE(check_fetch(*this, kafka::offset(7), false));
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

FIXTURE_TEST(
  test_remote_partition_single_batch_truncated_segments,
  cloud_storage_fixture) {
    auto segments = setup_s3_imposter(
      *this, 3, 10, manifest_inconsistency::truncated_segments);
    auto target = segments[2].max_offset;
    vlog(test_log.debug, "target offset: {}", target);
    print_segments(segments);
    BOOST_REQUIRE_THROW(
      read_single_batch_from_remote_partition(*this, target),
      std::system_error);
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

/// This test scans the entire range of offsets
FIXTURE_TEST(
  test_remote_partition_scan_full_truncated_segments, cloud_storage_fixture) {
    constexpr int num_segments = 3;

    auto segments = setup_s3_imposter(
      *this, 3, 10, manifest_inconsistency::truncated_segments);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;

    vlog(test_log.debug, "offset range: {}-{}", base, max);
    print_segments(segments);

    BOOST_REQUIRE_THROW(
      scan_remote_partition(*this, base, max), std::system_error);
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
    vlog(test_log.debug, "scan results: \n\n");
    int hdr_ix = 0;
    for (const auto& hdr : headers_read) {
        vlog(test_log.debug, "header at pos {}: {}", hdr_ix, hdr);
        hdr_ix++;
    }

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
    auto segments = setup_s3_imposter(*this, 3, 10);
    auto base = segments[2].max_offset + model::offset(10);
    auto max = base + model::offset(10);

    vlog(test_log.debug, "offset range: {}-{}", base, max);
    print_segments(segments);

    auto headers_read = scan_remote_partition(*this, base, max);
    BOOST_REQUIRE_EQUAL(headers_read.size(), 0);
}

/// This test scans the entire range of offsets
FIXTURE_TEST(
  test_remote_partition_scan_translate_full_1, cloud_storage_fixture) {
    constexpr int batches_per_segment = 10;
    constexpr int num_segments = 3;
    constexpr int total_batches = batches_per_segment * num_segments;
    batch_t data = {
      .num_records = 10, .type = model::record_batch_type::raft_data};
    batch_t conf = {
      .num_records = 1, .type = model::record_batch_type::raft_configuration};
    const std::vector<std::vector<batch_t>> batch_types = {
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
    };

    auto segments = setup_s3_imposter(*this, batch_types);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;

    vlog(test_log.debug, "offset range: {}-{}", base, max);
    print_segments(segments);

    auto headers_read = scan_remote_partition(*this, base, max);

    BOOST_REQUIRE_EQUAL(headers_read.size(), total_batches - 3);
    auto coverage = get_coverage(headers_read, segments, batches_per_segment);
    std::vector<bool> expected_coverage = {
      false, true, true, true, true, true, true, true, true, true,
      false, true, true, true, true, true, true, true, true, true,
      false, true, true, true, true, true, true, true, true, true,
    };
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      coverage.begin(),
      coverage.end(),
      expected_coverage.begin(),
      expected_coverage.end());
}

/// This test scans the entire range of offsets
FIXTURE_TEST(
  test_remote_partition_scan_translate_full_2, cloud_storage_fixture) {
    constexpr int batches_per_segment = 10;
    constexpr int num_segments = 3;
    constexpr int total_batches = batches_per_segment * num_segments;
    batch_t data = {
      .num_records = 10, .type = model::record_batch_type::raft_data};
    batch_t conf = {
      .num_records = 1, .type = model::record_batch_type::raft_configuration};
    const std::vector<std::vector<batch_t>> batch_types = {
      {conf, conf, conf, conf, conf, data, data, data, data, data},
      {conf, conf, conf, conf, conf, data, data, data, data, data},
      {conf, conf, conf, conf, conf, data, data, data, data, data},
    };

    auto segments = setup_s3_imposter(*this, batch_types);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;

    vlog(test_log.debug, "offset range: {}-{}", base, max);
    print_segments(segments);

    auto headers_read = scan_remote_partition(*this, base, max);

    BOOST_REQUIRE_EQUAL(headers_read.size(), total_batches - 15);
    auto coverage = get_coverage(headers_read, segments, batches_per_segment);
    std::vector<bool> expected_coverage = {
      false, false, false, false, false, true, true, true, true, true,
      false, false, false, false, false, true, true, true, true, true,
      false, false, false, false, false, true, true, true, true, true,
    };
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      coverage.begin(),
      coverage.end(),
      expected_coverage.begin(),
      expected_coverage.end());
}

/// This test scans the entire range of offsets
FIXTURE_TEST(
  test_remote_partition_scan_translate_full_3, cloud_storage_fixture) {
    constexpr int batches_per_segment = 10;
    constexpr int num_segments = 3;
    constexpr int total_batches = batches_per_segment * num_segments;
    batch_t data = {
      .num_records = 10, .type = model::record_batch_type::raft_data};
    batch_t conf = {
      .num_records = 1, .type = model::record_batch_type::raft_configuration};
    const std::vector<std::vector<batch_t>> batch_types = {
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, conf, conf, conf, conf, conf, conf, conf, conf, conf},
      {conf, data, data, data, data, data, data, data, data, data},
    };

    auto segments = setup_s3_imposter(*this, batch_types);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;

    vlog(test_log.debug, "offset range: {}-{}", base, max);
    print_segments(segments);

    auto headers_read = scan_remote_partition(*this, base, max);

    BOOST_REQUIRE_EQUAL(headers_read.size(), total_batches - 12);
    auto coverage = get_coverage(headers_read, segments, batches_per_segment);
    std::vector<bool> expected_coverage = {
      false, true,  true,  true,  true,  true,  true,  true,  true,  true,
      false, false, false, false, false, false, false, false, false, false,
      false, true,  true,  true,  true,  true,  true,  true,  true,  true,
    };
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      coverage.begin(),
      coverage.end(),
      expected_coverage.begin(),
      expected_coverage.end());
}

/// This test scans the entire range of offsets
FIXTURE_TEST(
  test_remote_partition_scan_translate_full_4, cloud_storage_fixture) {
    constexpr int batches_per_segment = 10;
    constexpr int num_segments = 3;
    batch_t data = {
      .num_records = 10, .type = model::record_batch_type::raft_data};
    batch_t conf = {
      .num_records = 1, .type = model::record_batch_type::raft_configuration};
    const std::vector<std::vector<batch_t>> batch_types = {
      {conf, conf, conf, conf, conf, conf, conf, conf, conf, conf},
      {conf, conf, conf, conf, conf, conf, conf, conf, conf, conf},
      {conf, conf, conf, conf, conf, conf, conf, conf, conf, data},
    };

    auto segments = setup_s3_imposter(*this, batch_types);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;

    vlog(test_log.debug, "offset range: {}-{}", base, max);
    print_segments(segments);

    auto headers_read = scan_remote_partition(*this, base, max);

    BOOST_REQUIRE_EQUAL(headers_read.size(), 1);
    auto coverage = get_coverage(headers_read, segments, batches_per_segment);
    std::vector<bool> expected_coverage = {
      false, false, false, false, false, false, false, false, false, false,
      false, false, false, false, false, false, false, false, false, false,
      false, false, false, false, false, false, false, false, false, true,
    };
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      coverage.begin(),
      coverage.end(),
      expected_coverage.begin(),
      expected_coverage.end());
}

/// This test scans the entire range of offsets
FIXTURE_TEST(
  test_remote_partition_scan_translate_full_5, cloud_storage_fixture) {
    constexpr int num_segments = 3;
    batch_t data = {
      .num_records = 10, .type = model::record_batch_type::raft_data};
    batch_t conf = {
      .num_records = 1, .type = model::record_batch_type::raft_configuration};
    const std::vector<std::vector<batch_t>> batch_types = {
      {conf, conf, conf, conf, conf, conf, conf, conf, conf, data},
      {conf},
      {conf, conf, conf, conf, conf, conf, conf, conf, conf, conf},
    };

    auto segments = setup_s3_imposter(*this, batch_types);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;

    vlog(test_log.debug, "offset range: {}-{}", base, max);
    print_segments(segments);

    auto headers_read = scan_remote_partition(*this, base, max);

    BOOST_REQUIRE_EQUAL(headers_read.size(), 1);
}

/// This test scans the entire range of offsets
FIXTURE_TEST(
  test_remote_partition_scan_translate_full_6, cloud_storage_fixture) {
    constexpr int num_segments = 3;
    batch_t data = {
      .num_records = 10, .type = model::record_batch_type::raft_data};
    batch_t conf = {
      .num_records = 1, .type = model::record_batch_type::raft_configuration};
    const std::vector<std::vector<batch_t>> batch_types = {
      {conf, conf, conf, conf, conf, conf, conf, conf, conf, conf},
      {conf, conf, conf, conf, conf, conf, conf, conf, conf, conf},
      {conf, conf, conf, conf, conf, conf, conf, conf, conf, conf},
    };

    auto segments = setup_s3_imposter(*this, batch_types);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;

    vlog(test_log.debug, "offset range: {}-{}", base, max);
    print_segments(segments);

    auto headers_read = scan_remote_partition(*this, base, max);

    BOOST_REQUIRE_EQUAL(headers_read.size(), 0);
}

FIXTURE_TEST(test_remote_partition_read_cached_index, cloud_storage_fixture) {
    // This test checks index materialization code path.
    // It's triggered when the segment is already present in the cache
    // when the remote_segment is created.
    // In oreder to have the segment hydrated we need to access it first and
    // then wait until eviction will collect unused remote_segment (60s).
    // This is unreliable and lengthy, so instead of doing this this test
    // uses two remote_partition instances. First one hydrates segment in
    // the cache. The second one is used to materialize the segment.
    constexpr int num_segments = 3;
    batch_t batch = {
      .num_records = 5,
      .type = model::record_batch_type::raft_data,
      .record_sizes = {100, 200, 300, 200, 100},
    };
    std::vector<std::vector<batch_t>> batches = {
      {batch, batch, batch},
      {batch, batch, batch},
      {batch, batch, batch},
    };
    auto segments = setup_s3_imposter(*this, batches);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;
    vlog(test_log.debug, "offset range: {}-{}", base, max);

    auto conf = get_configuration();
    auto bucket = cloud_storage_clients::bucket_name("bucket");
    auto m = ss::make_lw_shared<cloud_storage::partition_manifest>(
      manifest_ntp, manifest_revision);

    auto manifest = hydrate_manifest(api.local(), bucket);

    // starting max_bytes
    constexpr size_t max_bytes_limit = 4_KiB;

    // Read first segment using first remote_partition instance.
    // After this block finishes the segment will be hydrated.
    {
        auto partition = ss::make_shared<remote_partition>(
          manifest, api.local(), cache.local(), bucket);
        auto partition_stop = ss::defer(
          [&partition] { partition->stop().get(); });
        partition->start().get();

        storage::log_reader_config reader_config(
          base, max, ss::default_priority_class());

        reader_config.start_offset = segments.front().base_offset;
        reader_config.max_bytes = max_bytes_limit;
        vlog(test_log.info, "read first segment {}", reader_config);
        auto reader = partition->make_reader(reader_config).get().reader;
        auto headers_read
          = reader.consume(test_consumer(), model::no_timeout).get();
        BOOST_REQUIRE(!headers_read.empty());
    }

    // Read first segment using second remote_partition instance.
    // This will trigger offset_index materialization from cache.
    {
        auto partition = ss::make_shared<remote_partition>(
          manifest, api.local(), cache.local(), bucket);
        auto partition_stop = ss::defer(
          [&partition] { partition->stop().get(); });
        partition->start().get();

        storage::log_reader_config reader_config(
          base, max, ss::default_priority_class());

        reader_config.start_offset = segments.front().base_offset;
        reader_config.max_bytes = max_bytes_limit;
        vlog(test_log.info, "read last segment: {}", reader_config);
        auto reader = partition->make_reader(reader_config).get().reader;
        auto headers_read
          = reader.consume(test_consumer(), model::no_timeout).get();
        BOOST_REQUIRE(!headers_read.empty());
    }
}

static void remove_segment_from_s3(
  const cloud_storage::partition_manifest& m,
  model::offset o,
  cloud_storage::remote& api,
  const cloud_storage_clients::bucket_name& bucket) {
    static ss::abort_source never_abort;

    auto meta = m.get(o);
    BOOST_REQUIRE(meta != nullptr);
    auto path = m.generate_segment_path(*meta);
    retry_chain_node fib(never_abort, 60s, 1s);
    auto res = api
                 .delete_object(
                   bucket, cloud_storage_clients::object_key(path()), fib)
                 .get();
    BOOST_REQUIRE(res == cloud_storage::upload_result::success);
}

/// This test scans the entire range of offsets
FIXTURE_TEST(test_remote_partition_concurrent_truncate, cloud_storage_fixture) {
    constexpr int num_segments = 10;
    batch_t data = {
      .num_records = 10, .type = model::record_batch_type::raft_data};

    const std::vector<std::vector<batch_t>> batch_types = {
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
    };

    auto segments = setup_s3_imposter(*this, batch_types);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;

    vlog(test_log.debug, "offset range: {}-{}", base, max);

    // create a reader that consumes segments one by one
    static auto bucket = cloud_storage_clients::bucket_name("bucket");

    auto manifest = hydrate_manifest(api.local(), bucket);

    auto partition = ss::make_shared<remote_partition>(
      manifest, api.local(), cache.local(), bucket);
    auto partition_stop = ss::defer([&partition] { partition->stop().get(); });

    partition->start().get();

    {
        ss::abort_source as;
        storage::log_reader_config reader_config(
          base,
          max,
          0,
          std::numeric_limits<size_t>::max(),
          ss::default_priority_class(),
          std::nullopt,
          std::nullopt,
          as);

        // Start consuming before truncation, only consume one batch
        auto reader = partition->make_reader(reader_config).get().reader;
        auto headers_read
          = reader.consume(counting_batch_consumer(1), model::no_timeout).get();

        BOOST_REQUIRE(headers_read.size() == 1);
        BOOST_REQUIRE(headers_read.front().base_offset == model::offset(0));

        remove_segment_from_s3(manifest, model::offset(0), api.local(), bucket);
        BOOST_REQUIRE(manifest.advance_start_offset(model::offset(400)));
        manifest.truncate();
        manifest.advance_insync_offset(model::offset(10000));
        vlog(
          test_log.debug,
          "cloud_storage truncate manifest to {}",
          manifest.get_start_offset().value());

        // Try to consume remaining 99 batches. This reader should only be able
        // to consume from the cached segment, so only 9 batches will be present
        // in the list.
        headers_read
          = reader.consume(counting_batch_consumer(99), model::no_timeout)
              .get();
        std::move(reader).release();
        BOOST_REQUIRE_EQUAL(headers_read.size(), 9);
    }

    {
        ss::abort_source as;
        storage::log_reader_config reader_config(
          base,
          max,
          0,
          std::numeric_limits<size_t>::max(),
          ss::default_priority_class(),
          std::nullopt,
          std::nullopt,
          as);

        vlog(test_log.debug, "Creating new reader {}", reader_config);

        // After truncation reading from the old end should be impossible
        auto reader = partition->make_reader(reader_config).get().reader;
        auto headers_read
          = reader.consume(counting_batch_consumer(100), model::no_timeout)
              .get();

        BOOST_REQUIRE_EQUAL(headers_read.size(), 60);
        BOOST_REQUIRE_EQUAL(
          headers_read.front().base_offset, model::offset(400));
    }
}

FIXTURE_TEST(
  test_remote_partition_query_below_cutoff_point, cloud_storage_fixture) {
    constexpr int num_segments = 10;
    batch_t data = {
      .num_records = 10, .type = model::record_batch_type::raft_data};

    const std::vector<std::vector<batch_t>> batch_types = {
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
    };

    auto segments = setup_s3_imposter(*this, batch_types);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;

    vlog(test_log.debug, "offset range: {}-{}", base, max);

    // create a reader that consumes segments one by one
    static auto bucket = cloud_storage_clients::bucket_name("bucket");

    auto manifest = hydrate_manifest(api.local(), bucket);

    auto partition = ss::make_shared<remote_partition>(
      manifest, api.local(), cache.local(), bucket);
    auto partition_stop = ss::defer([&partition] { partition->stop().get(); });

    partition->start().get();

    model::offset cutoff_offset(500);

    remove_segment_from_s3(manifest, model::offset(0), api.local(), bucket);
    BOOST_REQUIRE(manifest.advance_start_offset(cutoff_offset));
    manifest.truncate();
    manifest.advance_insync_offset(model::offset(10000));
    vlog(
      test_log.debug,
      "cloud_storage truncate manifest to {}",
      manifest.get_start_offset().value());

    {
        ss::abort_source as;
        storage::log_reader_config reader_config(
          model::offset(200),
          model::offset(299),
          0,
          std::numeric_limits<size_t>::max(),
          ss::default_priority_class(),
          std::nullopt,
          std::nullopt,
          as);

        vlog(test_log.debug, "Creating new reader {}", reader_config);

        // After truncation reading from the old end should be impossible
        auto reader = partition->make_reader(reader_config).get().reader;
        auto headers_read
          = reader.consume(counting_batch_consumer(100), model::no_timeout)
              .get();

        BOOST_REQUIRE(headers_read.size() == 0);
    }
}

FIXTURE_TEST(
  test_remote_partition_compacted_segments_reupload, cloud_storage_fixture) {
    constexpr int num_segments = 10;
    batch_t data = {
      .num_records = 10, .type = model::record_batch_type::raft_data};

    const std::vector<std::vector<batch_t>> non_compacted_layout = {
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data},
    };

    auto segments = setup_s3_imposter(*this, non_compacted_layout);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;
    vlog(test_log.debug, "offset range: {}-{}", base, max);

    const std::vector<std::vector<batch_t>> compacted_layout = {
      {data, data, data, data, data, data, data, data, data, data,
       data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data,
       data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data,
       data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data,
       data, data, data, data, data, data, data, data, data, data},
      {data, data, data, data, data, data, data, data, data, data,
       data, data, data, data, data, data, data, data, data, data},
    };

    auto compacted_segments = make_segments(compacted_layout);

    // create a reader that consumes segments one by one
    static auto bucket = cloud_storage_clients::bucket_name("bucket");

    auto manifest = hydrate_manifest(api.local(), bucket);

    auto partition = ss::make_shared<remote_partition>(
      manifest, api.local(), cache.local(), bucket);
    auto partition_stop = ss::defer([&partition] { partition->stop().get(); });

    partition->start().get();

    // No re-uploads yet

    {
        ss::abort_source as;
        storage::log_reader_config reader_config(
          base,
          max,
          0,
          std::numeric_limits<size_t>::max(),
          ss::default_priority_class(),
          std::nullopt,
          std::nullopt,
          as);

        // Start consuming before truncation, only consume one batch
        auto reader = partition->make_reader(reader_config).get().reader;
        auto headers_read
          = reader.consume(counting_batch_consumer(1000), model::no_timeout)
              .get();

        BOOST_REQUIRE(headers_read.size() == 100);
        BOOST_REQUIRE(headers_read.front().base_offset == model::offset(0));
    }

    // Re-upload some of the segments

    {
        ss::abort_source as;
        storage::log_reader_config reader_config(
          base,
          max,
          0,
          std::numeric_limits<size_t>::max(),
          ss::default_priority_class(),
          std::nullopt,
          std::nullopt,
          as);

        // Start consuming before truncation, only consume one batch
        auto reader = partition->make_reader(reader_config).get().reader;
        auto headers_read
          = reader.consume(counting_batch_consumer(50), model::no_timeout)
              .get();

        BOOST_REQUIRE_EQUAL(headers_read.size(), 50);
        BOOST_REQUIRE_EQUAL(headers_read.front().base_offset, model::offset(0));
        BOOST_REQUIRE_EQUAL(
          headers_read.back().base_offset, model::offset(490));

        for (int i = 0; i < 10; i++) {
            const int batches_per_segment = 100;
            remove_segment_from_s3(
              manifest,
              model::offset(i * batches_per_segment),
              api.local(),
              bucket);
        }
        reupload_compacted_segments(*this, manifest, compacted_segments);
        manifest.advance_insync_offset(model::offset(10000));

        headers_read
          = reader.consume(counting_batch_consumer(50), model::no_timeout)
              .get();

        BOOST_REQUIRE_EQUAL(headers_read.size(), 50);
        BOOST_REQUIRE_EQUAL(
          headers_read.front().base_offset, model::offset(500));
        BOOST_REQUIRE_EQUAL(
          headers_read.back().base_offset, model::offset(990));
    }
}

static std::vector<size_t> client_batch_sizes = {
  0,
  10,
  50,
  100,
  200,
  300,
  400,
  500,
  600,
  700,
  800,
  900,
  1000,
  2000,
  3000,
  4000,
  5000,
  6000};

/// This test scans the entire range of offsets
FIXTURE_TEST(
  test_remote_partition_scan_translate_tx_fence, cloud_storage_fixture) {
    constexpr int batches_per_segment = 10;
    constexpr int num_segments = 9;
    constexpr int total_batches = batches_per_segment * num_segments;
    batch_t data = {
      .num_records = 1, .type = model::record_batch_type::raft_data};
    batch_t conf = {
      .num_records = 1, .type = model::record_batch_type::raft_configuration};
    batch_t tx_fence = {
      .num_records = 1, .type = model::record_batch_type::tx_fence};
    const std::vector<std::vector<batch_t>> batch_types = {
      {conf, tx_fence, data, data, data, data, data, data, data, data},
      {conf, data, tx_fence, data, data, data, data, data, data, data},
      {conf, data, data, tx_fence, data, data, data, data, data, data},
      {conf, data, data, data, tx_fence, data, data, data, data, data},
      {conf, data, data, data, data, tx_fence, data, data, data, data},
      {conf, data, data, data, data, data, tx_fence, data, data, data},
      {conf, data, data, data, data, data, data, tx_fence, data, data},
      {conf, data, data, data, data, data, data, data, tx_fence, data},
      {conf, data, data, data, data, data, data, data, data, tx_fence},
    };

    auto num_conf_batches = 0;
    auto num_tx_batches = 0;
    for (const auto& segment : batch_types) {
        for (const auto& b : segment) {
            if (b.type == model::record_batch_type::raft_configuration) {
                num_conf_batches++;
            } else if (b.type == model::record_batch_type::tx_fence) {
                num_tx_batches++;
            }
        }
    }

    auto segments = setup_s3_imposter(
      *this, batch_types, manifest_inconsistency::none);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;

    vlog(test_log.debug, "offset range: {}-{}", base, max);
    print_segments(segments);

    for (size_t sz : client_batch_sizes) {
        auto headers_read = scan_remote_partition_incrementally(
          *this, base, max, sz, 5, 5);

        BOOST_REQUIRE_EQUAL(
          headers_read.size(),
          total_batches - num_conf_batches - num_tx_batches);
    }
}

/// This test scans the partition with overlapping segments
FIXTURE_TEST(
  test_remote_partition_scan_translate_overlap_1, cloud_storage_fixture) {
    constexpr int batches_per_segment = 10;
    constexpr int num_segments = 10;
    constexpr int total_batches = batches_per_segment * num_segments;
    batch_t data = {
      .num_records = 1, .type = model::record_batch_type::raft_data};
    batch_t conf = {
      .num_records = 1, .type = model::record_batch_type::raft_configuration};
    const std::vector<std::vector<batch_t>> batch_types = {
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
    };

    auto num_conf_batches = 0;
    for (const auto& segment : batch_types) {
        for (const auto& b : segment) {
            if (b.type == model::record_batch_type::raft_configuration) {
                num_conf_batches++;
            }
        }
    }

    auto segments = setup_s3_imposter(
      *this, batch_types, manifest_inconsistency::overlapping_segments);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;

    vlog(test_log.debug, "offset range: {}-{}", base, max);
    print_segments(segments);

    for (size_t sz : client_batch_sizes) {
        auto headers_read = scan_remote_partition_incrementally(
          *this, base, max, sz);

        BOOST_REQUIRE_EQUAL(
          headers_read.size(), total_batches - num_conf_batches);
    }
}

/// This test scans the partition with duplicates
FIXTURE_TEST(
  test_remote_partition_scan_translate_with_duplicates_1,
  cloud_storage_fixture) {
    constexpr int batches_per_segment = 10;
    constexpr int num_segments = 10;
    constexpr int num_segments_with_duplicates = num_segments * 2;
    constexpr int total_batches = batches_per_segment * num_segments;
    batch_t data = {
      .num_records = 10, .type = model::record_batch_type::raft_data};
    batch_t conf = {
      .num_records = 1, .type = model::record_batch_type::raft_configuration};
    const std::vector<std::vector<batch_t>> batch_types = {
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
      {conf, data, data, data, data, data, data, data, data, data},
    };

    auto num_conf_batches = 0;
    auto num_data_batches = 0;
    for (const auto& segment : batch_types) {
        for (const auto& b : segment) {
            if (b.type == model::record_batch_type::raft_configuration) {
                num_conf_batches++;
            } else {
                num_data_batches++;
            }
        }
    }

    auto segments = setup_s3_imposter(
      *this, batch_types, manifest_inconsistency::duplicate_offset_ranges);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments_with_duplicates - 1].max_offset;

    vlog(test_log.debug, "offset range: {}-{}", base, max);
    print_segments(segments);

    for (size_t bsize : client_batch_sizes) {
        auto headers_read = scan_remote_partition_incrementally(
          *this, base, max, bsize);
        if (headers_read.size() != num_data_batches) {
            vlog(
              test_log.error,
              "Number of headers read: {}, expected: {}",
              headers_read.size(),
              total_batches - num_conf_batches);
            for (const auto& hdr : headers_read) {
                vlog(
                  test_log.info,
                  "base offset: {}, last offset: {}",
                  hdr.base_offset,
                  hdr.last_offset());
            }
        }
        BOOST_REQUIRE(headers_read.size() == num_data_batches);
    }
}

FIXTURE_TEST(
  test_remote_partition_scan_translate_with_duplicates_2,
  cloud_storage_fixture) {
    constexpr int batches_per_segment = 10;
    constexpr int num_segments = 10;
    constexpr int num_segments_with_duplicates = num_segments * 2;
    constexpr int total_batches = batches_per_segment * num_segments;
    batch_t data = {
      .num_records = 10, .type = model::record_batch_type::raft_data};
    batch_t conf = {
      .num_records = 1, .type = model::record_batch_type::raft_configuration};
    const std::vector<std::vector<batch_t>> batch_types = {
      {conf, data, data, conf, data, data, conf, data, data, conf},
      {conf, data, data, conf, data, data, conf, data, data, conf},
      {conf, data, data, conf, data, data, conf, data, data, conf},
      {conf, data, data, conf, data, data, conf, data, data, conf},
      {conf, data, data, conf, data, data, conf, data, data, conf},
      {conf, data, data, conf, data, data, conf, data, data, conf},
      {conf, data, data, conf, data, data, conf, data, data, conf},
      {conf, data, data, conf, data, data, conf, data, data, conf},
      {conf, data, data, conf, data, data, conf, data, data, conf},
      {conf, data, data, conf, data, data, conf, data, data, conf},
    };

    auto num_conf_batches = 0;
    auto num_data_batches = 0;
    for (const auto& segment : batch_types) {
        for (const auto& b : segment) {
            if (b.type == model::record_batch_type::raft_configuration) {
                num_conf_batches++;
            } else {
                num_data_batches++;
            }
        }
    }

    auto segments = setup_s3_imposter(
      *this, batch_types, manifest_inconsistency::duplicate_offset_ranges);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments_with_duplicates - 1].max_offset;

    vlog(test_log.debug, "offset range: {}-{}", base, max);
    print_segments(segments);

    for (size_t bsize : client_batch_sizes) {
        auto headers_read = scan_remote_partition_incrementally(
          *this, base, max, bsize);
        if (headers_read.size() != num_data_batches) {
            vlog(
              test_log.error,
              "Number of headers read: {}, expected: {}",
              headers_read.size(),
              total_batches - num_conf_batches);
            for (const auto& hdr : headers_read) {
                vlog(
                  test_log.info,
                  "base offset: {}, last offset: {}",
                  hdr.base_offset,
                  hdr.last_offset());
            }
        }
        BOOST_REQUIRE(headers_read.size() == num_data_batches);
    }
}

FIXTURE_TEST(test_remote_partition_scan_after_recovery, cloud_storage_fixture) {
    const char* manifest_json = R"json({
      "version": 1,
      "namespace": "test-ns",
      "topic": "test-topic",
      "partition": 42,
      "revision": 0,
      "last_offset": 1311,
      "insync_offset": 1313,
      "start_offset": 0,
      "segments": {
        "0-1-v1.log": {
          "is_compacted": false,
          "size_bytes": 15468,
          "committed_offset": 225,
          "base_offset": 0,
          "base_timestamp": 1680714100332,
          "max_timestamp": 1680714107958,
          "delta_offset": 0,
          "archiver_term": 1,
          "segment_term": 1,
          "delta_offset_end": 1,
          "sname_format": 2
        },
        "226-1-v1.log": {
          "is_compacted": false,
          "size_bytes": 190,
          "committed_offset": 226,
          "base_offset": 226,
          "base_timestamp": 1680714108861,
          "max_timestamp": 1680714107958,
          "delta_offset": 1,
          "archiver_term": 2,
          "segment_term": 1,
          "delta_offset_end": 1,
          "sname_format": 2
        },
        "227-2-v1.log": {
          "is_compacted": false,
          "size_bytes": 7976,
          "committed_offset": 340,
          "base_offset": 227,
          "base_timestamp": 1680714332546,
          "max_timestamp": 1680714337548,
          "delta_offset": 2,
          "archiver_term": 3,
          "segment_term": 2,
          "delta_offset_end": 4,
          "sname_format": 2
        },
        "341-3-v1.log": {
          "is_compacted": false,
          "size_bytes": 5256,
          "committed_offset": 414,
          "base_offset": 341,
          "base_timestamp": 1680714341227,
          "max_timestamp": 1680714343949,
          "delta_offset": 4,
          "archiver_term": 4,
          "segment_term": 3,
          "delta_offset_end": 6,
          "sname_format": 2
        },
        "415-4-v1.log": {
          "is_compacted": false,
          "size_bytes": 6616,
          "committed_offset": 508,
          "base_offset": 415,
          "base_timestamp": 1680714353405,
          "max_timestamp": 1680714356759,
          "delta_offset": 6,
          "archiver_term": 5,
          "segment_term": 4,
          "delta_offset_end": 8,
          "sname_format": 2
        },
        "501-5-v1.log": {
          "is_compacted": false,
          "size_bytes": 32708,
          "committed_offset": 989,
          "base_offset": 501,
          "base_timestamp": 1680714817104,
          "max_timestamp": 1680714832463,
          "delta_offset": 1,
          "archiver_term": 5,
          "segment_term": 5,
          "delta_offset_end": 1,
          "sname_format": 2
        },
        "990-5-v1.log": {
          "is_compacted": false,
          "size_bytes": 10596,
          "committed_offset": 1143,
          "base_offset": 990,
          "base_timestamp": 1680714833102,
          "max_timestamp": 1680714842970,
          "delta_offset": 1,
          "archiver_term": 5,
          "segment_term": 5,
          "delta_offset_end": 2,
          "sname_format": 2
        },
        "1144-5-v1.log": {
          "is_compacted": false,
          "size_bytes": 192,
          "committed_offset": 1144,
          "base_offset": 1144,
          "base_timestamp": 1680714846258,
          "max_timestamp": 1680714842970,
          "delta_offset": 2,
          "archiver_term": 6,
          "segment_term": 5,
          "delta_offset_end": 2,
          "sname_format": 2
        },
        "1145-6-v1.log": {
          "is_compacted": false,
          "size_bytes": 9846,
          "committed_offset": 1310,
          "base_offset": 1145,
          "base_timestamp": 1680714852301,
          "max_timestamp": 1680714857884,
          "delta_offset": 3,
          "archiver_term": 6,
          "segment_term": 6,
          "delta_offset_end": 5,
          "sname_format": 2
        },
        "1311-6-v1.log": {
          "is_compacted": false,
          "size_bytes": 193,
          "committed_offset": 1311,
          "base_offset": 1311,
          "base_timestamp": 1680714866941,
          "max_timestamp": 1680714857884,
          "delta_offset": 5,
          "archiver_term": 7,
          "segment_term": 6,
          "delta_offset_end": 5,
          "sname_format": 2
        }
      }
    })json";

    cloud_storage::partition_manifest manifest;
    iobuf manifest_body;
    manifest_body.append(manifest_json, std::strlen(manifest_json));
    auto is = make_iobuf_input_stream(std::move(manifest_body));
    manifest.update(std::move(is)).get();

    auto segments = setup_s3_imposter(*this, manifest);

    auto base = segments.front().base_offset;
    auto max = segments.back().max_offset;

    vlog(test_log.debug, "offset range: {}-{}", base, max);
    print_segments(segments);

    for (size_t sz : client_batch_sizes) {
        auto headers_read = scan_remote_partition_incrementally(
          *this, base, max, sz);

        // Check that there are no holes in the offset range.
        // This test works only with batches that contain a single record
        // so a simple check like this works.
        model::offset prev;
        for (const auto& hdr : headers_read) {
            BOOST_REQUIRE_EQUAL(hdr.base_offset, model::next_offset(prev));
            prev = hdr.base_offset;
        }
    }
}