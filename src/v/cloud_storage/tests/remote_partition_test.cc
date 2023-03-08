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
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <chrono>
#include <exception>
#include <iterator>
#include <numeric>
#include <random>
#include <system_error>

using namespace std::chrono_literals;
using namespace cloud_storage;

inline ss::logger test_log("test"); // NOLINT

static ss::abort_source never_abort;

static cloud_storage::lazy_abort_source always_continue{
  []() { return std::nullopt; }};

static constexpr model::cloud_credentials_source config_file{
  model::cloud_credentials_source::config_file};

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
      storage::segment_reader_handle(std::move(stream)));
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
    p1->close().get();
    return s;
}

static in_memory_segment
make_segment(model::offset base, const std::vector<batch_t>& batches) {
    auto num_config_batches = std::count_if(
      batches.begin(), batches.end(), [](batch_t t) {
          return t.type == model::record_batch_type::raft_configuration
                 || t.type == model::record_batch_type::archival_metadata;
      });
    auto num_config_records = std::accumulate(
      batches.begin(), batches.end(), 0U, [](size_t acc, batch_t b) {
          if (
            b.type != model::record_batch_type::raft_configuration
            && b.type != model::record_batch_type::archival_metadata) {
              return acc;
          }
          return acc + b.num_records;
      });
    iobuf segment_bytes = generate_segment(base, batches);
    std::vector<model::record_batch_header> hdr;
    std::vector<iobuf> rec;
    std::vector<uint64_t> off;
    auto p1 = make_recording_batch_parser(
      iobuf_deep_copy(segment_bytes), hdr, rec, off);
    p1->consume().get();
    p1->close().get();
    in_memory_segment s;
    s.bytes = linearize_iobuf(std::move(segment_bytes));
    s.base_offset = hdr.front().base_offset;
    s.max_offset = hdr.back().last_offset();
    s.headers = std::move(hdr);
    s.records = std::move(rec);
    s.file_offsets = std::move(off);
    s.sname = segment_name(fmt::format("{}-1-v1.log", s.base_offset()));
    s.num_config_batches = num_config_batches;
    s.num_config_records = num_config_records;
    s.delta_offset_overlap = 0;
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

static in_memory_segment merge_in_memory_segments(
  const in_memory_segment& lhs, const in_memory_segment& rhs) {
    vassert(
      model::next_offset(lhs.max_offset) == rhs.base_offset, "Bad base offset");
    in_memory_segment dst;
    dst.base_offset = lhs.base_offset;
    dst.max_offset = rhs.max_offset;
    dst.sname = lhs.sname;
    dst.num_config_batches = lhs.num_config_batches + rhs.num_config_batches;
    dst.num_config_records = lhs.num_config_records + rhs.num_config_records;
    dst.delta_offset_overlap = lhs.delta_offset_overlap;
    std::copy(
      lhs.headers.begin(), lhs.headers.end(), std::back_inserter(dst.headers));
    std::copy(
      rhs.headers.begin(), rhs.headers.end(), std::back_inserter(dst.headers));
    std::copy(
      lhs.file_offsets.begin(),
      lhs.file_offsets.end(),
      std::back_inserter(dst.file_offsets));
    auto last = dst.file_offsets.back() + lhs.headers.back().size_bytes;
    std::transform(
      rhs.file_offsets.begin(),
      rhs.file_offsets.end(),
      std::back_inserter(dst.file_offsets),
      [last](size_t o) { return o + last; });
    dst.bytes = lhs.bytes + rhs.bytes;
    return dst;
}

static in_memory_segment copy_in_memory_segment(const in_memory_segment& src) {
    // Copy everything except
    in_memory_segment dst;
    dst.base_offset = src.base_offset;
    dst.max_offset = src.max_offset;
    dst.sname = src.sname;
    dst.num_config_batches = src.num_config_batches;
    dst.num_config_records = src.num_config_records;
    dst.delta_offset_overlap = src.delta_offset_overlap;
    dst.headers = src.headers;
    dst.file_offsets = src.file_offsets;
    dst.bytes = src.bytes;
    return dst;
}

static in_memory_segment
copy_subsegment(const in_memory_segment& src, size_t shift, size_t length) {
    vassert(
      src.headers.size() > 1, "unexpected segment size {}", src.headers.size());
    in_memory_segment dst;
    dst.base_offset = src.headers.at(shift).base_offset;
    vlog(test_log.debug, "sub-segment {}", dst.base_offset);
    dst.file_offsets = src.file_offsets;
    auto first_fo = src.file_offsets.at(shift);
    dst.file_offsets.erase(
      dst.file_offsets.begin(), dst.file_offsets.begin() + shift);
    dst.file_offsets.resize(length);
    dst.headers = src.headers;
    dst.headers.erase(dst.headers.begin(), dst.headers.begin() + shift);
    dst.headers.resize(length);
    // NOTE: dst.records is kept empty since it's not used by tests
    std::string dst_bytes;
    std::copy(
      src.bytes.begin() + dst.file_offsets.front(),
      src.bytes.begin() + dst.file_offsets.back()
        + dst.headers.back().size_bytes,
      std::back_inserter(dst_bytes));
    dst.bytes = dst_bytes;
    // File offsets can only be adjusted after the segment is copied
    for (auto& fo : dst.file_offsets) {
        fo -= first_fo;
    }
    dst.max_offset = dst.headers.back().last_offset();
    dst.sname = segment_name(fmt::format("{}-1-v1.log", dst.base_offset));
    dst.num_config_batches = 0;
    dst.num_config_records = 0;
    for (const auto& h : dst.headers) {
        if (h.type != model::record_batch_type::raft_data) {
            dst.num_config_batches++;
            dst.num_config_records += h.record_count;
        }
    }
    // we have an overlap between to segments, the delta_offset_shift
    // has to store number of configuration records that overalp in both
    // segments
    for (const auto& h : src.headers) {
        auto o = h.base_offset;
        if (
          o >= dst.base_offset && o <= dst.max_offset
          && h.type != model::record_batch_type::raft_data) {
            dst.delta_offset_overlap += h.record_count;
        }
    }
    vlog(test_log.debug, "created sub-segment");
    return dst;
}

static constexpr bool is_internal_record_batch(model::record_batch_type type) {
    return type == model::record_batch_type::raft_configuration
           || type == model::record_batch_type::archival_metadata;
}

static std::vector<in_memory_segment> make_segments(
  const std::vector<std::vector<batch_t>>& segments,
  bool produce_overlapping = false,
  bool produce_duplicate = false) {
    vassert(
      !(produce_duplicate && produce_overlapping),
      "Only one inconsistency can be injected");
    std::vector<in_memory_segment> s;
    model::offset base_offset{0};
    if (produce_overlapping) {
        // In this case the overlap is one record batch:
        // s1: [0, 1, 2, 3, 4, 5, 6, 7]
        // s2:                      [7, 8, 9, 10, 11, 12, 13, 14, 15]
        //
        // s2 should have delta_offset_shift set to number of config records
        // in batch 7, otherwise the manifest will be generated incorrectly.
        in_memory_segment prev;
        for (int i = 0; i < segments.size(); i++) {
            const auto& batches = segments[i];
            auto body = make_segment(base_offset, batches);
            if (i > 0) {
                auto merged = merge_in_memory_segments(prev, body);
                auto truncated = copy_subsegment(
                  merged, prev.headers.size() - 1, body.headers.size() + 1);
                prev = std::move(body);
                // calculate partial overlap with first merged
                // segment (prev)
                truncated.delta_offset_overlap
                  = is_internal_record_batch(truncated.headers.front().type)
                      ? truncated.headers.front().record_count
                      : 0;
                s.push_back(std::move(truncated));
            } else {
                BOOST_REQUIRE(body.delta_offset_overlap == 0);
                prev = copy_in_memory_segment(body);
                s.push_back(std::move(body));
            }
            base_offset = s.back().max_offset + model::offset(1);
        }
    } else if (produce_duplicate) {
        // Here the overlap if full for duplicate:
        // s1 [0, 1, 2, 3]
        // s1'   [1, 2, 3]
        // s2             [4, 5, 6, 7]
        // s2'               [5, 6, 7]
        vlog(test_log.debug, "Producing duplicated log segments");
        for (int i = 0; i < segments.size(); i++) {
            const auto& batches = segments[i];
            auto body = make_segment(base_offset, batches);
            if (batches.size() > 1) {
                auto duplicate = copy_subsegment(body, 1, batches.size() - 1);
                s.push_back(std::move(body));
                s.push_back(std::move(duplicate));
            } else {
                s.push_back(std::move(body));
            }
            base_offset = s.back().max_offset + model::offset(1);
        }
    } else {
        for (int i = 0; i < segments.size(); i++) {
            const auto& batches = segments[i];
            auto body = make_segment(base_offset, batches);
            s.push_back(std::move(body));
            base_offset = s.back().max_offset + model::offset(1);
        }
    }
    return s;
}

static std::ostream& operator<<(std::ostream& o, const in_memory_segment& ims) {
    fmt::print(
      o,
      "name {}, base-offset {}, max-offset {}, do-not-reupload {}, "
      "num-config-batches {}, num-config-records {}, delta-offset-overlap {}\n",
      ims.sname,
      ims.base_offset,
      ims.max_offset,
      ims.do_not_reupload,
      ims.num_config_batches,
      ims.num_config_records,
      ims.delta_offset_overlap);
    for (size_t i = 0; i < ims.headers.size(); i++) {
        if (is_internal_record_batch(ims.headers[i].type)) {
            const auto& h = ims.headers[i];
            fmt::print(
              o,
              "\tconfiguration-batch {{ base_offset:{}, record_count:{} }}\n",
              h.base_offset,
              h.record_count);
        } else {
            const auto& h = ims.headers[i];
            fmt::print(
              o,
              "\tdata-batch {{ base_offset:{}, record_count:{} }}\n",
              h.base_offset,
              h.record_count);
        }
    }
    return o;
}

static void print_segments(const std::vector<in_memory_segment>& segments) {
    for (const auto& s : segments) {
        vlog(test_log.debug, "segment: {}", s);
    }
}

enum class manifest_inconsistency {
    none,
    truncated_segments,
    overlapping_segments,
    duplicate_offset_ranges,
};

static std::vector<cloud_storage_fixture::expectation>
make_imposter_expectations(
  cloud_storage::partition_manifest& m,
  const std::vector<in_memory_segment>& segments,
  bool truncate_segments = false,
  model::offset_delta delta = model::offset_delta(0)) {
    std::vector<cloud_storage_fixture::expectation> results;

    for (const auto& s : segments) {
        auto body = s.bytes;
        if (truncate_segments) {
            body = s.bytes.substr(0, s.bytes.size() / 2);
        }

        auto segment_delta = delta
                             - model::offset_delta(s.delta_offset_overlap);

        vlog(
          test_log.info,
          "computed segment delta {}, segment {}",
          segment_delta,
          s);
        BOOST_REQUIRE(segment_delta <= s.base_offset);
        cloud_storage::partition_manifest::segment_meta meta{
          .is_compacted = false,
          .size_bytes = s.bytes.size(),
          .base_offset = s.base_offset,
          .committed_offset = s.max_offset,
          .base_timestamp = {},
          .max_timestamp = {},
          .delta_offset = segment_delta,
          .ntp_revision = m.get_revision_id(),
          .delta_offset_end = model::offset_delta(delta)
                              + model::offset_delta(s.num_config_records),
        };

        m.add(s.sname, meta);
        delta = delta
                + model::offset(s.num_config_records - s.delta_offset_overlap);
        auto url = m.generate_segment_path(*m.get(meta.base_offset));
        results.push_back(cloud_storage_fixture::expectation{
          .url = "/" + url().string(), .body = body});
    }
    m.advance_insync_offset(m.get_last_offset());
    std::stringstream ostr;
    m.serialize(ostr);
    results.push_back(cloud_storage_fixture::expectation{
      .url = "/" + m.get_manifest_path()().string(),
      .body = ss::sstring(ostr.str())});
    vlog(
      test_log.info,
      "Uploaded manifest at {}:\n{}",
      m.get_manifest_path(),
      ostr.str());
    return results;
}

static void reupload_compacted_segments(
  cloud_storage_fixture& fixture,
  cloud_storage::partition_manifest& m,
  const std::vector<in_memory_segment>& segments,
  cloud_storage::remote& api,
  bool truncate_segments = false) {
    model::offset delta{0};
    for (const auto& s : segments) {
        auto body = s.bytes;
        if (truncate_segments) {
            body = s.bytes.substr(0, s.bytes.size() / 2);
        }

        cloud_storage::partition_manifest::segment_meta meta{
          .is_compacted = true,
          .size_bytes = s.bytes.size(),
          .base_offset = s.base_offset,
          .committed_offset = s.max_offset,
          .base_timestamp = {},
          .max_timestamp = {},
          .delta_offset = model::offset_delta(delta),
          .ntp_revision = m.get_revision_id(),
          .sname_format = segment_name_format::v2,
        };

        delta = delta + model::offset(s.num_config_records);

        if (!s.do_not_reupload) {
            // We are updating manifest before uploading segment: this is not
            // what the real upload path would do.  It is important that we
            // assert out if the upload doesn't succeed, to avoid manifest
            // and object store state getting out of sync.
            m.add(s.sname, meta);

            auto url = m.generate_segment_path(*m.get(meta.base_offset));
            vlog(test_log.debug, "reuploading segment {}", url);
            retry_chain_node rtc(never_abort, 60s, 1s);
            bytes bb;
            bb.resize(body.size());
            std::memcpy(bb.data(), body.data(), body.size());
            auto reset_stream = [body = std::move(bb)] {
                return ss::make_ready_future<
                  std::unique_ptr<storage::stream_provider>>(
                  std::make_unique<storage::segment_reader_handle>(
                    make_iobuf_input_stream(bytes_to_iobuf(body))));
            };
            auto result = api
                            .upload_segment(
                              cloud_storage_clients::bucket_name("bucket"),
                              url,
                              meta.size_bytes,
                              std::move(reset_stream),
                              rtc,
                              always_continue)
                            .get();
            BOOST_REQUIRE_EQUAL(result, cloud_storage::upload_result::success);
        }
    }
    m.advance_insync_offset(m.get_last_offset());
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

static auto setup_s3_imposter(
  cloud_storage_fixture& fixture,
  int num_segments,
  int num_batches_per_segment,
  manifest_inconsistency inject = manifest_inconsistency::none) {
    vassert(
      inject == manifest_inconsistency::none
        || inject == manifest_inconsistency::truncated_segments,
      "Not supported");
    // Create test data
    auto segments = make_segments(num_segments, num_batches_per_segment);
    cloud_storage::partition_manifest manifest(manifest_ntp, manifest_revision);
    auto expectations = make_imposter_expectations(
      manifest, segments, inject == manifest_inconsistency::truncated_segments);
    fixture.set_expectations_and_listen(expectations);
    return segments;
}

static auto setup_s3_imposter(
  cloud_storage_fixture& fixture,
  std::vector<std::vector<batch_t>> batches,
  manifest_inconsistency inject = manifest_inconsistency::none) {
    // Create test data
    auto segments = make_segments(
      batches,
      inject == manifest_inconsistency::overlapping_segments,
      inject == manifest_inconsistency::duplicate_offset_ranges);
    cloud_storage::partition_manifest manifest(manifest_ntp, manifest_revision);
    auto expectations = make_imposter_expectations(manifest, segments);
    fixture.set_expectations_and_listen(expectations);
    return segments;
}

static std::vector<in_memory_segment> make_segments(
  const std::vector<std::vector<batch_t>>& segments,
  model::offset base_offset) {
    std::vector<in_memory_segment> s;
    s.reserve(segments.size());
    for (const auto& batches : segments) {
        auto seg = make_segment(base_offset, batches);
        s.push_back(std::move(seg));
        base_offset = s.back().max_offset + model::offset(1);
    }
    return s;
}

static auto setup_s3_imposter(
  cloud_storage_fixture& fixture,
  model::offset base_offset,
  model::offset_delta base_delta,
  const std::vector<std::vector<batch_t>>& batches) {
    auto segments = make_segments(batches, base_offset);
    cloud_storage::partition_manifest manifest(manifest_ntp, manifest_revision);
    auto expectations = make_imposter_expectations(
      manifest, segments, false, base_delta);
    fixture.set_expectations_and_listen(expectations);
    return segments;
}

static partition_manifest hydrate_manifest(
  remote& api, const cloud_storage_clients::bucket_name& bucket) {
    partition_manifest m(manifest_ntp, manifest_revision);
    retry_chain_node rtc(never_abort, 1s, 200ms);
    auto key = m.get_manifest_path();
    auto res = api.download_manifest(bucket, key, m, rtc).get();
    BOOST_REQUIRE(res == cloud_storage::download_result::success);
    return m;
}

/// This test reads only a tip of the log
static model::record_batch_header read_single_batch_from_remote_partition(
  cloud_storage_fixture& fixture,
  model::offset target,
  bool expect_exists = true) {
    auto conf = fixture.get_configuration();
    static auto bucket = cloud_storage_clients::bucket_name("bucket");
    remote api(connection_limit(10), conf, config_file);
    api.start().get();
    auto action = ss::defer([&api] { api.stop().get(); });
    auto m = ss::make_lw_shared<cloud_storage::partition_manifest>(
      manifest_ntp, manifest_revision);
    storage::log_reader_config reader_config(
      target, target, ss::default_priority_class());

    auto manifest = hydrate_manifest(api, bucket);

    auto partition = ss::make_shared<remote_partition>(
      manifest, api, fixture.cache.local(), bucket);
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

/// Similar to prev function but scans the range of offsets instead of
/// returning a single one
static std::vector<model::record_batch_header> scan_remote_partition(
  cloud_storage_fixture& imposter,
  model::offset base,
  model::offset max = model::offset::max(),
  size_t maybe_max_segments = 0,
  size_t maybe_max_readers = 0) {
    auto conf = imposter.get_configuration();
    static auto bucket = cloud_storage_clients::bucket_name("bucket");
    if (maybe_max_segments) {
        config::shard_local_cfg()
          .cloud_storage_max_materialized_segments_per_shard(
            maybe_max_segments);
    }
    if (maybe_max_readers) {
        config::shard_local_cfg().cloud_storage_max_readers_per_shard(
          maybe_max_readers);
    }
    remote api(connection_limit(10), conf, config_file);
    api.start().get();
    auto action = ss::defer([&api] { api.stop().get(); });
    auto m = ss::make_lw_shared<cloud_storage::partition_manifest>(
      manifest_ntp, manifest_revision);
    storage::log_reader_config reader_config(
      base, max, ss::default_priority_class());

    auto manifest = hydrate_manifest(api, bucket);

    auto partition = ss::make_shared<remote_partition>(
      manifest, api, imposter.cache.local(), bucket);
    auto partition_stop = ss::defer([&partition] { partition->stop().get(); });

    partition->start().get();

    auto reader = partition->make_reader(reader_config).get().reader;

    auto headers_read
      = reader.consume(test_consumer(), model::no_timeout).get();
    std::move(reader).release();

    return headers_read;
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

struct segment_layout {
    std::vector<std::vector<batch_t>> segments;
    size_t num_data_batches;
};

static segment_layout generate_segment_layout(
  int num_segments, int seed, bool exclude_tx_fence = true) {
    static constexpr size_t max_segment_size = 20;
    static constexpr size_t max_batch_size = 10;
    static constexpr size_t max_record_bytes = 2048;
    size_t num_data_batches = 0;
    auto gen_segment = [&num_data_batches, exclude_tx_fence]() {
        size_t sz = random_generators::get_int((size_t)1, max_segment_size - 1);
        std::vector<batch_t> res;
        res.reserve(sz);
        model::record_batch_type types[] = {
          model::record_batch_type::raft_data,
          model::record_batch_type::raft_configuration,
          model::record_batch_type::archival_metadata,
          model::record_batch_type::tx_fence,
        };
        auto num_types = (sizeof(types) / sizeof(model::record_batch_type))
                         - static_cast<size_t>(exclude_tx_fence);
        for (size_t i = 0; i < sz; i++) {
            auto type = types[random_generators::get_int(num_types - 1)];
            size_t batch_size = random_generators::get_int(
              (size_t)1, max_batch_size - 1);
            if (
              type == model::record_batch_type::raft_configuration
              || type == model::record_batch_type::tx_fence) {
                // raft_configuration can only have one record
                // tx_fence can only have one record
                // archival_metadata can have more than one records
                batch_size = 1;
            }
            std::vector<size_t> sizes;
            for (int j = 0; j < batch_size; j++) {
                sizes.push_back(
                  random_generators::get_int(max_record_bytes - 1));
            }
            batch_t b{
              .num_records = static_cast<int>(batch_size),
              .type = type,
              .record_sizes = sizes,
            };
            if (b.type == model::record_batch_type::raft_data) {
                num_data_batches++;
            }
            res.push_back(b);
        }
        return res;
    };
    std::vector<std::vector<batch_t>> all_batches;
    all_batches.reserve(num_segments);
    for (int i = 0; i < num_segments; i++) {
        all_batches.push_back(gen_segment());
    }
    return {.segments = all_batches, .num_data_batches = num_data_batches};
}

/// This test scans the entire range of offsets
FIXTURE_TEST(
  test_remote_partition_scan_translate_full_random, cloud_storage_fixture) {
    constexpr int num_segments = 1000;
    const auto [batch_types, num_data_batches] = generate_segment_layout(
      num_segments, 42);
    auto segments = setup_s3_imposter(*this, batch_types);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;
    vlog(test_log.debug, "offset range: {}-{}", base, max);
    auto headers_read = scan_remote_partition(
      *this,
      base,
      max,
      random_generators::get_int(5, 20),
      random_generators::get_int(5, 20));
    model::offset expected_offset{0};
    for (const auto& header : headers_read) {
        BOOST_REQUIRE_EQUAL(expected_offset, header.base_offset);
        expected_offset = header.last_offset() + model::offset(1);
    }
    BOOST_REQUIRE_EQUAL(headers_read.size(), num_data_batches);
}

/// Similar to prev function but scans the range of offsets instead of
/// returning a single one
static std::vector<model::record_batch_header>
scan_remote_partition_incrementally(
  cloud_storage_fixture& imposter,
  model::offset base,
  model::offset max,
  size_t maybe_max_bytes = 0,
  size_t maybe_max_segments = 0,
  size_t maybe_max_readers = 0) {
    auto conf = imposter.get_configuration();
    static auto bucket = cloud_storage_clients::bucket_name("bucket");
    if (maybe_max_segments) {
        config::shard_local_cfg()
          .cloud_storage_max_materialized_segments_per_shard(
            maybe_max_segments);
    }
    if (maybe_max_readers) {
        config::shard_local_cfg().cloud_storage_max_readers_per_shard(
          maybe_max_readers);
    }
    remote api(connection_limit(10), conf, config_file);
    api.start().get();
    auto action = ss::defer([&api] { api.stop().get(); });
    auto m = ss::make_lw_shared<cloud_storage::partition_manifest>(
      manifest_ntp, manifest_revision);

    auto manifest = hydrate_manifest(api, bucket);
    auto partition = ss::make_shared<remote_partition>(
      manifest, api, imposter.cache.local(), bucket);
    auto partition_stop = ss::defer([&partition] { partition->stop().get(); });

    partition->start().get();

    std::vector<model::record_batch_header> headers;

    storage::log_reader_config reader_config(
      base, max, ss::default_priority_class());

    // starting max_bytes
    constexpr size_t max_bytes_limit = 4_KiB;
    reader_config.max_bytes = maybe_max_bytes != 0 ? maybe_max_bytes
                                                   : max_bytes_limit;

    auto next = base;

    int num_fetches = 0;
    while (next < max) {
        reader_config.start_offset = next;
        if (maybe_max_bytes == 0) {
            reader_config.max_bytes = random_generators::get_int(
              max_bytes_limit - 1);
        }
        vlog(test_log.info, "reader_config {}", reader_config);
        auto reader = partition->make_reader(reader_config).get().reader;
        auto headers_read
          = reader.consume(test_consumer(), model::no_timeout).get();
        if (headers_read.empty()) {
            break;
        }
        for (const auto& header : headers_read) {
            vlog(test_log.info, "header {}", header);
        }
        next = headers_read.back().last_offset() + model::offset(1);
        std::copy(
          headers_read.begin(),
          headers_read.end(),
          std::back_inserter(headers));
        num_fetches++;
    }
    BOOST_REQUIRE(num_fetches > 0);
    vlog(test_log.info, "{} fetch operations performed", num_fetches);
    return headers;
}

FIXTURE_TEST(
  test_remote_partition_scan_incrementally_random, cloud_storage_fixture) {
    constexpr int num_segments = 1000;
    const auto [batch_types, num_data_batches] = generate_segment_layout(
      num_segments, 42);
    auto segments = setup_s3_imposter(*this, batch_types);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;
    vlog(test_log.debug, "offset range: {}-{}", base, max);
    auto headers_read = scan_remote_partition_incrementally(
      *this,
      base,
      max,
      0,
      random_generators::get_int(5, 20),
      random_generators::get_int(5, 20));
    model::offset expected_offset{0};
    for (const auto& header : headers_read) {
        BOOST_REQUIRE_EQUAL(expected_offset, header.base_offset);
        expected_offset = header.last_offset() + model::offset(1);
    }
    BOOST_REQUIRE_EQUAL(headers_read.size(), num_data_batches);
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
    remote api(connection_limit(10), conf, config_file);
    api.start().get();
    auto action = ss::defer([&api] { api.stop().get(); });
    auto m = ss::make_lw_shared<cloud_storage::partition_manifest>(
      manifest_ntp, manifest_revision);

    auto manifest = hydrate_manifest(api, bucket);

    // starting max_bytes
    constexpr size_t max_bytes_limit = 4_KiB;

    // Read first segment using first remote_partition instance.
    // After this block finishes the segment will be hydrated.
    {
        auto partition = ss::make_shared<remote_partition>(
          manifest, api, cache.local(), bucket);
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
          manifest, api, cache.local(), bucket);
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
    auto conf = get_configuration();
    static auto bucket = cloud_storage_clients::bucket_name("bucket");
    remote api(connection_limit(10), conf, config_file);
    api.start().get();
    auto action = ss::defer([&api] { api.stop().get(); });

    auto manifest = hydrate_manifest(api, bucket);

    auto partition = ss::make_shared<remote_partition>(
      manifest, api, cache.local(), bucket);
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

        remove_segment_from_s3(manifest, model::offset(0), api, bucket);
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
    auto conf = get_configuration();
    static auto bucket = cloud_storage_clients::bucket_name("bucket");
    remote api(connection_limit(10), conf, config_file);
    api.start().get();
    auto action = ss::defer([&api] { api.stop().get(); });

    auto manifest = hydrate_manifest(api, bucket);

    auto partition = ss::make_shared<remote_partition>(
      manifest, api, cache.local(), bucket);
    auto partition_stop = ss::defer([&partition] { partition->stop().get(); });

    partition->start().get();

    model::offset cutoff_offset(500);

    remove_segment_from_s3(manifest, model::offset(0), api, bucket);
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
    auto conf = get_configuration();
    static auto bucket = cloud_storage_clients::bucket_name("bucket");
    remote api(connection_limit(10), conf, config_file);
    api.start().get();
    auto action = ss::defer([&api] { api.stop().get(); });

    auto manifest = hydrate_manifest(api, bucket);

    auto partition = ss::make_shared<remote_partition>(
      manifest, api, cache.local(), bucket);
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
              manifest, model::offset(i * batches_per_segment), api, bucket);
        }
        reupload_compacted_segments(*this, manifest, compacted_segments, api);
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

static std::vector<model::record_batch_header>
scan_remote_partition_incrementally_with_reuploads(
  cloud_storage_fixture& imposter,
  model::offset base,
  model::offset max,
  std::vector<in_memory_segment> segments,
  size_t maybe_max_segments = 0,
  size_t maybe_max_readers = 0) {
    auto conf = imposter.get_configuration();
    static auto bucket = cloud_storage_clients::bucket_name("bucket");
    if (maybe_max_segments) {
        config::shard_local_cfg()
          .cloud_storage_max_materialized_segments_per_shard(
            maybe_max_segments);
    }
    if (maybe_max_readers) {
        config::shard_local_cfg().cloud_storage_max_readers_per_shard(
          maybe_max_readers);
    }
    remote api(connection_limit(10), conf, config_file);
    api.start().get();
    auto action = ss::defer([&api] { api.stop().get(); });
    auto m = ss::make_lw_shared<cloud_storage::partition_manifest>(
      manifest_ntp, manifest_revision);

    auto manifest = hydrate_manifest(api, bucket);
    auto partition = ss::make_shared<remote_partition>(
      manifest, api, imposter.cache.local(), bucket);
    auto partition_stop = ss::defer([&partition] { partition->stop().get(); });

    partition->start().get();

    std::vector<model::record_batch_header> headers;

    storage::log_reader_config reader_config(
      base, max, ss::default_priority_class());

    // starting max_bytes
    constexpr size_t max_bytes_limit = 4_KiB;
    reader_config.max_bytes = max_bytes_limit;

    auto next = base;
    auto next_insync_offset = model::next_offset(manifest.get_insync_offset());
    auto drop_reupload_flag = [&segments] {
        for (auto& s : segments) {
            s.do_not_reupload = true;
        }
    };
    auto maybe_reupload_range = [&imposter,
                                 &manifest,
                                 &next_insync_offset,
                                 &segments,
                                 &api](model::offset begin) {
        // if this is true, start from prev segment, not the one which is
        // the closest to 'begin'
        auto shift_one_back = random_generators::get_int(0, 4) == 0;
        auto ix = 0;
        for (auto& s : segments) {
            if (s.base_offset > begin) {
                break;
            }
            if (!shift_one_back) {
                ix++;
            } else {
                shift_one_back = false;
            }
        }
        // choose how many segments to merge together
        auto n = random_generators::get_int(0, 4);
        vlog(
          test_log.debug,
          "reuploading {} segments starting from offset {}, insync_offset: {}, "
          "num segments: {}",
          n,
          segments[ix].base_offset,
          next_insync_offset,
          segments.size());
        auto merge_segments = [&segments, &manifest](int begin, int end) {
            auto meta_ptr = manifest.get(segments[begin].base_offset);
            if (meta_ptr->is_compacted) {
                vlog(
                  test_log.debug,
                  "segment {}-{} is already compacted, skipping",
                  meta_ptr->base_offset,
                  meta_ptr->committed_offset);
                return;
            }
            BOOST_REQUIRE(end - begin > 1);
            end = std::clamp(end, end, static_cast<int>(segments.size()));
            in_memory_segment& first = segments[begin];
            const in_memory_segment& last = segments[end - 1];
            vlog(
              test_log.debug,
              "merging segments {}-{} and {}-{}",
              first.base_offset,
              first.max_offset,
              last.base_offset,
              last.max_offset);
            for (int i = 1 + begin; i < end; i++) {
                auto& s = segments[i];
                first.base_offset = std::min(first.base_offset, s.base_offset);
                first.max_offset = std::max(first.max_offset, s.max_offset);
                first.do_not_reupload = false;
                first.num_config_batches += s.num_config_batches;
                first.num_config_records += s.num_config_records;
                std::copy(
                  std::make_move_iterator(s.records.begin()),
                  std::make_move_iterator(s.records.end()),
                  std::back_inserter(first.records));
                first.bytes.append(s.bytes.data(), s.bytes.size());
                std::copy(
                  s.headers.begin(),
                  s.headers.end(),
                  std::back_inserter(first.headers));
                std::copy(
                  s.file_offsets.begin(),
                  s.file_offsets.end(),
                  std::back_inserter(first.file_offsets));
            }
            segments.erase(
              segments.begin() + 1 + begin, segments.begin() + end);
        };
        if (n > 1) {
            merge_segments(ix, ix + n);
            reupload_compacted_segments(imposter, manifest, segments, api);
            manifest.advance_insync_offset(next_insync_offset);
            next_insync_offset = model::next_offset(next_insync_offset);
        } else if (n == 1) {
            segments[ix].do_not_reupload = false;
            reupload_compacted_segments(imposter, manifest, segments, api);
            manifest.advance_insync_offset(next_insync_offset);
            next_insync_offset = model::next_offset(next_insync_offset);
        }
        vlog(
          test_log.debug,
          "completed reuploading {} segments, num segments: {}",
          n,
          segments.size());
    };

    int num_fetches = 0;
    while (next < max) {
        reader_config.start_offset = next;
        reader_config.max_bytes = random_generators::get_int(
          max_bytes_limit - 1);
        drop_reupload_flag();
        maybe_reupload_range(next);
        vlog(test_log.info, "reader_config {}", reader_config);
        auto reader = partition->make_reader(reader_config).get().reader;
        auto headers_read
          = reader.consume(test_consumer(), model::no_timeout).get();
        if (headers_read.empty()) {
            break;
        }
        for (const auto& header : headers_read) {
            vlog(test_log.info, "header {}", header);
        }
        next = headers_read.back().last_offset() + model::offset(1);
        std::copy(
          headers_read.begin(),
          headers_read.end(),
          std::back_inserter(headers));
        num_fetches++;
    }

    BOOST_REQUIRE(num_fetches > 0);
    vlog(test_log.info, "{} fetch operations performed", num_fetches);
    return headers;
}

FIXTURE_TEST(
  test_remote_partition_scan_incrementally_random_with_reuploads,
  cloud_storage_fixture) {
    constexpr int num_segments = 1000;
    const auto [batch_types, num_data_batches] = generate_segment_layout(
      num_segments, 42);
    auto segments = setup_s3_imposter(*this, batch_types);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;
    vlog(test_log.debug, "full offset range: {}-{}", base, max);
    auto headers_read = scan_remote_partition_incrementally_with_reuploads(
      *this,
      base,
      max,
      std::move(segments),
      random_generators::get_int(5, 20),
      random_generators::get_int(5, 20));
    model::offset expected_offset{0};
    for (const auto& header : headers_read) {
        BOOST_REQUIRE_EQUAL(expected_offset, header.base_offset);
        expected_offset = header.last_offset() + model::offset(1);
    }
    BOOST_REQUIRE_EQUAL(headers_read.size(), num_data_batches);
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

namespace {

ss::future<> scan_until_close(
  remote_partition& partition,
  const storage::log_reader_config& reader_config,
  ss::gate& g) {
    gate_guard guard{g};
    while (!g.is_closed()) {
        try {
            auto translating_reader = co_await partition.make_reader(
              reader_config);
            auto reader = std::move(translating_reader.reader);
            auto headers_read = co_await reader.consume(
              test_consumer(), model::no_timeout);
        } catch (...) {
            test_log.info("Error scanning: {}", std::current_exception());
        }
    }
}

} // anonymous namespace

// Test designed to reproduce a hang seen during shutdown.
FIXTURE_TEST(test_scan_while_shutting_down, cloud_storage_fixture) {
    constexpr int num_segments = 1000;
    const auto [segment_layout, num_data_batches] = generate_segment_layout(
      num_segments, 42, false);
    auto segments = setup_s3_imposter(*this, segment_layout);
    auto base = segments[0].base_offset;

    auto remote_conf = this->get_configuration();
    remote api(connection_limit(10), remote_conf, config_file);
    api.start().get();
    auto action = ss::defer([&api] { api.stop().get(); });

    auto m = ss::make_lw_shared<cloud_storage::partition_manifest>(
      manifest_ntp, manifest_revision);

    storage::log_reader_config reader_config(
      base, model::offset::max(), ss::default_priority_class());
    static auto bucket = cloud_storage_clients::bucket_name("bucket");
    auto manifest = hydrate_manifest(api, bucket);
    auto partition = ss::make_shared<remote_partition>(
      manifest, api, this->cache.local(), bucket);
    partition->start().get();
    auto partition_stop = ss::defer([&partition] { partition->stop().get(); });

    ss::gate g;
    ssx::background = scan_until_close(*partition, reader_config, g);
    auto close_fut = ss::maybe_yield()
                       .then([] { return ss::maybe_yield(); })
                       .then([] { return ss::maybe_yield(); })
                       .then([] {
                           return ss::sleep(std::chrono::milliseconds(10));
                       })
                       .then([&] {
                           api.shutdown_connections();
                           return g.close();
                       });
    ss::with_timeout(model::timeout_clock::now() + 60s, std::move(close_fut))
      .get();
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

FIXTURE_TEST(
  test_remote_partition_scan_incrementally_random_with_overlaps,
  cloud_storage_fixture) {
    constexpr int num_segments = 1000;
    const auto [batch_types, num_data_batches] = generate_segment_layout(
      num_segments, 42);
    auto segments = setup_s3_imposter(
      *this, batch_types, manifest_inconsistency::overlapping_segments);
    auto base = segments[0].base_offset;
    auto max = segments.back().max_offset;
    vlog(test_log.debug, "offset range: {}-{}", base, max);

    scan_remote_partition_incrementally(*this, base, max);
}

FIXTURE_TEST(
  test_remote_partition_scan_incrementally_random_with_duplicates,
  cloud_storage_fixture) {
    constexpr int num_segments = 500;
    const auto [batch_types, num_data_batches] = generate_segment_layout(
      num_segments, 42);
    auto segments = setup_s3_imposter(
      *this, batch_types, manifest_inconsistency::duplicate_offset_ranges);
    auto base = segments[0].base_offset;
    auto max = segments.back().max_offset;
    vlog(test_log.debug, "offset range: {}-{}", base, max);

    scan_remote_partition_incrementally(*this, base, max);
}

FIXTURE_TEST(
  test_remote_partition_scan_incrementally_random_with_tx_fence,
  cloud_storage_fixture) {
    constexpr int num_segments = 1000;
    const auto [segment_layout, num_data_batches] = generate_segment_layout(
      num_segments, 42, false);
    auto segments = setup_s3_imposter(*this, segment_layout);
    auto base = segments[0].base_offset;
    auto max = segments.back().max_offset;
    vlog(test_log.debug, "offset range: {}-{}", base, max);

    auto headers_read = scan_remote_partition_incrementally(
      *this, base, max, 0, 20, 20);
    model::offset expected_offset{0};
    size_t ix_header = 0;
    for (size_t ix_seg = 0; ix_seg < segment_layout.size(); ix_seg++) {
        for (size_t ix_batch = 0; ix_batch < segment_layout[ix_seg].size();
             ix_batch++) {
            auto batch = segment_layout[ix_seg][ix_batch];
            if (batch.type == model::record_batch_type::tx_fence) {
                expected_offset++;
            } else if (batch.type == model::record_batch_type::raft_data) {
                auto header = headers_read[ix_header];
                BOOST_REQUIRE_EQUAL(expected_offset, header.base_offset);
                expected_offset = header.last_offset() + model::offset(1);
                ix_header++;
            } else {
                // raft_configuratoin or archival_metadata
                // no need to update expected_offset or ix_header
            }
        }
    }
}
