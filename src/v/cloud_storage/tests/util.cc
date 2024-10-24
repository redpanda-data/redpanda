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
#include "cloud_storage/tests/util.h"

#include "cloud_storage/partition_manifest_downloader.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "utils/stream_provider.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <ostream>
#include <random>
#include <vector>

namespace cloud_storage {

static const remote_path_provider path_provider(std::nullopt, std::nullopt);

segment_layout
generate_segment_layout(int num_segments, int seed, bool exclude_tx_fence) {
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

std::ostream& operator<<(std::ostream& o, const in_memory_segment& ims) {
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

std::unique_ptr<storage::continuous_batch_parser> make_recording_batch_parser(
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

in_memory_segment
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
    std::optional<model::timestamp> base_ts;
    std::optional<model::timestamp> last_ts;
    for (const auto& b : batches) {
        if (b.timestamp.has_value()) {
            last_ts = b.timestamp;
            if (!base_ts.has_value()) {
                base_ts = b.timestamp;
            }
        }
    }
    auto [segment_bytes, next_offset] = generate_segment(base, batches);
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
    s.max_offset = next_offset - model::offset(1);
    s.headers = std::move(hdr);
    s.records = std::move(rec);
    s.file_offsets = std::move(off);
    s.sname = segment_name(fmt::format("{}-1-v1.log", s.base_offset()));
    s.num_config_batches = num_config_batches;
    s.num_config_records = num_config_records;
    s.delta_offset_overlap = 0;
    s.base_timestamp = base_ts;
    s.last_timestamp = last_ts;
    return s;
}

in_memory_segment make_segment(model::offset base, int num_batches) {
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

std::vector<in_memory_segment>
make_segments(int num_segments, int num_batches) {
    std::vector<in_memory_segment> s;
    model::offset base_offset{0};
    for (int i = 0; i < num_segments; i++) {
        s.push_back(make_segment(base_offset, num_batches));
        base_offset = s.back().max_offset + model::offset(1);
    }
    return s;
}

in_memory_segment merge_in_memory_segments(
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

in_memory_segment copy_in_memory_segment(const in_memory_segment& src) {
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

in_memory_segment
copy_subsegment(const in_memory_segment& src, size_t shift, size_t length) {
    vassert(
      src.headers.size() > 1, "unexpected segment size {}", src.headers.size());
    in_memory_segment dst;
    dst.base_offset = src.headers.at(shift).base_offset;
    vlog(test_util_log.debug, "sub-segment {}", dst.base_offset);
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
    vlog(test_util_log.debug, "created sub-segment");
    return dst;
}

std::vector<in_memory_segment>
make_segments(const partition_manifest& manifest) {
    std::vector<in_memory_segment> segments;
    for (const auto& meta : manifest) {
        auto num_config_records = meta.delta_offset_end() - meta.delta_offset();
        auto num_records = meta.committed_offset() - meta.base_offset() + 1;
        std::vector<batch_t> all_batches;
        for (long i = 0; i < num_records; i++) {
            if (i < num_config_records) {
                all_batches.push_back(batch_t{
                  .num_records = 1,
                  .type = model::record_batch_type::archival_metadata,
                  .record_sizes = {random_generators::get_int(10UL, 200UL)},
                });
            } else {
                all_batches.push_back(batch_t{
                  .num_records = 1,
                  .type = model::record_batch_type::raft_data,
                  .record_sizes = {random_generators::get_int(10UL, 200UL)},
                });
            }
        }
        std::random_device dev;
        std::mt19937 mtws(dev());
        std::shuffle(all_batches.begin(), all_batches.end(), mtws);
        auto body = make_segment(meta.base_offset, all_batches);
        segments.push_back(std::move(body));
    }
    return segments;
}

std::vector<in_memory_segment> make_segments(
  const std::vector<std::vector<batch_t>>& segments,
  bool produce_overlapping,
  bool produce_duplicate) {
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
        vlog(test_util_log.debug, "Producing duplicated log segments");
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

std::vector<in_memory_segment> make_segments(
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

std::vector<cloud_storage_fixture::expectation> make_imposter_expectations(
  const cloud_storage::partition_manifest& m,
  const std::vector<in_memory_segment>& segments) {
    std::vector<cloud_storage_fixture::expectation> results;
    for (const auto& s : segments) {
        auto url = m.generate_segment_path(
          *m.get(s.base_offset), path_provider);
        results.push_back(cloud_storage_fixture::expectation{
          .url = url().string(), .body = s.bytes});
    }
    auto serialized = [&] {
        auto s_data = m.serialize().get();
        auto buf = s_data.stream.read_exactly(s_data.size_bytes).get();
        return ss::sstring(buf.begin(), buf.end());
    };
    results.push_back(cloud_storage_fixture::expectation{
      .url = m.get_manifest_path(path_provider)().string(),
      .body = serialized()});
    std::stringstream ostr;
    m.serialize_json(ostr);
    vlog(
      test_util_log.info,
      "Uploaded manifest at {}:\n{}",
      m.get_manifest_path(path_provider),
      ostr.str());
    return results;
}

std::vector<cloud_storage_fixture::expectation> make_imposter_expectations(
  cloud_storage::partition_manifest& m,
  const std::vector<in_memory_segment>& segments,
  bool truncate_segments,
  model::offset_delta delta,
  segment_name_format sname_format) {
    std::vector<cloud_storage_fixture::expectation> results;

    for (const auto& s : segments) {
        auto body = s.bytes;
        if (truncate_segments) {
            body = s.bytes.substr(0, s.bytes.size() / 2);
        }

        auto segment_delta = delta
                             - model::offset_delta(s.delta_offset_overlap);

        vlog(
          test_util_log.info,
          "computed segment delta {}, segment {}",
          segment_delta,
          s);
        BOOST_REQUIRE(model::offset_cast(segment_delta) <= s.base_offset());
        cloud_storage::partition_manifest::segment_meta meta{
          .is_compacted = false,
          .size_bytes = s.bytes.size(),
          .base_offset = s.base_offset,
          .committed_offset = s.max_offset,
          .base_timestamp = s.base_timestamp.value_or(model::timestamp()),
          .max_timestamp = s.last_timestamp.value_or(model::timestamp()),
          .delta_offset = segment_delta,
          .ntp_revision = m.get_revision_id(),
          .delta_offset_end = model::offset_delta(delta)
                              + model::offset_delta(s.num_config_records),
          .sname_format = sname_format};

        m.add(s.sname, meta);
        delta = delta
                + model::offset(s.num_config_records - s.delta_offset_overlap);
        auto url = m.generate_segment_path(
          *m.get(meta.base_offset), path_provider);
        results.push_back(cloud_storage_fixture::expectation{
          .url = url().string(), .body = body});
    }
    m.advance_insync_offset(m.get_last_offset());
    auto serialized = [&] {
        auto s_data = m.serialize().get();
        auto buf = s_data.stream.read_exactly(s_data.size_bytes).get();
        return ss::sstring(buf.begin(), buf.end());
    };
    results.push_back(cloud_storage_fixture::expectation{
      .url = m.get_manifest_path(path_provider)().string(),
      .body = serialized()});
    std::ostringstream ostr;
    m.serialize_json(ostr);

    vlog(
      test_util_log.info,
      "Uploaded manifest at {}:\n{}",
      m.get_manifest_path(path_provider),
      ostr.str());
    return results;
}

std::vector<in_memory_segment> setup_s3_imposter(
  cloud_storage_fixture& fixture,
  int num_segments,
  int num_batches_per_segment,
  manifest_inconsistency inject,
  segment_name_format sname_format) {
    vassert(
      inject == manifest_inconsistency::none
        || inject == manifest_inconsistency::truncated_segments,
      "Not supported");
    // Create test data
    auto segments = make_segments(num_segments, num_batches_per_segment);
    cloud_storage::partition_manifest manifest(manifest_ntp, manifest_revision);
    auto expectations = make_imposter_expectations(
      manifest,
      segments,
      inject == manifest_inconsistency::truncated_segments,
      model::offset_delta(0),
      sname_format);
    fixture.set_expectations_and_listen(expectations);
    return segments;
}

std::vector<in_memory_segment> setup_s3_imposter(
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

/// Generate segments and replace the original ones.
/// The batches should describe updated offset range.
std::vector<in_memory_segment> replace_segments(
  cloud_storage_fixture& fixture,
  cloud_storage::partition_manifest& manifest,
  model::offset base_offset,
  model::offset_delta base_delta,
  const std::vector<std::vector<batch_t>>& batches) {
    // the batches are supposed to replace the ones which are already
    // in the manifest
    auto segments = make_segments(batches, base_offset);

    // remove old segments
    std::vector<ss::sstring> segments_to_remove;
    for (const auto& s : segments) {
        auto bo = s.base_offset;
        auto it = manifest.find(bo);
        BOOST_REQUIRE(it != manifest.end());
        auto path = manifest.generate_segment_path(*it, path_provider);
        segments_to_remove.push_back(path().native());
    }
    fixture.remove_expectations(segments_to_remove);

    // remove manifest from the list
    auto manifest_url = manifest.get_manifest_path(path_provider)().string();

    auto expectations = make_imposter_expectations(
      manifest, segments, false, base_delta);

    auto it = std::find_if(
      expectations.begin(),
      expectations.end(),
      [manifest_url](const cloud_storage_fixture::expectation& e) {
          return e.url == manifest_url;
      });

    vassert(it != expectations.end(), "Can't find manifest URL");
    expectations.erase(it);

    // add re-generated segments to the impostor
    fixture.add_expectations(expectations);
    return segments;
}

std::vector<in_memory_segment> setup_s3_imposter(
  cloud_storage_fixture& fixture,
  std::vector<std::vector<batch_t>> batches,
  manifest_inconsistency inject) {
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

std::vector<in_memory_segment> setup_s3_imposter(
  cloud_storage_fixture& fixture,
  const cloud_storage::partition_manifest& manifest) {
    auto segments = make_segments(manifest);
    auto expectations = make_imposter_expectations(manifest, segments);
    fixture.set_expectations_and_listen(expectations);
    return segments;
}

partition_manifest hydrate_manifest(
  remote& api, const cloud_storage_clients::bucket_name& bucket) {
    static ss::abort_source never_abort;

    remote_path_provider path_provider(std::nullopt, std::nullopt);
    partition_manifest_downloader dl(
      bucket, path_provider, manifest_ntp, manifest_revision, api);
    partition_manifest m(manifest_ntp, manifest_revision);
    retry_chain_node rtc(never_abort, 300s, 200ms);
    ss::lowres_clock::update();
    auto res = dl.download_manifest(rtc, &m).get();
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(res.value() == find_partition_manifest_outcome::success);
    return m;
}

/// Similar to prev function but scans the range of offsets instead of
/// returning a single one
std::vector<model::record_batch_header> scan_remote_partition_incrementally(
  cloud_storage_fixture& imposter,
  model::offset base,
  model::offset max,
  size_t maybe_max_bytes,
  size_t maybe_max_segments,
  size_t maybe_max_readers) {
    // The lowres clock could become stale after reactor stall. In this
    // case, if the reactor stall was longer than 1s and the next the next
    // call to ss::lowres_clock::now() will result in a sudden jump forward
    // in time and the timeout for the next operation will be computed
    // incorrectly.
    ss::lowres_clock::update();
    auto conf = imposter.get_configuration();
    if (maybe_max_segments) {
        config::shard_local_cfg()
          .cloud_storage_max_materialized_segments_per_shard.set_value(
            maybe_max_segments);
    }
    if (maybe_max_readers) {
        config::shard_local_cfg()
          .cloud_storage_max_segment_readers_per_shard.set_value(
            maybe_max_readers);
    }
    auto manifest = hydrate_manifest(
      imposter.api.local(), imposter.bucket_name);
    partition_probe probe(manifest.get_ntp());
    auto manifest_view = ss::make_shared<async_manifest_view>(
      imposter.api,
      imposter.cache,
      manifest,
      imposter.bucket_name,
      path_provider);
    auto partition = ss::make_shared<remote_partition>(
      manifest_view,
      imposter.api.local(),
      imposter.cache.local(),
      imposter.bucket_name,
      probe);
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
        vlog(test_util_log.info, "reader_config {}", reader_config);
        auto reader = partition->make_reader(reader_config).get().reader;
        auto headers_read
          = reader.consume(test_consumer(), model::no_timeout).get();
        if (headers_read.empty()) {
            break;
        }
        for (const auto& header : headers_read) {
            vlog(test_util_log.info, "header {}", header);
        }
        next = headers_read.back().last_offset() + model::offset(1);
        std::copy(
          headers_read.begin(),
          headers_read.end(),
          std::back_inserter(headers));
        num_fetches++;
    }
    BOOST_REQUIRE(num_fetches > 0);
    vlog(test_util_log.info, "{} fetch operations performed", num_fetches);
    return headers;
}

/// Similar to prev function but scans the range of offsets instead of
/// returning a single one
std::vector<model::record_batch_header> scan_remote_partition(
  cloud_storage_fixture& imposter,
  model::offset base,
  model::offset max,
  size_t maybe_max_segments,
  size_t maybe_max_readers) {
    // The lowres clock could become stale after reactor stall. In this
    // case, if the reactor stall was longer than 1s and the next the next
    // call to ss::lowres_clock::now() will result in a sudden jump forward
    // in time and the timeout for the next operation will be computed
    // incorrectly.
    ss::lowres_clock::update();
    auto conf = imposter.get_configuration();
    if (maybe_max_segments) {
        config::shard_local_cfg()
          .cloud_storage_max_materialized_segments_per_shard.set_value(
            maybe_max_segments);
    }
    if (maybe_max_readers) {
        config::shard_local_cfg()
          .cloud_storage_max_segment_readers_per_shard.set_value(
            maybe_max_readers);
    }
    storage::log_reader_config reader_config(
      base, max, ss::default_priority_class());

    auto manifest = hydrate_manifest(
      imposter.api.local(), imposter.bucket_name);
    partition_probe probe(manifest.get_ntp());
    auto manifest_view = ss::make_shared<async_manifest_view>(
      imposter.api,
      imposter.cache,
      manifest,
      imposter.bucket_name,
      path_provider);
    auto manifest_view_stop = ss::defer(
      [&manifest_view] { manifest_view->stop().get(); });
    manifest_view->start().get();

    auto partition = ss::make_shared<remote_partition>(
      manifest_view,
      imposter.api.local(),
      imposter.cache.local(),
      imposter.bucket_name,
      probe);
    auto partition_stop = ss::defer([&partition] { partition->stop().get(); });

    partition->start().get();

    auto reader = partition->make_reader(reader_config).get().reader;

    auto headers_read
      = reader.consume(test_consumer(), model::no_timeout).get();
    std::move(reader).release();

    return headers_read;
}

/// Similar to previous function but uses timequery to start the scan
scan_result scan_remote_partition(
  cloud_storage_fixture& imposter,
  model::offset min,
  model::timestamp timestamp,
  model::offset max,
  size_t maybe_max_segments,
  size_t maybe_max_readers) {
    // The lowres clock could become stale after reactor stall. In this
    // case, if the reactor stall was longer than 1s and the next the next
    // call to ss::lowres_clock::now() will result in a sudden jump forward
    // in time and the timeout for the next operation will be computed
    // incorrectly.
    ss::lowres_clock::update();
    auto conf = imposter.get_configuration();
    if (maybe_max_segments) {
        config::shard_local_cfg()
          .cloud_storage_max_materialized_segments_per_shard.set_value(
            maybe_max_segments);
    }
    if (maybe_max_readers) {
        config::shard_local_cfg()
          .cloud_storage_max_segment_readers_per_shard.set_value(
            maybe_max_readers);
    }
    auto manifest = hydrate_manifest(
      imposter.api.local(), imposter.bucket_name);
    storage::log_reader_config reader_config(
      min, max, ss::default_priority_class());
    reader_config.first_timestamp = timestamp;

    partition_probe probe(manifest.get_ntp());
    auto manifest_view = ss::make_shared<async_manifest_view>(
      imposter.api,
      imposter.cache,
      manifest,
      imposter.bucket_name,
      path_provider);
    auto manifest_view_stop = ss::defer(
      [&manifest_view] { manifest_view->stop().get(); });

    manifest_view->start().get();
    auto partition = ss::make_shared<remote_partition>(
      manifest_view,
      imposter.api.local(),
      imposter.cache.local(),
      imposter.bucket_name,
      probe);
    auto partition_stop = ss::defer([&partition] { partition->stop().get(); });

    partition->start().get();

    auto bytes_read = probe.get_bytes_read();
    auto bytes_skip = probe.get_bytes_skip();
    auto bytes_accept = probe.get_bytes_skip();
    auto records_read = probe.get_records_read();

    auto reader = partition->make_reader(reader_config).get().reader;

    auto headers_read
      = reader.consume(test_consumer(), model::no_timeout).get();
    std::move(reader).release();

    return {
      .headers = std::move(headers_read),
      .bytes_read = probe.get_bytes_read() - bytes_read,
      .records_read = probe.get_records_read() - records_read,
      .bytes_skip = probe.get_bytes_skip() - bytes_skip,
      .bytes_accept = probe.get_bytes_accept() - bytes_accept,
    };
}

std::vector<model::record_batch_header>
scan_remote_partition_incrementally_with_closest_lso(
  cloud_storage_fixture& imposter,
  model::offset base,
  model::offset max,
  size_t maybe_max_segments,
  size_t maybe_max_readers) {
    ss::lowres_clock::update();
    auto conf = imposter.get_configuration();
    if (maybe_max_segments) {
        config::shard_local_cfg()
          .cloud_storage_max_materialized_segments_per_shard.set_value(
            maybe_max_segments);
    }
    if (maybe_max_readers) {
        config::shard_local_cfg()
          .cloud_storage_max_segment_readers_per_shard.set_value(
            maybe_max_readers);
    }
    auto manifest = hydrate_manifest(
      imposter.api.local(), imposter.bucket_name);
    partition_probe probe(manifest.get_ntp());

    auto manifest_view = ss::make_shared<async_manifest_view>(
      imposter.api,
      imposter.cache,
      manifest,
      imposter.bucket_name,
      path_provider);

    auto partition = ss::make_shared<remote_partition>(
      manifest_view,
      imposter.api.local(),
      imposter.cache.local(),
      imposter.bucket_name,
      probe);

    auto partition_stop = ss::defer([&partition] { partition->stop().get(); });

    partition->start().get();

    std::vector<model::record_batch_header> headers;

    storage::log_reader_config reader_config(
      base, model::next_offset(base), ss::default_priority_class());

    // starting max_bytes
    reader_config.max_bytes = 1;

    auto next = base;

    int num_fetches = 0;
    while (next < max) {
        reader_config.start_offset = next;
        reader_config.max_offset = model::next_offset(next);
        vlog(test_util_log.info, "reader_config {}", reader_config);
        auto reader = partition->make_reader(reader_config).get().reader;
        auto headers_read
          = reader.consume(test_consumer(), model::no_timeout).get();
        if (headers_read.empty()) {
            // If the reader returned the empty result then the offset
            // corresponds to tx-batch. Our own tx-batches looks like offset
            // gaps to the client. We're always adding tx-batches with only one
            // record so we can increment the 'next' offset and continue.
            next = model::next_offset(next);
            vlog(
              test_util_log.info,
              "Reader config: {} produced empty result, next offset set to {}",
              reader_config,
              next);
            // test is prepared to see the gaps in place of tx-fence batches
            continue;
        }
        BOOST_REQUIRE(headers_read.size() == 1);
        vlog(test_util_log.info, "header {}", headers_read.front());
        next = headers_read.back().last_offset() + model::offset(1);
        std::copy(
          headers_read.begin(),
          headers_read.end(),
          std::back_inserter(headers));
        num_fetches++;
    }
    BOOST_REQUIRE(num_fetches > 0);
    vlog(test_util_log.info, "{} fetch operations performed", num_fetches);
    return headers;
}

void reupload_compacted_segments(
  cloud_storage_fixture& fixture,
  cloud_storage::partition_manifest& m,
  const std::vector<in_memory_segment>& segments,
  bool truncate_segments) {
    ss::lowres_clock::update();
    static ss::abort_source never_abort;

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

            auto url = m.generate_segment_path(
              *m.get(meta.base_offset), path_provider);
            vlog(test_util_log.debug, "reuploading segment {}", url);
            retry_chain_node rtc(never_abort, 60s, 1s);
            bytes bb;
            bb.resize(body.size());
            std::memcpy(bb.data(), body.data(), body.size());
            auto reset_stream = [body = std::move(bb)] {
                return ss::make_ready_future<std::unique_ptr<stream_provider>>(
                  std::make_unique<storage::segment_reader_handle>(
                    make_iobuf_input_stream(bytes_to_iobuf(body))));
            };
            auto result = fixture.api.local()
                            .upload_segment(
                              fixture.bucket_name,
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

} // namespace cloud_storage
