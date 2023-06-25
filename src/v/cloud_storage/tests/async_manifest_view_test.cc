/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "cloud_storage/async_manifest_view.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/tests/cloud_storage_fixture.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/tests/util.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "test_utils/fixture.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/testing/seastar_test.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <iterator>
#include <numeric>

using namespace cloud_storage;

static ss::logger test_log("async_manifest_view_log");
static const model::initial_revision_id manifest_rev(111);

class set_config_mixin {
public:
    static constexpr std::chrono::milliseconds cache_ttl = 100ms;
    set_config_mixin() {
        config::shard_local_cfg().cloud_storage_manifest_cache_size.set_value(
          (size_t)40960);
        config::shard_local_cfg().cloud_storage_manifest_cache_ttl_ms.set_value(
          cache_ttl);
    }
};

class async_manifest_view_fixture
  : public cloud_storage_fixture
  , public set_config_mixin {
public:
    async_manifest_view_fixture()
      : cloud_storage_fixture()
      , stm_manifest(manifest_ntp, manifest_rev)
      , bucket("test-bucket")
      , rtc(as)
      , ctxlog(test_log, rtc)
      , probe(manifest_ntp)
      , view(api, cache, stm_manifest, bucket, probe) {
        stm_manifest.set_archive_start_offset(
          model::offset{0}, model::offset_delta{0});
        stm_manifest.set_archive_clean_offset(model::offset{0}, 0);
        view.start().get();
        base_timestamp = model::timestamp_clock::now() - storage_duration;
        last_timestamp = base_timestamp;
    }

    ~async_manifest_view_fixture() { view.stop().get(); }

    // The current content of the manifest will be spilled over to the archive
    // and new elements will be generated.
    void generate_manifest_section(int num_segments, bool hydrate = true) {
        if (stm_manifest.empty()) {
            add_random_segments(stm_manifest, num_segments);
        }
        auto so = model::next_offset(stm_manifest.get_last_offset());
        add_random_segments(stm_manifest, num_segments);
        auto tmp = stm_manifest.spillover(so);
        spillover_manifest spm(manifest_ntp, manifest_rev);
        for (const auto& meta : tmp) {
            spm.add(meta);
        }
        // update cache
        auto path = spm.get_manifest_path();
        if (hydrate) {
            auto stream = spm.serialize().get();
            cache.local()
              .put(path, stream.stream, ss::default_priority_class())
              .get();
            stream.stream.close().get();
        }
        // upload to the cloud
        auto [in_stream, size_bytes] = spm.serialize().get();
        iobuf tmp_buf;
        auto out_stream = make_iobuf_ref_output_stream(tmp_buf);
        ss::copy(in_stream, out_stream).get();
        in_stream.close().get();
        out_stream.close().get();
        ss::sstring body = linearize_iobuf(std::move(tmp_buf));
        expectation exp{
          .url = path().string(),
          .body = body,
        };
        _expectations.push_back(std::move(exp));
        spillover_start_offsets.push_back(so);
    }

    void listen() { set_expectations_and_listen(_expectations); }

    void collect_segments_to(std::vector<segment_meta>& meta) {
        all_segments = std::ref(meta);
    }

    // Generate random segments and add them to the manifest
    void add_random_segments(partition_manifest& manifest, int num_segments) {
        auto base = manifest.empty()
                      ? model::offset(0)
                      : model::next_offset(manifest.get_last_offset());
        auto delta = model::offset_delta(0);
        static constexpr int64_t ts_step = 1000;
        static constexpr size_t segment_size = 4097;
        for (int i = 0; i < num_segments; i++) {
            auto last = base
                        + model::offset(random_generators::get_int(1, 100));
            auto delta_end = model::offset_delta(
              random_generators::get_int(delta(), delta() + delta()));
            segment_meta meta{
              .is_compacted = false,
              .size_bytes = segment_size,
              .base_offset = base,
              .committed_offset = last,
              .base_timestamp = model::to_timestamp(last_timestamp),
              .max_timestamp = model::to_timestamp(last_timestamp),
              .delta_offset = delta,
              .ntp_revision = manifest_rev,
              .archiver_term = model::term_id(1),
              .segment_term = model::term_id(1),
              .delta_offset_end = delta_end,
              .sname_format = segment_name_format::v3,
            };
            base = model::next_offset(last);
            delta = delta_end;
            last_timestamp += std::chrono::milliseconds(ts_step);
            manifest.add(meta);
            if (all_segments.has_value()) {
                all_segments->get().push_back(manifest.last_segment().value());
            }
        }
    }

    void print_diff(
      const std::vector<segment_meta>& actual,
      const std::vector<segment_meta>& expected,
      int limit = 4) {
        int quota = limit;
        if (expected != actual) {
            auto lhs = expected.begin();
            auto rhs = actual.begin();
            while (lhs != expected.end()) {
                if (*lhs != *rhs) {
                    vlog(
                      test_log.info,
                      "{} - expected: {}, actual: {}",
                      limit - quota,
                      *lhs,
                      *rhs);
                }
                quota--;
                if (quota > 0) {
                    break;
                }
                ++lhs;
                ++rhs;
            }
        }
    }

    partition_manifest stm_manifest;
    cloud_storage_clients::bucket_name bucket;
    ss::abort_source as;
    retry_chain_node rtc;
    retry_chain_logger ctxlog;
    partition_probe probe;
    async_manifest_view view;
    std::vector<expectation> _expectations;
    std::vector<model::offset> spillover_start_offsets;
    model::offset _last_spillover_offset;
    std::optional<std::reference_wrapper<std::vector<segment_meta>>>
      all_segments;
    model::timestamp_clock::time_point base_timestamp;
    model::timestamp_clock::time_point last_timestamp;
    static constexpr std::chrono::milliseconds storage_duration = 10h;
};

FIXTURE_TEST(test_async_manifest_view_base, async_manifest_view_fixture) {
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    listen();

    auto cursor = view.get_cursor(model::offset{0}).get();
    BOOST_REQUIRE(cursor.has_value());
}

FIXTURE_TEST(test_async_manifest_view_fetch, async_manifest_view_fixture) {
    // Generate series of spillover manifests and query them individually
    // using `view.get_cursor(offset)` calls.
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    listen();

    for (auto so : spillover_start_offsets) {
        vlog(test_log.info, "Get cursor for offset {}", so);
        auto cursor = view.get_cursor(so).get();
        BOOST_REQUIRE(cursor.has_value());

        cursor.value()->with_manifest([so](const partition_manifest& m) {
            BOOST_REQUIRE_EQUAL(m.get_start_offset().value(), so);
        });

        auto next = std::upper_bound(
          spillover_start_offsets.begin(), spillover_start_offsets.end(), so);

        if (next != spillover_start_offsets.end()) {
            cursor.value()->with_manifest([next](const partition_manifest& m) {
                vlog(test_log.info, "Checking spillover manifest");
                BOOST_REQUIRE_EQUAL(
                  model::next_offset(m.get_last_offset()), *next);
            });
        } else {
            cursor.value()->with_manifest([this](const partition_manifest& m) {
                vlog(test_log.info, "Checking STM manifest");
                BOOST_REQUIRE_EQUAL(
                  m.get_start_offset(),
                  stm_manifest.get_start_offset().value());
            });
        }
    }
}

FIXTURE_TEST(test_async_manifest_view_iter, async_manifest_view_fixture) {
    std::vector<segment_meta> expected;
    collect_segments_to(expected);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    listen();

    std::vector<segment_meta> actual;
    model::offset so = model::offset{0};
    auto maybe_cursor = view.get_cursor(so).get();
    if (maybe_cursor.has_failure()) {
        BOOST_REQUIRE(
          maybe_cursor.error() == error_outcome::manifest_not_found);
    }
    auto cursor = std::move(maybe_cursor.value());
    do {
        cursor->with_manifest([&](const partition_manifest& m) {
            for (auto meta : m) {
                actual.push_back(meta);
            }
        });
    } while (cursor->next().get().value());
    print_diff(actual, expected);
    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());
    BOOST_REQUIRE(expected == actual);
}

FIXTURE_TEST(test_async_manifest_view_truncate, async_manifest_view_fixture) {
    // Check that segments in the truncated part are not accessible
    std::vector<segment_meta> expected;
    collect_segments_to(expected);
    generate_manifest_section(100);
    auto clean_offset = stm_manifest.get_start_offset().value();
    generate_manifest_section(100);
    generate_manifest_section(100);
    auto new_so = model::next_offset(
      stm_manifest.last_segment()->committed_offset);
    auto new_delta = stm_manifest.last_segment()->delta_offset_end;
    std::vector<segment_meta> removed;
    std::swap(expected, removed);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    listen();

    vlog(test_log.info, "Set archive start offset to {}", new_so);
    stm_manifest.set_archive_start_offset(new_so, new_delta);

    model::offset so = model::offset{0};
    auto maybe_cursor = view.get_cursor(so).get();
    BOOST_REQUIRE(maybe_cursor.has_failure());
    BOOST_REQUIRE(maybe_cursor.error() == error_outcome::out_of_range);

    maybe_cursor = view.get_cursor(new_so).get();
    BOOST_REQUIRE(!maybe_cursor.has_failure());

    std::vector<segment_meta> actual;
    auto cursor = std::move(maybe_cursor.value());
    do {
        cursor->with_manifest([&](const partition_manifest& m) {
            vlog(
              test_log.info,
              "Looking at the manifest [{}/{}], archive start: {}",
              m.get_start_offset(),
              m.get_last_offset(),
              stm_manifest.get_archive_start_offset());
            for (auto meta : m) {
                actual.push_back(meta);
            }
        });
    } while (cursor->next().get().value());
    print_diff(actual, expected);
    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());
    BOOST_REQUIRE(expected == actual);

    auto backlog_cursor = view.get_retention_backlog().get();
    BOOST_REQUIRE(!backlog_cursor.has_failure());

    actual.clear();
    cursor = std::move(backlog_cursor.value());
    do {
        cursor->with_manifest([&](const partition_manifest& m) {
            vlog(
              test_log.info,
              "Looking at the backlog manifest [{}/{}], archive start: {}",
              m.get_start_offset(),
              m.get_last_offset(),
              stm_manifest.get_archive_start_offset());
            for (auto meta : m) {
                actual.push_back(meta);
            }
        });
    } while (cursor->next().get().value());
    print_diff(actual, removed);
    BOOST_REQUIRE_EQUAL(removed.size(), actual.size());
    BOOST_REQUIRE(removed == actual);

    // Move clean offset and check that the backlog is updated
    // correctly.
    stm_manifest.set_archive_clean_offset(clean_offset, 0);
    std::erase_if(removed, [clean_offset](const segment_meta& m) {
        return m.committed_offset < clean_offset;
    });
    actual.clear();
    backlog_cursor = view.get_retention_backlog().get();
    BOOST_REQUIRE(!backlog_cursor.has_failure());
    cursor = std::move(backlog_cursor.value());
    do {
        cursor->with_manifest([&](const partition_manifest& m) {
            vlog(
              test_log.info,
              "Looking at the backlog manifest [{}/{}], archive start: {}",
              m.get_start_offset(),
              m.get_last_offset(),
              stm_manifest.get_archive_start_offset());
            for (auto meta : m) {
                actual.push_back(meta);
            }
        });
    } while (cursor->next().get().value());
    print_diff(actual, removed);
    BOOST_REQUIRE_EQUAL(removed.size(), actual.size());
    BOOST_REQUIRE(removed == actual);
}

FIXTURE_TEST(
  test_async_manifest_view_truncate_mid_manifest, async_manifest_view_fixture) {
    // Check that segments in the truncated part are not accessible
    std::vector<segment_meta> expected;
    collect_segments_to(expected);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    auto [new_so, new_so_delta] = [this] {
        model::offset so;
        model::offset_delta delta;
        auto quota = 20;
        for (const auto& meta : stm_manifest) {
            so = meta.base_offset;
            delta = meta.delta_offset;
            quota--;
            if (quota == 0) {
                break;
            }
        }
        return std::make_tuple(so, delta);
    }();
    std::vector<segment_meta> removed;
    auto eit = std::find_if(
      expected.begin(), expected.end(), [o = new_so](segment_meta m) {
          return m.base_offset == o;
      });
    vlog(test_log.info, "Removing expected elements up to {}", *eit);
    std::copy(expected.begin(), eit, std::back_inserter(removed));
    expected.erase(expected.begin(), eit);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    listen();

    vlog(test_log.info, "Setting archive start offset to {}", new_so);
    stm_manifest.set_archive_start_offset(new_so, new_so_delta);

    auto maybe_cursor = view.get_cursor(new_so).get();
    BOOST_REQUIRE(!maybe_cursor.has_failure());

    vlog(test_log.info, "Validating async_manifest_view content");
    std::vector<segment_meta> actual;
    auto cursor = std::move(maybe_cursor.value());
    do {
        cursor->with_manifest([&](const partition_manifest& m) {
            vlog(
              test_log.info,
              "Looking at the manifest {}/{}",
              m.get_start_offset(),
              m.get_last_offset());
            for (auto meta : m) {
                if (
                  meta.base_offset < stm_manifest.get_archive_start_offset()) {
                    // The cursor only returns full manifests. If the new
                    // archive start offset is in the middle of the manifest
                    // it will return the whole manifest and the user has
                    // to skip all segments below the archive start offset
                    // manually.
                    continue;
                }
                actual.push_back(meta);
            }
        });
    } while (cursor->next().get().value());
    print_diff(actual, expected);
    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());
    BOOST_REQUIRE(expected == actual);

    vlog(test_log.info, "Validating async_manifest_view backlog");
    auto backlog_cursor = view.get_retention_backlog().get();
    BOOST_REQUIRE(!backlog_cursor.has_failure());

    actual.clear();
    cursor = std::move(backlog_cursor.value());
    do {
        cursor->with_manifest([&](const partition_manifest& m) {
            vlog(
              test_log.info,
              "Looking at the manifest {}/{}",
              m.get_start_offset(),
              m.get_last_offset());
            for (auto meta : m) {
                if (
                  meta.base_offset >= stm_manifest.get_archive_start_offset()) {
                    // The cursor only returns full manifests. If the new
                    // archive start offset is in the middle of the manifest
                    // the backlog will contain full manifest and the user has
                    // to read up until the start offset of the manifest.
                    break;
                }
                actual.push_back(meta);
            }
        });
    } while (cursor->next().get().value());
    print_diff(actual, removed);
    BOOST_REQUIRE_EQUAL(removed.size(), actual.size());
    BOOST_REQUIRE(removed == actual);
}

FIXTURE_TEST(test_async_manifest_view_evict, async_manifest_view_fixture) {
    for (int i = 0; i < 20; i++) {
        generate_manifest_section(100);
    }
    listen();

    model::offset so = model::offset{0};
    auto maybe_cursor = view.get_cursor(so).get();
    BOOST_REQUIRE(!maybe_cursor.has_failure());
    auto stale_cursor = std::move(maybe_cursor.value());

    // Force eviction of the stale_cursor
    vlog(test_log.debug, "Saturating cache");
    std::vector<std::unique_ptr<cloud_storage::async_manifest_view_cursor>>
      cursors;
    for (auto it = std::next(spillover_start_offsets.begin());
         it != spillover_start_offsets.end();
         it++) {
        auto o = *it;
        vlog(test_log.debug, "Fetching manifest for offset {}", o);
        auto tmp_cursor = view.get_cursor(o).get();
        BOOST_REQUIRE(!tmp_cursor.has_failure());
        auto cursor = std::move(tmp_cursor.value());
        cursor->with_manifest([o](const partition_manifest& m) {
            BOOST_REQUIRE_EQUAL(o, m.get_start_offset().value());
        });
        cursors.emplace_back(std::move(cursor));
    }
    BOOST_REQUIRE_EQUAL(cursors.size(), spillover_start_offsets.size() - 1);

    ss::sleep(cache_ttl * 2).get();

    vlog(
      test_log.debug,
      "Cursor's actual status: {}, expected status: {}",
      stale_cursor->get_status(),
      async_manifest_view_cursor_status::evicted);
    BOOST_REQUIRE(
      stale_cursor->get_status() == async_manifest_view_cursor_status::evicted);
}

FIXTURE_TEST(test_async_manifest_view_retention, async_manifest_view_fixture) {
    std::vector<segment_meta> expected;
    collect_segments_to(expected);
    for (int i = 0; i < 10; i++) {
        generate_manifest_section(100);
    }
    listen();

    size_t total_size = 0;
    for (const auto& meta : expected) {
        total_size += meta.size_bytes;
    }

    // Check the case when retention overshoots
    auto rr1 = view.compute_retention(total_size * 2, std::nullopt).get();
    BOOST_REQUIRE(rr1.has_value());
    BOOST_REQUIRE(rr1.value().offset == model::offset{});
    BOOST_REQUIRE(rr1.value().delta == model::offset_delta{});

    auto rr2 = view.compute_retention(std::nullopt, storage_duration * 2).get();
    BOOST_REQUIRE(rr2.has_value());
    BOOST_REQUIRE(rr2.value().offset == model::offset{});
    BOOST_REQUIRE(rr2.value().delta == model::offset_delta{});

    auto rr3
      = view.compute_retention(total_size * 2, storage_duration * 2).get();
    BOOST_REQUIRE(rr3.has_value());
    BOOST_REQUIRE(rr3.value().offset == model::offset{});
    BOOST_REQUIRE(rr3.value().delta == model::offset_delta{});

    // Check the case when time-based retention wins
    int quota = 50;
    size_t prefix_size = 0;
    model::timestamp prefix_timestamp;
    model::offset prefix_base_offset;
    model::offset_delta prefix_delta;
    for (const auto& meta : expected) {
        prefix_size += meta.size_bytes;
        prefix_timestamp = meta.base_timestamp;
        prefix_base_offset = meta.base_offset;
        prefix_delta = meta.delta_offset;
        quota--;
        if (quota == 0) {
            break;
        }
    }

    vlog(
      test_log.info,
      "Triggering size-based retention, {} bytes will be evicted, total size "
      "is {} bytes, expected new start offset: {}",
      prefix_size,
      total_size,
      prefix_base_offset);
    auto rr4 = view
                 .compute_retention(total_size - prefix_size, storage_duration)
                 .get();
    BOOST_REQUIRE(rr4.has_value());
    BOOST_REQUIRE_EQUAL(rr4.value().offset, prefix_base_offset);
    BOOST_REQUIRE_EQUAL(rr4.value().delta, prefix_delta);

    // Check the case when size-based retention wins
    auto now = model::timestamp::now();
    auto delta = now - prefix_timestamp;
    vlog(
      test_log.info,
      "Triggering time-based retention at {}, delta {}, expected new start "
      "offset: {}",
      prefix_timestamp,
      delta,
      prefix_base_offset);
    auto rr5 = view
                 .compute_retention(
                   total_size, std::chrono::milliseconds(delta.value()) + 1s)
                 .get();
    BOOST_REQUIRE(rr5.has_value());
    BOOST_REQUIRE_EQUAL(rr5.value().offset, prefix_base_offset);
    BOOST_REQUIRE_EQUAL(rr5.value().delta, prefix_delta);

    // Check case when the start offset in the archive is advanced past
    // start kafka offset override.
    auto cur_res
      = view.get_cursor(*view.stm_manifest().get_start_offset()).get();
    BOOST_REQUIRE(!cur_res.has_error());
    auto cur = std::move(cur_res.value());
    // Set expected offset to the start of the second segment
    cur->next().get();
    prefix_base_offset = cur->manifest()->get().begin()->base_offset;
    prefix_delta = cur->manifest()->get().begin()->delta_offset;
    stm_manifest.advance_start_kafka_offset(prefix_base_offset - prefix_delta);
    vlog(
      test_log.info,
      "Triggering offset-based retention, current start kafka offset override: "
      "{}, expected offset: {}, expected delta: {}",
      stm_manifest.get_start_kafka_offset_override(),
      prefix_base_offset,
      prefix_delta);

    auto rr6 = view.compute_retention(total_size, storage_duration).get();

    BOOST_REQUIRE(rr6.has_value());
    BOOST_REQUIRE_EQUAL(rr6.value().offset, prefix_base_offset);
    BOOST_REQUIRE_EQUAL(rr6.value().delta, prefix_delta);
}
