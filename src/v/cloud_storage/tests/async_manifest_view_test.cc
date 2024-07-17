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
#include "model/timestamp.h"
#include "test_utils/fixture.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/testing/seastar_test.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <iterator>
#include <numeric>

using namespace cloud_storage;
using eof = async_manifest_view_cursor::eof;

static ss::logger test_log("async_manifest_view_log");
static const model::initial_revision_id manifest_rev(111);
static const remote_path_provider path_provider(std::nullopt, std::nullopt);

class set_config_mixin {
public:
    static constexpr std::chrono::milliseconds cache_ttl = 100ms;
    set_config_mixin() {
        config::shard_local_cfg().cloud_storage_manifest_cache_size.set_value(
          (size_t)40960);
        config::shard_local_cfg().cloud_storage_manifest_cache_ttl_ms.set_value(
          cache_ttl);
    }

    ~set_config_mixin() {
        config::shard_local_cfg().cloud_storage_manifest_cache_size.reset();
        config::shard_local_cfg().cloud_storage_manifest_cache_ttl_ms.reset();
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
      , view(api, cache, stm_manifest, bucket, path_provider) {
        stm_manifest.set_archive_start_offset(
          model::offset{0}, model::offset_delta{0});
        stm_manifest.set_archive_clean_offset(model::offset{0}, 0);
        view.start().get();
        base_timestamp = model::timestamp_clock::now() - storage_duration;
        last_timestamp = base_timestamp;
    }

    ~async_manifest_view_fixture() { view.stop().get(); }

    expectation spill_manifest(const spillover_manifest& spm, bool hydrate) {
        stm_manifest.spillover(spm.make_manifest_metadata());
        // update cache
        auto path = spm.get_manifest_path(path_provider);
        if (hydrate) {
            auto stream = spm.serialize().get();
            auto reservation = cache.local().reserve_space(123, 1).get();
            cache.local()
              .put(
                path, stream.stream, reservation, ss::default_priority_class())
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
        return expectation{
          .url = path().string(),
          .body = body,
        };
    }

    /// List of files with serialized spillover manifests
    /// and STM manifest.
    struct manifest_files {
        ss::sstring stm_path;
        std::vector<ss::sstring> spills;
    };

    void load_stm_manifest_from_file(const ss::sstring& stm_path) {
        // Load the manifest from file
        iobuf body;
        auto stm_file = ss::open_file_dma(stm_path, ss::open_flags::ro).get();
        auto inp_str = ss::make_file_input_stream(stm_file);
        auto out_str = make_iobuf_ref_output_stream(body);
        ss::copy(inp_str, out_str).get();

        // The stm_manifest is created with different ntp/rev
        stm_manifest.from_iobuf(std::move(body));
        // No need to upload to S3
    }

    void put_spill_to_cache(const spillover_manifest& spm) {
        auto path = spm.get_manifest_path(path_provider);
        auto stream = spm.serialize().get();
        auto reservation = cache.local().reserve_space(123, 1).get();
        cache.local()
          .put(path, stream.stream, reservation, ss::default_priority_class())
          .get();
        stream.stream.close().get();
    }

    void load_spillover_manifest_from_file(const ss::sstring& spill_path) {
        iobuf body;
        auto file = ss::open_file_dma(spill_path, ss::open_flags::ro).get();
        auto inp_str = ss::make_file_input_stream(file);
        auto out_str = make_iobuf_ref_output_stream(body);
        ss::copy(inp_str, out_str).get();

        // Try to use different ntp and rev
        spillover_manifest spm(manifest_ntp, manifest_rev);
        spm.from_iobuf(std::move(body));
        upload_to_s3_imposter(spm);
        put_spill_to_cache(spm);
    }

    // Upload the manifest to the "cloud storage"
    void upload_to_s3_imposter(const partition_manifest& pm) {
        auto [in_stream, size_bytes] = pm.serialize().get();
        iobuf tmp_buf;
        auto out_stream = make_iobuf_ref_output_stream(tmp_buf);
        ss::copy(in_stream, out_stream).get();
        in_stream.close().get();
        out_stream.close().get();
        ss::sstring body = linearize_iobuf(std::move(tmp_buf));
        auto path = pm.get_manifest_path(path_provider);
        _expectations.push_back({
          .url = path().string(),
          .body = body,
        });
        vlog(
          test_log.info,
          "Spillover manifest uploaded to {}",
          _expectations.back().url);
    }

    // The current content of the manifest will be spilled over to the
    // archive and new elements will be generated.
    void generate_manifest_section(int num_segments, bool hydrate = true) {
        if (stm_manifest.empty()) {
            add_random_segments(stm_manifest, num_segments);
        }
        auto so = model::next_offset(stm_manifest.get_last_offset());
        add_random_segments(stm_manifest, num_segments);
        spillover_manifest spm(manifest_ntp, manifest_rev);
        for (const auto& meta : stm_manifest) {
            if (meta.committed_offset >= so) {
                break;
            }
            spm.add(meta);
        }
        auto exp = spill_manifest(spm, hydrate);
        _expectations.push_back(std::move(exp));
        spillover_start_offsets.push_back(so);
    }

    void trigger_spillover(int num_segments, bool hydrate = true) {
        BOOST_REQUIRE_GT(stm_manifest.size(), num_segments + 1);

        const auto so = model::next_offset(stm_manifest.get_last_offset());
        spillover_manifest spm(manifest_ntp, manifest_rev);
        for (const auto& meta : stm_manifest) {
            spm.add(meta);
            if (--num_segments == 0) {
                break;
            }
        }

        stm_manifest.spillover(spm.make_manifest_metadata());

        // update cache
        auto path = spm.get_manifest_path(path_provider);
        if (hydrate) {
            auto stream = spm.serialize().get();
            auto reservation = cache.local().reserve_space(123, 1).get();
            cache.local()
              .put(
                path, stream.stream, reservation, ss::default_priority_class())
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

    void add_segments_to_stm_manifest(std::vector<segment_meta> segs) {
        for (const auto& meta : segs) {
            stm_manifest.add(meta);
            if (all_segments.has_value()) {
                all_segments->get().push_back(
                  stm_manifest.last_segment().value());
            }
        }
    }

    void listen() { set_expectations_and_listen(_expectations); }

    void collect_segments_to(std::vector<segment_meta>& meta) {
        all_segments = std::ref(meta);
    }

    std::vector<segment_meta>
    generate_random_segments(partition_manifest& manifest, int num_segments) {
        std::vector<segment_meta> segs;
        segs.reserve(num_segments);

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

            segs.push_back(meta);
        }

        return segs;
    }

    // Generate random segments and add them to the manifest
    void add_random_segments(partition_manifest& manifest, int num_segments) {
        auto segs = generate_random_segments(manifest, num_segments);
        add_segments_to_stm_manifest(std::move(segs));
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

        cursor.value()
          ->with_manifest([so](const partition_manifest& m) {
              BOOST_REQUIRE_EQUAL(m.get_start_offset().value(), so);
          })
          .get();

        auto next = std::upper_bound(
          spillover_start_offsets.begin(), spillover_start_offsets.end(), so);

        if (next != spillover_start_offsets.end()) {
            cursor.value()
              ->with_manifest([next](const partition_manifest& m) {
                  vlog(test_log.info, "Checking spillover manifest");
                  BOOST_REQUIRE_EQUAL(
                    model::next_offset(m.get_last_offset()), *next);
              })
              .get();
        } else {
            cursor.value()
              ->with_manifest([this](const partition_manifest& m) {
                  vlog(test_log.info, "Checking STM manifest");
                  BOOST_REQUIRE_EQUAL(
                    m.get_start_offset(),
                    stm_manifest.get_start_offset().value());
              })
              .get();
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
        cursor
          ->with_manifest([&](const partition_manifest& m) {
              for (auto meta : m) {
                  actual.push_back(meta);
              }
          })
          .get();
    } while (cursor->next().get().value() != eof::yes);
    print_diff(actual, expected);
    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());
    BOOST_REQUIRE(expected == actual);
}

FIXTURE_TEST(test_async_manifest_view_truncate, async_manifest_view_fixture) {
    // Check archive truncation
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
    BOOST_REQUIRE(
      maybe_cursor.has_error()
      && maybe_cursor.error() == cloud_storage::error_outcome::out_of_range);

    // The clean offset should still be accesible such that retention
    // can operate above it.
    maybe_cursor = view
                     .get_cursor(
                       so,
                       std::nullopt,
                       cloud_storage::async_manifest_view::cursor_base_t::
                         archive_clean_offset)
                     .get();
    BOOST_REQUIRE(!maybe_cursor.has_failure());

    maybe_cursor = view.get_cursor(new_so).get();
    BOOST_REQUIRE(!maybe_cursor.has_failure());

    std::vector<segment_meta> actual;
    auto cursor = std::move(maybe_cursor.value());
    do {
        cursor
          ->with_manifest([&](const partition_manifest& m) {
              vlog(
                test_log.info,
                "Looking at the manifest [{}/{}], archive start: {}",
                m.get_start_offset(),
                m.get_last_offset(),
                stm_manifest.get_archive_start_offset());
              for (auto meta : m) {
                  actual.push_back(meta);
              }
          })
          .get();
    } while (cursor->next().get().value() != eof::yes);
    print_diff(actual, expected);
    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());
    BOOST_REQUIRE(expected == actual);

    auto backlog_cursor = view.get_retention_backlog().get();
    BOOST_REQUIRE(!backlog_cursor.has_failure());

    actual.clear();
    cursor = std::move(backlog_cursor.value());
    do {
        cursor
          ->with_manifest([&](const partition_manifest& m) {
              vlog(
                test_log.info,
                "Looking at the backlog manifest [{}/{}], archive start: "
                "{}",
                m.get_start_offset(),
                m.get_last_offset(),
                stm_manifest.get_archive_start_offset());
              for (auto meta : m) {
                  actual.push_back(meta);
              }
          })
          .get();
    } while (cursor->next().get().value() != eof::yes);
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
        cursor
          ->with_manifest([&](const partition_manifest& m) {
              vlog(
                test_log.info,
                "Looking at the backlog manifest [{}/{}], archive start: "
                "{}",
                m.get_start_offset(),
                m.get_last_offset(),
                stm_manifest.get_archive_start_offset());
              for (auto meta : m) {
                  actual.push_back(meta);
              }
          })
          .get();
    } while (cursor->next().get().value() != eof::yes);
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
        cursor
          ->with_manifest([&](const partition_manifest& m) {
              vlog(
                test_log.info,
                "Looking at the manifest {}/{}",
                m.get_start_offset(),
                m.get_last_offset());
              for (auto meta : m) {
                  if (
                    meta.base_offset
                    < stm_manifest.get_archive_start_offset()) {
                      // The cursor only returns full manifests. If the new
                      // archive start offset is in the middle of the
                      // manifest it will return the whole manifest and the
                      // user has to skip all segments below the archive
                      // start offset manually.
                      continue;
                  }
                  actual.push_back(meta);
              }
          })
          .get();
    } while (cursor->next().get().value() != eof::yes);
    print_diff(actual, expected);
    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());
    BOOST_REQUIRE(expected == actual);

    vlog(test_log.info, "Validating async_manifest_view backlog");
    auto backlog_cursor = view.get_retention_backlog().get();
    BOOST_REQUIRE(!backlog_cursor.has_failure());

    actual.clear();
    cursor = std::move(backlog_cursor.value());
    do {
        cursor
          ->with_manifest([&](const partition_manifest& m) {
              vlog(
                test_log.info,
                "Looking at the manifest {}/{}",
                m.get_start_offset(),
                m.get_last_offset());
              for (auto meta : m) {
                  if (
                    meta.base_offset
                    >= stm_manifest.get_archive_start_offset()) {
                      // The cursor only returns full manifests. If the new
                      // archive start offset is in the middle of the
                      // manifest the backlog will contain full manifest and
                      // the user has to read up until the start offset of
                      // the manifest.
                      break;
                  }
                  actual.push_back(meta);
              }
          })
          .get();
    } while (cursor->next().get().value() != eof::yes);
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
        cursor
          ->with_manifest([o](const partition_manifest& m) {
              BOOST_REQUIRE_EQUAL(o, m.get_start_offset().value());
          })
          .get();
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
    // auto rr1 = view.compute_retention(total_size * 2,
    // std::nullopt).get(); BOOST_REQUIRE(rr1.has_value());
    // BOOST_REQUIRE_EQUAL(rr1.value().offset, model::offset{});
    // BOOST_REQUIRE_EQUAL(rr1.value().delta, model::offset_delta{});

    auto rr2 = view.compute_retention(std::nullopt, storage_duration * 2).get();
    BOOST_REQUIRE(rr2.has_value());
    BOOST_REQUIRE_EQUAL(rr2.value().offset, model::offset{});
    BOOST_REQUIRE_EQUAL(rr2.value().delta, model::offset_delta{});
    return;

    auto rr3
      = view.compute_retention(total_size * 2, storage_duration * 2).get();
    BOOST_REQUIRE(rr3.has_value());
    BOOST_REQUIRE_EQUAL(rr3.value().offset, model::offset{});
    BOOST_REQUIRE_EQUAL(rr3.value().delta, model::offset_delta{});

    // Check the case when time-based retention wins
    int quota = 50;
    size_t prefix_size = 0;
    model::timestamp prefix_timestamp;
    model::offset prefix_base_offset;
    model::offset_delta prefix_delta;
    for (const auto& meta : expected) {
        prefix_timestamp = meta.base_timestamp;
        prefix_base_offset = meta.base_offset;
        prefix_delta = meta.delta_offset;

        if (quota == 0) {
            break;
        }

        prefix_size += meta.size_bytes;
        quota--;
    }

    vlog(
      test_log.info,
      "Triggering size-based retention, {} bytes will be evicted, total "
      "size "
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
    prefix_base_offset = cur->manifest()->begin()->base_offset;
    prefix_delta = cur->manifest()->begin()->delta_offset;
    stm_manifest.advance_start_kafka_offset(prefix_base_offset - prefix_delta);
    vlog(
      test_log.info,
      "Triggering offset-based retention, current start kafka offset "
      "override: "
      "{}, expected offset: {}, expected delta: {}",
      stm_manifest.get_start_kafka_offset_override(),
      prefix_base_offset,
      prefix_delta);

    auto rr6 = view.compute_retention(total_size, storage_duration).get();

    BOOST_REQUIRE(rr6.has_value());
    BOOST_REQUIRE_EQUAL(rr6.value().offset, prefix_base_offset);
    BOOST_REQUIRE_EQUAL(rr6.value().delta, prefix_delta);
}

FIXTURE_TEST(test_async_manifest_view_after_gc, async_manifest_view_fixture) {
    /*
     * This test checks that that retention uses the archive clean offset
     * as its starting point. It artificially advances the archive start
     * offset and expects for the truncation point to be set past the end
     * of the first segment in the "active" archive.
     */
    std::vector<segment_meta> expected;
    collect_segments_to(expected);
    for (int i = 0; i < 3; i++) {
        generate_manifest_section(5);
    }

    listen();

    // This test expects all generated segments to have the same size.
    const auto segments_to_keep = 6;
    const size_t size_limit = segments_to_keep * expected[0].size_bytes;
    const auto truncation_point = expected[expected.size() - segments_to_keep];

    // The test created 3 spillover manifest with 5 segments each.
    // The STM manifest also has 5 segments. Advance the start offset
    // to the second segment in the second spillover manifest.
    BOOST_REQUIRE(spillover_start_offsets.size() == 3);
    const auto second_spill_start = spillover_start_offsets[0];
    auto iter = std::next(std::find_if(
      expected.begin(), expected.end(), [second_spill_start](const auto& meta) {
          return meta.base_offset == second_spill_start;
      }));
    const auto second_seg_second_spill = *iter;

    stm_manifest.set_archive_start_offset(
      second_seg_second_spill.base_offset,
      second_seg_second_spill.delta_offset);

    // Advance the clean archive offset through each possible position given
    // the current start offset and verify that the truncation point is
    // correct (it should be the same every time).
    size_t prev_segment_size = 0;
    for (const auto& pre_start_seg : expected) {
        if (
          pre_start_seg.base_offset > stm_manifest.get_archive_start_offset()) {
            break;
        }

        stm_manifest.set_archive_clean_offset(
          pre_start_seg.base_offset, prev_segment_size);
        vlog(
          test_log.info,
          "Triggering size based retention, current archive start offset: "
          "{}, current archive clean offset: {}, expected offset: {}",
          stm_manifest.get_archive_start_offset(),
          stm_manifest.get_archive_clean_offset(),
          truncation_point.base_offset);

        auto res = view.compute_retention(size_limit, std::nullopt).get();
        BOOST_REQUIRE(res.has_value());

        if (res.value().offset > truncation_point.base_offset) {
            vlog(
              test_log.error,
              "Retention collected too much data:"
              "truncation offset: {}, expected : {}",
              res.value().offset,
              truncation_point.base_offset);
        }

        BOOST_CHECK_EQUAL(res.value().offset, truncation_point.base_offset);
        BOOST_CHECK_EQUAL(res.value().delta, truncation_point.delta_offset);

        prev_segment_size = pre_start_seg.size_bytes;
    }
}

FIXTURE_TEST(
  test_async_manifest_view_last_offset_term, async_manifest_view_fixture) {
    // Test prologue: set up two spillover manifests and the stm
    // manifest as follows:
    //          spill 1      spill 2       stm
    //       [----------] [----------] [----------]
    //        ^        ^   ^   ^^   ^            ^
    // Term:  1        1   2   23   3            3
    std::vector<segment_meta> expected;
    collect_segments_to(expected);

    auto first_term = generate_random_segments(stm_manifest, 10);
    auto first_term_last_offset = first_term.back().committed_offset;
    add_segments_to_stm_manifest(std::move(first_term));

    auto second_term = generate_random_segments(stm_manifest, 5);
    auto second_term_last_offset = second_term.back().committed_offset;

    for (auto& meta : second_term) {
        meta.segment_term = model::term_id{2};
    }
    add_segments_to_stm_manifest(std::move(second_term));

    auto third_term = generate_random_segments(stm_manifest, 15);
    auto third_term_last_offset = third_term.back().committed_offset;
    for (auto& meta : third_term) {
        meta.segment_term = model::term_id{3};
    }
    add_segments_to_stm_manifest(std::move(third_term));

    trigger_spillover(10);
    trigger_spillover(10);

    // Check that the cloud log layout matches our expectations
    BOOST_REQUIRE_EQUAL(stm_manifest.begin()->segment_term, model::term_id{3});
    BOOST_REQUIRE_EQUAL(stm_manifest.get_spillover_map().size(), 2);
    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_spillover_map().begin()->archiver_term,
      model::term_id{1});
    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_spillover_map().begin()->segment_term,
      model::term_id{1});
    BOOST_REQUIRE_EQUAL(
      std::next(stm_manifest.get_spillover_map().begin())->archiver_term,
      model::term_id{2});
    BOOST_REQUIRE_EQUAL(
      std::next(stm_manifest.get_spillover_map().begin())->segment_term,
      model::term_id{3});

    // Query the last offset for each term (epoch in Kafka speak) and
    // verify against expectations.
    auto first_term_query = view.get_term_last_offset(model::term_id{1}).get();
    BOOST_REQUIRE(first_term_query.has_value());
    BOOST_REQUIRE(first_term_query.value().has_value());
    BOOST_REQUIRE_EQUAL(
      kafka::offset_cast(first_term_query.value().value()),
      first_term_last_offset);

    auto second_term_query = view.get_term_last_offset(model::term_id{2}).get();
    BOOST_REQUIRE(second_term_query.has_value());
    BOOST_REQUIRE(second_term_query.value().has_value());
    BOOST_REQUIRE_EQUAL(
      kafka::offset_cast(second_term_query.value().value()),
      second_term_last_offset);

    auto third_term_query = view.get_term_last_offset(model::term_id{3}).get();
    BOOST_REQUIRE(third_term_query.has_value());
    BOOST_REQUIRE(third_term_query.value().has_value());
    BOOST_REQUIRE_EQUAL(
      kafka::offset_cast(third_term_query.value().value()),
      third_term_last_offset);

    auto future_term_query
      = view.get_term_last_offset(model::term_id{100}).get();
    BOOST_REQUIRE(future_term_query.has_value());
    BOOST_REQUIRE(future_term_query.value() == std::nullopt);

    auto past_term_query = view.get_term_last_offset(model::term_id{100}).get();
    BOOST_REQUIRE(past_term_query.has_value());
    BOOST_REQUIRE(past_term_query.value() == std::nullopt);
}

FIXTURE_TEST(
  test_async_manifest_view_spill_concurrently, async_manifest_view_fixture) {
    // Enumerate all segments while spilling.
    std::vector<segment_meta> expected;
    collect_segments_to(expected);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_random_segments(stm_manifest, 10);
    listen();

    model::offset so = model::offset{0};
    auto maybe_cursor = view.get_cursor(so).get();
    BOOST_REQUIRE(!maybe_cursor.has_failure());

    std::vector<segment_meta> actual;
    auto cursor = std::move(maybe_cursor.value());
    do {
        vlog(
          test_log.info,
          "Manifest before sync [{}/{}], cursor status: {}",
          cursor->manifest()->get_start_offset(),
          cursor->manifest()->get_last_offset(),
          cursor->get_status());

        if (
          cursor->get_status()
          == cloud_storage::async_manifest_view_cursor_status::
            materialized_stm) {
            // Trigger fake spillover, spill manifest with one segment
            spillover_manifest spm(manifest_ntp, manifest_rev);
            for (const auto& meta : stm_manifest) {
                spm.add(meta);
                break;
            }
            spill_manifest(spm, true);
            vlog(
              test_log.info,
              "Spilled new manifest with metadata: {}",
              spm.make_manifest_metadata());
        }

        cursor->maybe_sync_manifest().get();

        vlog(
          test_log.info,
          "Manifest after sync [{}/{}], cursor status: {}",
          cursor->manifest()->get_start_offset(),
          cursor->manifest()->get_last_offset(),
          cursor->get_status());

        for (auto meta : *cursor->manifest()) {
            actual.push_back(meta);
        }

    } while (cursor->next().get().value() != eof::yes);

    print_diff(actual, expected);
    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());
    BOOST_REQUIRE(expected == actual);
}

FIXTURE_TEST(test_async_manifest_view_test_iter, async_manifest_view_fixture) {
    // Enumerate all segments while spilling.
    std::vector<segment_meta> expected;
    collect_segments_to(expected);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_random_segments(stm_manifest, 10);
    listen();

    model::offset so = model::offset{0};
    auto maybe_cursor = view.get_cursor(so).get();
    BOOST_REQUIRE(!maybe_cursor.has_failure());

    std::vector<segment_meta> actual;
    auto cursor = std::move(maybe_cursor.value());
    cloud_storage::for_each_segment(
      std::move(cursor),
      [&actual](const cloud_storage::segment_meta& m) mutable {
          actual.push_back(m);
          return ss::stop_iteration::no;
      })
      .get();

    print_diff(actual, expected);
    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());
    BOOST_REQUIRE(expected == actual);
}

FIXTURE_TEST(test_async_manifest_view_test_iter2, async_manifest_view_fixture) {
    std::vector<segment_meta> expected;
    collect_segments_to(expected);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_random_segments(stm_manifest, 10);
    listen();

    model::offset so = model::offset{0};
    auto maybe_cursor = view.get_cursor(so).get();
    BOOST_REQUIRE(!maybe_cursor.has_failure());

    std::vector<segment_meta> actual;
    auto cursor = std::move(maybe_cursor.value());
    cloud_storage::for_each_manifest(
      std::move(cursor),
      [&actual](ssx::task_local_ptr<const cloud_storage::partition_manifest>
                  p) mutable {
          for (const auto& m : *p) {
              actual.push_back(m);
          }
          return ss::stop_iteration::no;
      })
      .get();

    print_diff(actual, expected);
    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());
    BOOST_REQUIRE(expected == actual);
}

FIXTURE_TEST(test_async_manifest_view_timequery, async_manifest_view_fixture) {
    // Enumerate all segments while spilling.
    std::vector<segment_meta> expected;
    collect_segments_to(expected);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_random_segments(stm_manifest, 10);
    listen();

    // Find exact matches for all segments
    for (const auto& meta : expected) {
        auto target = async_view_timestamp_query(
          kafka::offset(0), meta.base_timestamp, kafka::offset::max());
        auto maybe_cursor = view.get_cursor(target).get();
        BOOST_REQUIRE(!maybe_cursor.has_failure());
        auto cursor = std::move(maybe_cursor.value());
        cursor
          ->with_manifest([&](const partition_manifest& m) {
              vlog(
                test_log.info,
                "looking at the manifest [{}/{}], STM [{}/{}]]",
                m.begin()->base_timestamp,
                m.last_segment()->max_timestamp,
                stm_manifest.begin()->base_timestamp,
                stm_manifest.last_segment()->max_timestamp);
              auto res = m.timequery(target.ts);
              BOOST_REQUIRE(res.has_value());
              BOOST_REQUIRE(res.value().base_timestamp == target.ts);
          })
          .get();
    }
}

FIXTURE_TEST(
  test_async_manifest_view_timequery_with_gaps, async_manifest_view_fixture) {
    // Enumerate all segments while spilling.
    std::vector<segment_meta> expected;
    collect_segments_to(expected);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_random_segments(stm_manifest, 10);
    listen();

    // Our generate_manifest_section() function generates segments with gaps
    // in timestamps. Both base_timestamp and max_timestamp are generated
    // randomly but they're always equal for the same segment. This means
    // that there is a gap between any two segments.

    for (const auto& meta : expected) {
        auto target = async_view_timestamp_query(
          kafka::offset(0),
          model::timestamp(meta.base_timestamp() - 1),
          kafka::offset::max());
        auto maybe_cursor = view.get_cursor(target).get();
        BOOST_REQUIRE(!maybe_cursor.has_failure());
        auto cursor = std::move(maybe_cursor.value());
        cursor
          ->with_manifest([&](const partition_manifest& m) {
              vlog(
                test_log.info,
                "query: {}, manifest: [{}/{}], STM: [{}/{}]]",
                target,
                m.begin()->base_timestamp,
                m.last_segment()->max_timestamp,
                stm_manifest.begin()->base_timestamp,
                stm_manifest.last_segment()->max_timestamp);
              auto res = m.timequery(target.ts);
              BOOST_REQUIRE(res.has_value());
              BOOST_REQUIRE(
                model::timestamp(res.value().base_timestamp.value() - 1)
                == target.ts);
          })
          .get();
    }
}
