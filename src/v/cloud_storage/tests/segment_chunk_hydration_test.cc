/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iostream.h"
#include "cloud_storage/materialized_resources.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/tests/cloud_storage_fixture.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"
#include "utils/lazy_abort_source.h"
#include "utils/stream_provider.h"

#include <seastar/util/defer.hh>

inline ss::logger test_log("test"); // NOLINT
namespace cloud_storage {
remote_path_provider path_provider(std::nullopt, std::nullopt);
class remote_segment_test_helper {
public:
    explicit remote_segment_test_helper(remote_segment& r)
      : _rs{r} {}

    segment_chunks& chunk_api() { return _rs._chunks_api.value(); }

private:
    remote_segment& _rs;
};
} // namespace cloud_storage

using namespace cloud_storage;
using upload_index_t = ss::bool_class<struct upload_index_tag>;

namespace ranges = std::ranges;
namespace views = std::views;

namespace {
ss::abort_source never_abort;
lazy_abort_source always_continue([]() { return std::nullopt; });

remote::reset_input_stream make_reset_fn(const iobuf& segment_bytes) {
    return [&segment_bytes] {
        auto out = iobuf_deep_copy(segment_bytes);
        return ss::make_ready_future<std::unique_ptr<stream_provider>>(
          std::make_unique<storage::segment_reader_handle>(
            make_iobuf_input_stream(std::move(out))));
    };
}

void upload_index(
  cloud_storage_fixture& f,
  const partition_manifest::segment_meta& meta,
  const iobuf& segment_bytes,
  const remote_segment_path& path,
  retry_chain_node& fib) {
    offset_index ix{
      meta.base_offset,
      meta.base_kafka_offset(),
      0,
      remote_segment_sampling_step_bytes,
      meta.base_timestamp};

    auto builder = make_remote_segment_index_builder(
      manifest_ntp,
      make_iobuf_input_stream(segment_bytes.copy()),
      ix,
      meta.delta_offset,
      remote_segment_sampling_step_bytes);

    builder->consume().get();
    builder->close().get();
    auto ixbuf = ix.to_iobuf();
    auto upload_res
      = f.api.local()
          .upload_object({
            .transfer_details
            = {.bucket = cloud_storage_clients::bucket_name{f.bucket_name}, .key = cloud_storage_clients::object_key{path().native() + ".index"}, .parent_rtc = fib},
            .payload = std::move(ixbuf),
          })
          .get();
    BOOST_REQUIRE(upload_res == upload_result::success);
}

partition_manifest chunk_read_baseline(
  cloud_storage_fixture& f,
  retry_chain_node& fib,
  iobuf segment_bytes,
  upload_index_t index_upload = upload_index_t::yes) {
    auto conf = f.get_configuration();
    partition_manifest m(manifest_ntp, manifest_revision);
    model::initial_revision_id segment_ntp_revision{777};
    uint64_t clen = segment_bytes.size_bytes();
    auto reset_stream = make_reset_fn(segment_bytes);

    partition_manifest::segment_meta meta{
      .is_compacted = false,
      .size_bytes = segment_bytes.size_bytes(),
      .base_offset = model::offset(1),
      .committed_offset = model::offset(20),
      .base_timestamp = {},
      .max_timestamp = {},
      .delta_offset = model::offset_delta(0),
      .ntp_revision = segment_ntp_revision,
      .sname_format = segment_name_format::v3};

    auto path = m.generate_segment_path(meta, path_provider);
    f.set_expectations_and_listen({}, {{"Range"}});

    if (index_upload) {
        upload_index(f, meta, segment_bytes, path, fib);
    }

    BOOST_REQUIRE(
      f.api.local()
        .upload_segment(
          f.bucket_name, path, clen, reset_stream, fib, always_continue)
        .get()
      == upload_result::success);
    m.add(meta);

    return m;
}

template<typename Test>
void test_wrapper(
  cloud_storage_fixture& f,
  Test do_test,
  upload_index_t index_upload = upload_index_t::yes) {
    config::shard_local_cfg().cloud_storage_cache_chunk_size.set_value(
      static_cast<uint64_t>(128_KiB));
    auto reset_cfg = ss::defer(
      [] { config::shard_local_cfg().cloud_storage_cache_chunk_size.reset(); });

    const auto key = model::offset(1);
    retry_chain_node fib(never_abort, 300s, 200ms);
    const iobuf segment_bytes = generate_segment(model::offset(1), 300);

    const auto m = chunk_read_baseline(
      f, fib, segment_bytes.copy(), index_upload);
    const auto meta = *m.get(key);
    partition_probe probe(manifest_ntp);
    auto& ts_probe = f.api.local().materialized().get_read_path_probe();
    remote_segment segment(
      f.api.local(),
      f.cache.local(),
      f.bucket_name,
      m.generate_segment_path(meta, path_provider),
      m.get_ntp(),
      meta,
      fib,
      probe,
      ts_probe);

    remote_segment_test_helper t{segment};
    auto& chunk_api = t.chunk_api();
    auto close_segment = ss::defer([&segment] { segment.stop().get(); });
    do_test(segment, chunk_api, segment_bytes);
}

const std::regex log_file_expr{".*-.*log(\\.\\d+)?$"};
const std::regex chunk_file_expr{".*_chunks/\\d+$"};

bool is_segment_dl_req(const http_test_utils::request_info& req) {
    return req.method == "GET"
           && std::regex_match(req.url.begin(), req.url.end(), log_file_expr);
}

bool is_chunk_file(const std::filesystem::directory_entry& e) {
    return std::regex_match(e.path().native(), chunk_file_expr);
}

} // namespace

FIXTURE_TEST(test_remote_segment_chunk_read, cloud_storage_fixture) {
    /**
     * This test creates a segment large enough to be split into multiple
     * chunks, and then creates a data stream which will read through all of
     * them.
     */

    auto test =
      [](remote_segment& segment, segment_chunks&, const iobuf& segment_bytes) {
          ss::abort_source as{};
          // The offset data stream uses an implementation which will iterate
          // over all chunks in the segment.
          auto stream = segment
                          .offset_data_stream(
                            segment.get_base_kafka_offset(),
                            // using a very large kafka offset makes sure we
                            // iterate over the entire segment in chunks.
                            kafka::offset{100000000},
                            std::nullopt,
                            ss::default_priority_class(),
                            as)
                          .get()
                          .stream;

          iobuf downloaded;
          auto rds = make_iobuf_ref_output_stream(downloaded);
          ss::copy(stream, rds).get();
          stream.close().get();

          BOOST_REQUIRE(!segment.is_fallback_engaged());

          /*
           * when fallback mode is NOT engaged, the estimated min cache cost for
           * the segment should be the size of a single chunk.
           */
          BOOST_REQUIRE(
            segment.min_cache_cost()
            == std::make_pair(
              config::shard_local_cfg().cloud_storage_cache_chunk_size(),
              true));

          segment.stop().get();

          BOOST_REQUIRE_EQUAL(
            downloaded.size_bytes(), segment.get_segment_size());
          BOOST_REQUIRE(downloaded == segment_bytes);
      };
    test_wrapper(*this, test);
}

FIXTURE_TEST(test_remote_segment_chunk_read_fallback, cloud_storage_fixture) {
    /**
     * The index for the segment is not uploaded. This should result in
     * failure to download the index, which will engage fallback mode. The full
     * segment will then be downloaded, and reads will not be done through
     * chunks but through the log segment file.
     */

    auto test =
      [&](
        remote_segment& segment, segment_chunks&, const iobuf& segment_bytes) {
          ss::abort_source as;
          auto stream = segment
                          .offset_data_stream(
                            segment.get_base_kafka_offset(),
                            kafka::offset{100000000},
                            std::nullopt,
                            ss::default_priority_class(),
                            as)
                          .get()
                          .stream;

          iobuf downloaded;
          auto rds = make_iobuf_ref_output_stream(downloaded);
          ss::copy(stream, rds).get();

          stream.close().get();

          BOOST_REQUIRE(segment.is_fallback_engaged());

          /*
           * when fallback mode IS engaged, the estimated min cache cost for the
           * segment should be the size of the segment itself.
           */
          BOOST_REQUIRE(
            segment.min_cache_cost()
            == std::make_pair(segment_bytes.size_bytes(), false));

          const auto does_not_have_range_header = [](const auto& req) {
              return req.header("Range") == "";
          };

          BOOST_REQUIRE(ranges::all_of(
            get_requests(is_segment_dl_req), does_not_have_range_header));

          const auto is_chunk_path = [](std::string_view v) {
              return v.find("_chunks") != v.npos;
          };

          const auto is_log_path = [&](const auto& e) {
              const auto path = e.path().native();
              BOOST_REQUIRE(!is_chunk_path(path));
              return std::regex_match(path.begin(), path.end(), log_file_expr);
          };

          BOOST_REQUIRE(ranges::any_of(
            std::filesystem::recursive_directory_iterator{
              tmp_directory.get_path()},
            is_log_path));
          BOOST_REQUIRE(downloaded == segment_bytes);
      };
    test_wrapper(*this, test, upload_index_t::no);
}

FIXTURE_TEST(test_chunks_initialization, cloud_storage_fixture) {
    auto test =
      [](remote_segment& segment, segment_chunks& chunk_api, const iobuf&) {
          segment.hydrate().get();
          const auto& coarse_index = segment.get_coarse_index();
          vlog(test_log.info, "coarse index of {} items", coarse_index.size());

          const auto& first = chunk_api.get(0);
          BOOST_REQUIRE(first.current_state == chunk_state::not_available);
          BOOST_REQUIRE_EQUAL(first.required_after_n_chunks, 0);
          BOOST_REQUIRE_EQUAL(first.required_by_readers_in_future, 0);
          BOOST_REQUIRE(!first.handle.has_value());

          auto last_offset_in_chunks = 0;
          for (auto [kafka_offset, file_offset] : coarse_index) {
              const auto& chunk = chunk_api.get(file_offset);
              BOOST_REQUIRE(chunk.current_state == chunk_state::not_available);
              BOOST_REQUIRE_EQUAL(chunk.required_after_n_chunks, 0);
              BOOST_REQUIRE_EQUAL(chunk.required_by_readers_in_future, 0);
              BOOST_REQUIRE(!chunk.handle.has_value());
              last_offset_in_chunks = file_offset;
          }

          auto current_offset = 0;
          auto it = coarse_index.begin();
          while (current_offset < last_offset_in_chunks) {
              current_offset = chunk_api.get_next_chunk_start(current_offset);
              BOOST_REQUIRE_EQUAL(current_offset, it->second);
              ++it;
          }

          BOOST_REQUIRE_EQUAL(current_offset, last_offset_in_chunks);
      };
    test_wrapper(*this, test);
}

FIXTURE_TEST(test_chunk_hydration, cloud_storage_fixture) {
    auto test = [&](
                  remote_segment& segment,
                  segment_chunks& chunk_api,
                  const iobuf& segment_bytes) {
        segment.hydrate().get();
        const auto& coarse_index = segment.get_coarse_index();

        chunk_api.hydrate_chunk(0).get();
        const auto& chunk = chunk_api.get(0);
        BOOST_REQUIRE(chunk.current_state == chunk_state::hydrated);
        BOOST_REQUIRE(chunk.handle.has_value());

        for (auto [kafka_offset, file_offset] : coarse_index) {
            auto handle = chunk_api.hydrate_chunk(file_offset).get();
            // The file handle is open
            BOOST_REQUIRE(*handle);

            const auto& chunk = chunk_api.get(file_offset);
            BOOST_REQUIRE(chunk.current_state == chunk_state::hydrated);
            BOOST_REQUIRE(chunk.handle.has_value());
        }

        size_t begin_expected = 0;
        for (const auto& req : get_requests(is_segment_dl_req)) {
            auto header = req.header("Range");
            BOOST_REQUIRE(header.has_value());
            auto byte_range = parse_byte_header(header.value());
            BOOST_REQUIRE(
              chunk_api.get_byte_range_for_chunk(
                begin_expected, segment_bytes.size_bytes() - 1)
              == byte_range);
            begin_expected = byte_range.second + 1;
        }
    };
    test_wrapper(*this, test);
}

FIXTURE_TEST(test_chunk_future_reader_stats, cloud_storage_fixture) {
    auto test =
      [](remote_segment& segment, segment_chunks& chunk_api, const iobuf&) {
          segment.hydrate().get();
          chunk_start_offset_t end = std::prev(chunk_api.end())->first;
          chunk_api.register_readers(0, end);

          auto required_after = 1;
          for (const auto& [_, chunk] : chunk_api) {
              BOOST_REQUIRE_EQUAL(chunk.required_by_readers_in_future, 1);
              BOOST_REQUIRE_EQUAL(
                chunk.required_after_n_chunks, required_after++);
          }

          for (const auto& [chunk_start, chunk] : chunk_api) {
              BOOST_REQUIRE_EQUAL(chunk.required_by_readers_in_future, 1);
              BOOST_REQUIRE_EQUAL(chunk.required_after_n_chunks, 1);
              chunk_api.mark_acquired_and_update_stats(chunk_start, end);
              BOOST_REQUIRE_EQUAL(chunk.required_by_readers_in_future, 0);
              BOOST_REQUIRE_EQUAL(chunk.required_after_n_chunks, 0);
          }
      };
    test_wrapper(*this, test);
}

FIXTURE_TEST(test_chunk_multiple_readers, cloud_storage_fixture) {
    /**
     * This test exercises using many readers against a remote segment while
     * using chunks. The idea is to exercise the waitlist per chunk but there
     * are no deterministic assertions for this in the test, we simply wait for
     * all reads to finish for all readers.
     */
    config::shard_local_cfg().cloud_storage_cache_chunk_size.set_value(
      static_cast<uint64_t>(128_KiB));
    auto reset_cfg = ss::defer(
      [] { config::shard_local_cfg().cloud_storage_cache_chunk_size.reset(); });

    auto key = model::offset(1);
    retry_chain_node fib(never_abort, 300s, 200ms);
    iobuf segment_bytes = generate_segment(model::offset(1), 300);

    auto m = chunk_read_baseline(*this, fib, segment_bytes.copy());
    auto meta = *m.get(key);

    partition_probe probe(manifest_ntp);
    auto& ts_probe = api.local().materialized().get_read_path_probe();

    auto segment = ss::make_lw_shared<remote_segment>(
      api.local(),
      cache.local(),
      bucket_name,
      m.generate_segment_path(meta, path_provider),
      m.get_ntp(),
      meta,
      fib,
      probe,
      ts_probe);

    auto close_segment = ss::defer([&segment] { segment->stop().get(); });
    remote_segment_test_helper t{*segment};
    auto& chunk_api = t.chunk_api();

    segment->hydrate().get();
    chunk_api.start().get();

    storage::offset_translator_state ot_state(m.get_ntp());

    storage::log_reader_config reader_config(
      model::offset{1}, model::offset{1000000}, ss::default_priority_class());
    reader_config.max_bytes = std::numeric_limits<size_t>::max();

    std::vector<std::unique_ptr<remote_segment_batch_reader>> readers{};
    for (auto i = 0; i < 1000; ++i) {
        readers.push_back(std::make_unique<remote_segment_batch_reader>(
          segment, reader_config, probe, ts_probe, ssx::semaphore_units()));
    }

    auto all_readers_done = [&readers] {
        return ranges::all_of(
          readers, [](const auto& reader) { return reader->is_eof(); });
    };

    while (!all_readers_done()) {
        std::vector<
          ss::future<result<ss::circular_buffer<model::record_batch>>>>
          reads;
        reads.reserve(readers.size());
        ranges::transform(
          readers, std::back_inserter(reads), [&](auto& reader) {
              return reader->read_some(model::no_timeout, ot_state);
          });

        auto results = ss::when_all_succeed(reads.begin(), reads.end()).get();
        BOOST_REQUIRE(ranges::all_of(
          results, [](const auto& result) { return !result.has_error(); }));
    }

    for (const auto& reader : readers) {
        reader->stop().get();
    }
}

FIXTURE_TEST(test_chunk_prefetch, cloud_storage_fixture) {
    const uint16_t prefetch = 3;
    scoped_config reset;
    reset.get("cloud_storage_chunk_prefetch").set_value(uint16_t{prefetch});

    auto test_f = [&](remote_segment& r, segment_chunks& c, const iobuf&) {
        r.hydrate().get();
        c.hydrate_chunk(0).get();

        RPTEST_REQUIRE_EVENTUALLY(
          10s, [&c] { return !c.downloads_in_progress(); })

        const auto& requests_made = get_requests(is_segment_dl_req);

        auto i = 0;
        for (const auto& [start, chunk] : c) {
            BOOST_REQUIRE(chunk.current_state == chunk_state::hydrated);
            BOOST_REQUIRE(chunk.handle.has_value());

            const auto is_path_to_chunk = [&](const auto& entry) {
                return entry.path().native().ends_with(
                  fmt::format("_chunks/{}", start));
            };

            BOOST_REQUIRE(ranges::any_of(
              std::filesystem::recursive_directory_iterator{
                tmp_directory.get_path()},
              is_path_to_chunk));

            const auto does_match_byte_range = [&](const auto& req) {
                const auto header = req.header("Range");
                BOOST_REQUIRE(header.has_value());
                return c.get_byte_range_for_chunk(
                         start, r.get_segment_size() - 1)
                       == parse_byte_header(header.value());
            };

            BOOST_REQUIRE(ranges::any_of(requests_made, does_match_byte_range));
            if (++i > prefetch) {
                break;
            }
        }
    };

    test_wrapper(*this, test_f);
}

FIXTURE_TEST(test_abort_hydration_timeout, cloud_storage_fixture) {
    scoped_config reset;
    reset.get("cloud_storage_hydration_timeout_ms").set_value(0ms);

    auto test = [](remote_segment& segment, segment_chunks&, const iobuf&) {
        ss::abort_source as;
        BOOST_REQUIRE_THROW(
          segment
            .offset_data_stream(
              segment.get_base_kafka_offset(),
              kafka::offset{100000000},
              std::nullopt,
              ss::default_priority_class(),
              as)
            .get(),
          ss::timed_out_error);
    };
    test_wrapper(*this, test);
}

FIXTURE_TEST(test_abort_hydration_triggered_externally, cloud_storage_fixture) {
    auto test = [](remote_segment& segment, segment_chunks&, const iobuf&) {
        ss::abort_source as;
        as.request_abort();
        BOOST_REQUIRE_THROW(
          segment
            .offset_data_stream(
              segment.get_base_kafka_offset(),
              kafka::offset{100000000},
              std::nullopt,
              ss::default_priority_class(),
              as)
            .get(),
          ss::abort_requested_exception);
    };
    test_wrapper(*this, test);
}

FIXTURE_TEST(test_chunk_mixed_dl_results, cloud_storage_fixture) {
    // This test simulates a situation where multiple chunk downloads are queued
    // up, some fail and other succeed. The loop in chunk API should correctly
    // handle a mixed set of results.

    // The first chunk download is marked to fail
    const auto is_request_for_first_chunk = [](const auto& r) {
        return is_segment_dl_req(r)
               && parse_byte_header(r.header("Range").value()).first == 0;
    };

    fail_request_if(
      is_request_for_first_chunk,
      {.status = ss::http::reply::status_type::bad_request});

    auto test_f = [&](remote_segment& r, segment_chunks& c, const iobuf&) {
        r.hydrate().get();

        std::vector<chunk_start_offset_t> starts{
          0,
          c.get_next_chunk_start(0),
          c.get_next_chunk_start(c.get_next_chunk_start(0))};

        std::vector<ss::future<segment_chunk::handle_t>> gets;
        gets.reserve(starts.size());

        for (auto start : starts) {
            gets.emplace_back(c.hydrate_chunk(start));
        }

        auto results = ss::when_all(gets.begin(), gets.end()).get();

        RPTEST_REQUIRE_EVENTUALLY(
          10s, [&c] { return !c.downloads_in_progress(); })

        const auto& requests_made = get_requests(is_segment_dl_req);

        for (size_t i = 0; i < starts.size(); ++i) {
            auto start = starts[i];
            auto result = std::move(results[i]);

            const auto does_match_byte_range = [&](const auto& req) {
                const auto header = req.header("Range");
                BOOST_REQUIRE(header.has_value());
                return c.get_byte_range_for_chunk(
                         start, r.get_segment_size() - 1)
                       == parse_byte_header(header.value());
            };

            BOOST_REQUIRE(ranges::any_of(requests_made, does_match_byte_range));

            const auto is_path_to_chunk = [&](const auto& entry) {
                return entry.path().native().ends_with(
                  fmt::format("_chunks/{}", start));
            };

            const auto& chunk = c.get(start);
            auto dit = std::filesystem::recursive_directory_iterator{
              tmp_directory.get_path()};

            if (start != 0) {
                BOOST_REQUIRE(chunk.current_state == chunk_state::hydrated);
                BOOST_REQUIRE(chunk.handle.has_value());
                BOOST_REQUIRE(ranges::any_of(dit, is_path_to_chunk));
                auto res = result.get();
                BOOST_REQUIRE(*res);
                auto p = ss::make_lw_shared(ss::file{});
                BOOST_REQUIRE(p);
            } else {
                // chunk 0 is marked to fail download
                BOOST_REQUIRE(
                  chunk.current_state == chunk_state::not_available);
                BOOST_REQUIRE(!chunk.handle.has_value());
                BOOST_REQUIRE(ranges::none_of(dit, is_path_to_chunk));
                auto err = result.get_exception();
                BOOST_REQUIRE(err);
            }
        }
    };

    test_wrapper(*this, test_f);
}

namespace {

std::vector<chunk_start_offset_t>
pick_random_chunk_offsets(segment_chunks& c, size_t count = 1000) {
    std::vector<chunk_start_offset_t> result;
    result.reserve(count);

    int to_fill = count;
    std::vector<chunk_start_offset_t> offsets;
    for (auto start : std::views::keys(c)) {
        offsets.push_back(start);
    }
    while (to_fill > 0) {
        auto pos = random_generators::randomized_range(
          size_t{0}, offsets.size());
        ranges::transform(
          pos, std::back_inserter(result), [&](auto p) { return offsets[p]; });
        to_fill -= offsets.size();
    }

    result.resize(count);
    return result;
}

ss::future<> delete_random_chunks(
  cloud_storage_fixture& f, ss::abort_source& as, ss::gate& gate) {
    using dit = std::filesystem::recursive_directory_iterator;
    auto g = gate.hold();
    while (!as.abort_requested()) {
        for (auto it = dit{f.tmp_directory.get_path()}; it != dit{}; ++it) {
            if (is_chunk_file(*it)) {
                if (random_generators::get_int(100) > 20) {
                    vlog(test_log.info, "removing {}", it->path().native());
                    co_await ss::remove_file(it->path().native());
                }
            }
        }
        co_await ss::sleep(1ms);
    }
}

} // namespace

FIXTURE_TEST(test_chunk_stop_while_downloading, cloud_storage_fixture) {
    scoped_config reset;
    reset.get("cloud_storage_chunk_prefetch").set_value(uint16_t{3});

    // Simulates an abrupt stop while downloading, to make sure no part of the
    // system hangs or fails if downloads are in progress and the segment is
    // stopped

    auto test_f = [&](remote_segment& r, segment_chunks& c, const iobuf&) {
        r.hydrate().get();

        size_t n_test_size = 10000;
        std::vector<ss::future<segment_chunk::handle_t>> gets;
        gets.reserve(n_test_size);
        for (auto ro : pick_random_chunk_offsets(c, n_test_size)) {
            gets.emplace_back(c.hydrate_chunk(ro));
        }

        r.stop().get();
        BOOST_REQUIRE(!c.downloads_in_progress());

        auto failed_futures = ss::when_all(gets.begin(), gets.end()).get()
                              | views::filter(
                                [](auto& f) { return f.failed(); });
        ranges::for_each(failed_futures, [](auto& r) { r.get_exception(); });
    };

    test_wrapper(*this, test_f);
}

FIXTURE_TEST(test_chunk_dl_while_removing_files, cloud_storage_fixture) {
    // This test exercises both waiter semantics, because each chunk is
    // requested multiple times, and materialization failure handling on file
    // delete. The background task deletes random chunk files, so remote_segment
    // has to re-hydrate deleted chunks.
    auto test_f = [&](remote_segment& r, segment_chunks& c, const iobuf&) {
        r.hydrate().get();

        size_t n_test_size = 10000;
        std::vector<ss::future<segment_chunk::handle_t>> gets;
        gets.reserve(n_test_size);
        for (auto ro : pick_random_chunk_offsets(c, n_test_size)) {
            gets.emplace_back(c.hydrate_chunk(ro));
        }

        // Timebox the test so that we don't end up waiting on the gets
        // forever
        RPTEST_REQUIRE_EVENTUALLY(
          60s, [&c] { return !c.downloads_in_progress(); })

        auto results = ss::when_all(gets.begin(), gets.end()).get();
        for (auto& r : results) {
            BOOST_REQUIRE(!r.failed());
            auto handle = r.get();
            BOOST_REQUIRE(*handle);
        }
    };

    ss::abort_source as;
    ss::gate g;
    ssx::background = delete_random_chunks(*this, as, g);
    test_wrapper(*this, test_f);
    as.request_abort();
    g.close().get();
}

FIXTURE_TEST(test_chunk_dl_with_random_http_errors, cloud_storage_fixture) {
    // Inject random http errors, a large number of retryable ones and a small
    // number of non retryable ones. The majority of requests should succeed
    // eventually, although the limited time available means some retryable
    // requests will also fail.
    auto test_f = [&](remote_segment& r, segment_chunks& c, const iobuf&) {
        r.hydrate().get();

        size_t n_test_size = 10000;
        std::vector<ss::future<segment_chunk::handle_t>> gets;
        gets.reserve(n_test_size);
        for (auto ro : pick_random_chunk_offsets(c, n_test_size)) {
            gets.emplace_back(c.hydrate_chunk(ro));
        }

        // Deferred so that if the test fails due to timeout the logs are not
        // polluted with ignored exception errors
        auto resolve_futures = ss::defer([&]() {
            auto results = ss::when_all(gets.begin(), gets.end()).get();
            int failed = 0;
            for (auto& r : results) {
                if (r.failed()) {
                    auto ex = r.get_exception();
                    BOOST_REQUIRE(ex);
                    failed += 1;
                } else {
                    auto f = r.get();
                    BOOST_REQUIRE(*f);
                }
            }

            vlog(
              test_log.info,
              "total failed requests: {}/{}",
              failed,
              n_test_size);
        });

        RPTEST_REQUIRE_EVENTUALLY(
          120s, [&c] { return !c.downloads_in_progress(); })
    };

    int injected = 0;
    // Non retryable errors
    fail_request_if(
      [&injected](const auto& r) {
          if (!is_segment_dl_req(r)) {
              return false;
          }
          auto fail = random_generators::get_int(100) > 80;
          if (fail) {
              injected += 1;
          }
          return fail;
      },
      {.status = ss::http::reply::status_type::not_found});

    // Retryable errors
    fail_request_if(
      [&injected](const auto& r) {
          if (!is_segment_dl_req(r)) {
              return false;
          }
          auto fail = random_generators::get_int(100) > 20;
          if (fail) {
              injected += 1;
          }
          return fail;
      },
      {.status = ss::http::reply::status_type::internal_server_error});

    test_wrapper(*this, test_f);
    vlog(
      test_log.info,
      "total failures injected {}, total requests made {}",
      injected,
      get_requests(is_segment_dl_req).size());
}

FIXTURE_TEST(
  test_chunk_prefetch_with_overlapping_waiters, cloud_storage_fixture) {
    // A chunk prefetch schedules download for that chunk. There may be a
    // request for the same chunk later on via a consumer. This test checks that
    // such requests do not result in chunk FDs being leaked.
    scoped_config reset;
    reset.get("cloud_storage_chunk_prefetch").set_value(uint16_t{3});

    auto test_f = [&](remote_segment& r, segment_chunks& c, const iobuf&) {
        r.hydrate().get();

        size_t n_test_size = 10000;
        std::vector<ss::future<segment_chunk::handle_t>> gets;
        gets.reserve(n_test_size);
        for (auto ro : pick_random_chunk_offsets(c, n_test_size)) {
            gets.emplace_back(c.hydrate_chunk(ro));
        }

        RPTEST_REQUIRE_EVENTUALLY(
          120s, [&c] { return !c.downloads_in_progress(); })

        auto results = ss::when_all(gets.begin(), gets.end()).get();
        BOOST_REQUIRE(ranges::all_of(results, [](auto& res) {
            auto h = res.get();
            return bool{*h};
        }));
    };

    test_wrapper(*this, test_f);
}
