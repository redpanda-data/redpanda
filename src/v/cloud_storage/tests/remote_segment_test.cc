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
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/tests/cloud_storage_fixture.h"
#include "cloud_storage/tests/common_def.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "seastarx.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/segment.h"
#include "storage/segment_appender_utils.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/types.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <exception>
#include <stdexcept>

using namespace std::chrono_literals;
using namespace cloud_storage;

inline ss::logger test_log("test"); // NOLINT

static ss::abort_source never_abort;

static const cloud_storage_clients::bucket_name bucket("bucket");

static cloud_storage::lazy_abort_source always_continue([]() {
    return std::nullopt;
});

/**
 * Helper: generate a function suitable for passing to upload_segment(),
 * exposing some synthetic data as a segment_reader_handle.
 */
remote::reset_input_stream make_reset_fn(iobuf& segment_bytes) {
    return [&segment_bytes] {
        auto out = iobuf_deep_copy(segment_bytes);
        return ss::make_ready_future<std::unique_ptr<storage::stream_provider>>(
          std::make_unique<storage::segment_reader_handle>(
            make_iobuf_input_stream(std::move(out))));
    };
}

FIXTURE_TEST(
  test_remote_segment_successful_download, cloud_storage_fixture) { // NOLINT
    auto conf = get_configuration();
    partition_manifest m(manifest_ntp, manifest_revision);
    model::initial_revision_id segment_ntp_revision{777};
    iobuf segment_bytes = generate_segment(model::offset(1), 20);
    uint64_t clen = segment_bytes.size_bytes();
    auto reset_stream = make_reset_fn(segment_bytes);
    retry_chain_node fib(never_abort, 1000ms, 200ms);
    partition_manifest::segment_meta meta{
      .is_compacted = false,
      .size_bytes = segment_bytes.size_bytes(),
      .base_offset = model::offset(1),
      .committed_offset = model::offset(20),
      .base_timestamp = {},
      .max_timestamp = {},
      .delta_offset = model::offset_delta(0),
      .ntp_revision = segment_ntp_revision,
      .sname_format = segment_name_format::v2};
    auto path = m.generate_segment_path(meta);
    set_expectations_and_listen({});
    auto upl_res = api.local()
                     .upload_segment(
                       bucket, path, clen, reset_stream, fib, always_continue)
                     .get();
    BOOST_REQUIRE(upl_res == upload_result::success);
    m.add(meta);

    partition_probe probe{manifest_ntp};
    remote_segment segment(
      api.local(),
      cache.local(),
      bucket,
      m.generate_segment_path(meta),
      m.get_ntp(),
      meta,
      fib,
      probe);

    auto reader_handle
      = segment.data_stream(0, ss::default_priority_class()).get();

    iobuf downloaded;
    auto rds = make_iobuf_ref_output_stream(downloaded);
    ss::copy(reader_handle.stream(), rds).get();
    reader_handle.close().get();

    segment.stop().get();

    BOOST_REQUIRE_EQUAL(downloaded.size_bytes(), segment_bytes.size_bytes());
    BOOST_REQUIRE(downloaded == segment_bytes);
}

FIXTURE_TEST(test_remote_segment_timeout, cloud_storage_fixture) { // NOLINT
    auto conf = get_configuration();
    partition_manifest m(manifest_ntp, manifest_revision);
    auto name = segment_name("7-8-v1.log");
    m.add(
      name,
      partition_manifest::segment_meta{
        .is_compacted = false,
        .size_bytes = 123,
        .base_offset = model::offset(7),
        .committed_offset = model::offset(123),
        .base_timestamp = {},
        .max_timestamp = {},
        .delta_offset = model::offset_delta(0),
        .ntp_revision = manifest_revision});

    retry_chain_node fib(never_abort, 100ms, 20ms);
    auto meta = *m.get(name);
    partition_probe probe{manifest_ntp};
    remote_segment segment(
      api.local(),
      cache.local(),
      bucket,
      m.generate_segment_path(meta),
      m.get_ntp(),
      meta,
      fib,
      probe);

    BOOST_REQUIRE_THROW(
      segment.data_stream(0, ss::default_priority_class()).get(),
      download_exception);
    segment.stop().get();
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
    auto upload_res = f.api.local()
                        .upload_object(
                          bucket,
                          cloud_storage_clients::object_key{
                            path().native() + ".index"},
                          std::move(ixbuf),
                          fib)
                        .get();
    BOOST_REQUIRE(upload_res == upload_result::success);
}

FIXTURE_TEST(
  test_remote_segment_batch_reader_single_batch,
  cloud_storage_fixture) { // NOLINT
    set_expectations_and_listen({});
    auto conf = get_configuration();
    partition_manifest m(manifest_ntp, manifest_revision);
    iobuf segment_bytes = generate_segment(model::offset(1), 100);
    partition_manifest::segment_meta meta{
      .is_compacted = false,
      .size_bytes = segment_bytes.size_bytes(),
      .base_offset = model::offset(1),
      .committed_offset = model::offset(100),
      .base_timestamp = {},
      .max_timestamp = {},
      .delta_offset = model::offset_delta(0),
      .ntp_revision = manifest_revision,
      .sname_format = segment_name_format::v3};
    auto path = m.generate_segment_path(meta);
    uint64_t clen = segment_bytes.size_bytes();
    auto reset_stream = make_reset_fn(segment_bytes);
    retry_chain_node fib(never_abort, 10000ms, 200ms);

    upload_index(*this, meta, segment_bytes, path, fib);

    auto upl_res = api.local()
                     .upload_segment(
                       bucket, path, clen, reset_stream, fib, always_continue)
                     .get();
    BOOST_REQUIRE(upl_res == upload_result::success);
    m.add(meta);

    storage::log_reader_config reader_config(
      model::offset(1), model::offset(1), ss::default_priority_class());

    partition_probe probe(manifest_ntp);
    auto segment = ss::make_lw_shared<remote_segment>(
      api.local(),
      cache.local(),
      bucket,
      m.generate_segment_path(meta),
      m.get_ntp(),
      meta,
      fib,
      probe);

    remote_segment_batch_reader reader(
      segment, reader_config, probe, ssx::semaphore_units());
    storage::offset_translator_state ot_state(m.get_ntp());

    auto s = reader.read_some(model::no_timeout, ot_state).get();
    BOOST_REQUIRE(static_cast<bool>(s));

    std::vector<model::offset> offsets;
    for (const auto& batch : s.value()) {
        // should only recv one batch
        offsets.push_back(batch.base_offset());
    }
    reader.stop().get();
    segment->stop().get();

    BOOST_REQUIRE(offsets.size() == 1);
    BOOST_REQUIRE(offsets.at(0) == model::offset(1));
}

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

void test_remote_segment_batch_reader(
  cloud_storage_fixture& fixture,
  int num_batches,
  int ix_begin,
  int ix_end) { // NOLINT
    iobuf segment_bytes = generate_segment(model::offset(1), num_batches);

    std::vector<model::record_batch_header> headers;
    std::vector<iobuf> records;
    std::vector<uint64_t> file_offsets;
    // account all batches
    auto parser = make_recording_batch_parser(
      iobuf_deep_copy(segment_bytes), headers, records, file_offsets);
    parser->consume().get();
    parser->close().get();
    vlog(test_log.debug, "expected {} headers", headers.size());
    for (const auto& hdr : headers) {
        vlog(test_log.debug, "expected header {}", hdr);
    }

    fixture.set_expectations_and_listen({});
    auto conf = fixture.get_configuration();

    partition_manifest m(manifest_ntp, manifest_revision);
    uint64_t clen = segment_bytes.size_bytes();
    partition_manifest::segment_meta meta{
      .is_compacted = false,
      .size_bytes = segment_bytes.size_bytes(),
      .base_offset = headers.front().base_offset,
      .committed_offset = headers.back().last_offset(),
      .base_timestamp = {},
      .max_timestamp = {},
      .delta_offset = model::offset_delta(0),
      .ntp_revision = manifest_revision,
      .sname_format = segment_name_format::v3};

    auto path = m.generate_segment_path(meta);
    retry_chain_node fib(never_abort, 10000ms, 200ms);

    upload_index(fixture, meta, segment_bytes, path, fib);

    auto reset_stream = make_reset_fn(segment_bytes);
    auto upl_res = fixture.api.local()
                     .upload_segment(
                       bucket, path, clen, reset_stream, fib, always_continue)
                     .get();
    BOOST_REQUIRE(upl_res == upload_result::success);
    m.add(meta);

    // pick offsets for fetch request
    model::offset begin = headers.at(ix_begin).base_offset;
    model::offset end = headers.at(ix_end).last_offset();

    storage::log_reader_config reader_config(
      begin, end, ss::default_priority_class());
    reader_config.max_bytes = std::numeric_limits<size_t>::max();

    partition_probe probe(manifest_ntp);
    auto segment = ss::make_lw_shared<remote_segment>(
      fixture.api.local(),
      fixture.cache.local(),
      bucket,
      m.generate_segment_path(meta),
      m.get_ntp(),
      meta,
      fib,
      probe);

    remote_segment_batch_reader reader(
      segment, reader_config, probe, ssx::semaphore_units());
    storage::offset_translator_state ot_state(m.get_ntp());

    size_t batch_ix = 0;
    bool done = false;
    while (!done) {
        vlog(test_log.debug, "batch_ix {}", batch_ix);
        auto s = reader.read_some(model::no_timeout, ot_state).get();
        BOOST_REQUIRE(static_cast<bool>(s));
        BOOST_REQUIRE(s.value().size() != 0);
        for (const auto& batch : s.value()) {
            vlog(
              test_log.debug,
              "parsing batch {} + {}, value: {}",
              ix_begin,
              batch_ix,
              batch.header());
            BOOST_REQUIRE(headers.at(ix_begin + batch_ix) == batch.header());
            BOOST_REQUIRE(records.at(ix_begin + batch_ix) == batch.data());
            batch_ix++;
            done = batch.header().last_offset() >= end;
        }
    }
    reader.stop().get();
    segment->stop().get();
    BOOST_REQUIRE_EQUAL(batch_ix, (ix_end - ix_begin) + 1 /*inclusive range*/);
}

FIXTURE_TEST(
  test_remote_segment_batch_reader_batches_0_99,
  cloud_storage_fixture) { // NOLINT
    test_remote_segment_batch_reader(*this, 100, 0, 99);
}

FIXTURE_TEST(
  test_remote_segment_batch_reader_batches_0_20,
  cloud_storage_fixture) { // NOLINT
    test_remote_segment_batch_reader(*this, 100, 0, 20);
}

FIXTURE_TEST(
  test_remote_segment_batch_reader_batches_10_20,
  cloud_storage_fixture) { // NOLINT
    test_remote_segment_batch_reader(*this, 100, 10, 20);
}

FIXTURE_TEST(
  test_remote_segment_batch_reader_batches_70_99,
  cloud_storage_fixture) { // NOLINT
    test_remote_segment_batch_reader(*this, 100, 70, 99);
}

// Checks that we can use reader to extract batches
// and when it's done we can reset the config and
// reuse the reader (without closing it first).
FIXTURE_TEST(
  test_remote_segment_batch_reader_repeatable_read,
  cloud_storage_fixture) { // NOLINT
    iobuf segment_bytes = generate_segment(model::offset(1), 100);

    std::vector<model::record_batch_header> headers;
    std::vector<iobuf> records;
    std::vector<uint64_t> file_offsets;
    // account all batches
    auto parser = make_recording_batch_parser(
      iobuf_deep_copy(segment_bytes), headers, records, file_offsets);
    parser->consume().get();
    parser->close().get();
    vlog(test_log.debug, "expected {} headers", headers.size());
    for (const auto& hdr : headers) {
        vlog(test_log.debug, "expected header {}", hdr);
    }

    set_expectations_and_listen({});
    auto conf = get_configuration();

    partition_manifest m(manifest_ntp, manifest_revision);
    uint64_t clen = segment_bytes.size_bytes();
    partition_manifest::segment_meta meta{
      .is_compacted = false,
      .size_bytes = segment_bytes.size_bytes(),
      .base_offset = headers.front().base_offset,
      .committed_offset = headers.back().last_offset(),
      .base_timestamp = {},
      .max_timestamp = {},
      .delta_offset = model::offset_delta(0),
      .ntp_revision = manifest_revision};
    auto path = m.generate_segment_path(meta);
    auto reset_stream = make_reset_fn(segment_bytes);
    retry_chain_node fib(never_abort, 1000ms, 200ms);
    auto upl_res = api.local()
                     .upload_segment(
                       bucket, path, clen, reset_stream, fib, always_continue)
                     .get();
    BOOST_REQUIRE(upl_res == upload_result::success);
    m.add(meta);

    partition_probe probe(manifest_ntp);
    auto segment = ss::make_lw_shared<remote_segment>(
      api.local(),
      cache.local(),
      bucket,
      m.generate_segment_path(meta),
      m.get_ntp(),
      meta,
      fib,
      probe);

    remote_segment_batch_reader reader(
      segment,
      storage::log_reader_config(
        headers.at(0).base_offset,
        headers.at(0).last_offset(),
        ss::default_priority_class()),
      probe,
      ssx::semaphore_units());
    storage::offset_translator_state ot_state(m.get_ntp());

    auto s = reader.read_some(model::no_timeout, ot_state).get();
    BOOST_REQUIRE(static_cast<bool>(s));

    std::vector<model::offset> offsets;
    for (const auto& batch : s.value()) {
        // should only recv one batch
        offsets.push_back(batch.base_offset());
    }

    BOOST_REQUIRE(offsets.size() == 1);
    BOOST_REQUIRE(offsets.at(0) == headers.at(0).base_offset);
    BOOST_REQUIRE(
      reader.config().start_offset
      == headers.at(0).last_offset() + model::offset{1});

    // Update config and retry read
    reader.config().max_offset = headers.at(1).last_offset();
    auto t = reader.read_some(model::no_timeout, ot_state).get();
    for (const auto& batch : t.value()) {
        // should only recv one batch
        offsets.push_back(batch.base_offset());
    }
    BOOST_REQUIRE(offsets.size() == 2);
    BOOST_REQUIRE(offsets.at(1) == headers.at(1).base_offset);

    reader.stop().get();
    segment->stop().get();
}

using upload_index_t = ss::bool_class<struct upload_index_tag>;

static partition_manifest chunk_read_baseline(
  cloud_storage_fixture& f,
  model::offset key,
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

    auto path = m.generate_segment_path(meta);
    f.set_expectations_and_listen({}, {{"Range"}});

    if (index_upload) {
        upload_index(f, meta, segment_bytes, path, fib);
    }

    BOOST_REQUIRE(
      f.api.local()
        .upload_segment(bucket, path, clen, reset_stream, fib, always_continue)
        .get()
      == upload_result::success);
    m.add(meta);

    return m;
}

FIXTURE_TEST(test_remote_segment_chunk_read, cloud_storage_fixture) {
    /**
     * This test creates a segment large enough to be split into multiple
     * chunks, and then creates a data stream which will read through all of
     * them.
     */

    // Use a small chunk size to exercise switching across chunk files during
    // read
    config::shard_local_cfg().cloud_storage_cache_chunk_size.set_value(
      static_cast<uint64_t>(128_KiB));

    auto key = model::offset(1);
    retry_chain_node fib(never_abort, 300s, 200ms);
    iobuf segment_bytes = generate_segment(model::offset(1), 300);

    auto m = chunk_read_baseline(*this, key, fib, segment_bytes.copy());
    auto meta = *m.get(key);
    partition_probe probe(manifest_ntp);
    remote_segment segment(
      api.local(),
      cache.local(),
      bucket,
      m.generate_segment_path(meta),
      m.get_ntp(),
      meta,
      fib,
      probe);

    // The offset data stream uses an implementation which will iterate over all
    // chunks in the segment.
    auto stream = segment
                    .offset_data_stream(
                      m.get(key)->base_kafka_offset(),
                      // using a very large kafka offset makes sure we iterate
                      // over the entire segment in chunks.
                      kafka::offset{100000000},
                      std::nullopt,
                      ss::default_priority_class())
                    .get()
                    .stream;

    iobuf downloaded;
    auto rds = make_iobuf_ref_output_stream(downloaded);
    ss::copy(stream, rds).get();
    stream.close().get();

    BOOST_REQUIRE(!segment.is_fallback_engaged());

    /*
     * when fallback mode is NOT engaged, the estimated min cache cost for the
     * segment should be the size of a single chunk.
     */
    BOOST_REQUIRE(
      segment.min_cache_cost()
      == std::make_pair(
        config::shard_local_cfg().cloud_storage_cache_chunk_size(), true));

    segment.stop().get();

    BOOST_REQUIRE_EQUAL(downloaded.size_bytes(), segment_bytes.size_bytes());
    BOOST_REQUIRE(downloaded == segment_bytes);
}

FIXTURE_TEST(test_remote_segment_chunk_read_fallback, cloud_storage_fixture) {
    /**
     * The index for the segment is not uploaded. This should result in
     * failure to download the index, which will engage fallback mode. The full
     * segment will then be downloaded, and reads will not be done through
     * chunks but through the log segment file.
     */

    auto key = model::offset(1);
    retry_chain_node fib(never_abort, 300s, 200ms);
    iobuf segment_bytes = generate_segment(model::offset(1), 300);

    auto m = chunk_read_baseline(
      *this, key, fib, segment_bytes.copy(), upload_index_t::no);

    auto meta = *m.get(key);
    partition_probe probe(manifest_ntp);
    remote_segment segment(
      api.local(),
      cache.local(),
      bucket,
      m.generate_segment_path(meta),
      m.get_ntp(),
      meta,
      fib,
      probe);

    auto stream = segment
                    .offset_data_stream(
                      m.get(key)->base_kafka_offset(),
                      kafka::offset{100000000},
                      std::nullopt,
                      ss::default_priority_class())
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

    std::regex log_file_expr{".*-.*log(\\.\\d+)?$"};

    for (const auto& req : get_requests()) {
        if (
          req.method != "GET"
          || !std::regex_match(req.url.begin(), req.url.end(), log_file_expr)) {
            continue;
        }

        BOOST_REQUIRE(req.header("Range") == "");
    }

    auto is_chunk_path = [](std::string_view v) {
        return v.find("_chunks") != v.npos;
    };

    bool log_found = false;
    for (const auto& entry : std::filesystem::recursive_directory_iterator{
           tmp_directory.get_path()}) {
        const auto path = entry.path().native();
        BOOST_REQUIRE(!is_chunk_path(path));
        if (std::regex_match(path.begin(), path.end(), log_file_expr)) {
            log_found = true;
        }
    }

    BOOST_REQUIRE(log_found);

    segment.stop().get();

    BOOST_REQUIRE_EQUAL(downloaded.size_bytes(), segment_bytes.size_bytes());
    BOOST_REQUIRE(downloaded == segment_bytes);
}

FIXTURE_TEST(test_chunks_initialization, cloud_storage_fixture) {
    config::shard_local_cfg().cloud_storage_cache_chunk_size.set_value(
      static_cast<uint64_t>(128_KiB));

    auto key = model::offset(1);
    retry_chain_node fib(never_abort, 300s, 200ms);
    iobuf segment_bytes = generate_segment(model::offset(1), 300);

    auto m = chunk_read_baseline(*this, key, fib, segment_bytes.copy());

    auto meta = *m.get(key);
    partition_probe probe(manifest_ntp);
    remote_segment segment(
      api.local(),
      cache.local(),
      bucket,
      m.generate_segment_path(meta),
      m.get_ntp(),
      meta,
      fib,
      probe);

    segment_chunks chunk_api{segment, segment.max_hydrated_chunks()};

    auto close_segment = ss::defer([&segment] { segment.stop().get(); });

    // Download index
    segment.hydrate().get();
    chunk_api.start().get();

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
}

FIXTURE_TEST(test_chunk_hydration, cloud_storage_fixture) {
    config::shard_local_cfg().cloud_storage_cache_chunk_size.set_value(
      static_cast<uint64_t>(128_KiB));

    auto key = model::offset(1);
    retry_chain_node fib(never_abort, 300s, 200ms);
    iobuf segment_bytes = generate_segment(model::offset(1), 300);

    auto m = chunk_read_baseline(*this, key, fib, segment_bytes.copy());

    auto meta = *m.get(key);
    partition_probe probe(manifest_ntp);
    remote_segment segment(
      api.local(),
      cache.local(),
      bucket,
      m.generate_segment_path(meta),
      m.get_ntp(),
      meta,
      fib,
      probe);

    segment_chunks chunk_api{segment, segment.max_hydrated_chunks()};

    auto close_segment = ss::defer([&segment] { segment.stop().get(); });

    segment.hydrate().get();
    chunk_api.start().get();

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

    auto begin_expected = 0;
    auto it = coarse_index.begin();
    auto end_expected = it->second - 1;
    std::regex log_file_expr{".*-.*log(\\.\\d+)?$"};
    for (const auto& req : get_requests()) {
        if (
          req.method != "GET"
          || !std::regex_match(req.url.begin(), req.url.end(), log_file_expr)) {
            continue;
        }

        auto header = req.header("Range");

        // All log GET requests have range header
        BOOST_REQUIRE(header.has_value());
        vlog(
          test_log.info,
          "comparing range {} with expected range {}-{}",
          header.value(),
          begin_expected,
          end_expected);
        auto byte_range = parse_byte_header(header.value());
        BOOST_REQUIRE_EQUAL(begin_expected, byte_range.first);
        BOOST_REQUIRE_EQUAL(end_expected, byte_range.second);

        begin_expected = byte_range.second + 1;
        if (it != coarse_index.end()) {
            BOOST_REQUIRE_EQUAL(it->second, begin_expected);
            it++;
            end_expected = it == coarse_index.end()
                             ? segment_bytes.size_bytes() - 1
                             : it->second - 1;
        }
    }
}

FIXTURE_TEST(test_chunk_future_reader_stats, cloud_storage_fixture) {
    config::shard_local_cfg().cloud_storage_cache_chunk_size.set_value(
      static_cast<uint64_t>(128_KiB));

    auto key = model::offset(1);
    retry_chain_node fib(never_abort, 10s, 200ms);
    iobuf segment_bytes = generate_segment(model::offset(1), 300);

    auto m = chunk_read_baseline(*this, key, fib, segment_bytes.copy());

    auto meta = *m.get(key);
    partition_probe probe(manifest_ntp);
    remote_segment segment(
      api.local(),
      cache.local(),
      bucket,
      m.generate_segment_path(meta),
      m.get_ntp(),
      meta,
      fib,
      probe);

    segment_chunks chunk_api{segment, segment.max_hydrated_chunks()};
    auto close_segment = ss::defer([&segment] { segment.stop().get(); });
    segment.hydrate().get();
    chunk_api.start().get();

    chunk_start_offset_t end = std::prev(chunk_api.end())->first;
    chunk_api.register_readers(0, end);

    auto required_after = 1;
    for (const auto& [_, chunk] : chunk_api) {
        BOOST_REQUIRE_EQUAL(chunk.required_by_readers_in_future, 1);
        BOOST_REQUIRE_EQUAL(chunk.required_after_n_chunks, required_after++);
    }

    for (const auto& [chunk_start, chunk] : chunk_api) {
        BOOST_REQUIRE_EQUAL(chunk.required_by_readers_in_future, 1);
        BOOST_REQUIRE_EQUAL(chunk.required_after_n_chunks, 1);
        chunk_api.mark_acquired_and_update_stats(chunk_start, end);
        BOOST_REQUIRE_EQUAL(chunk.required_by_readers_in_future, 0);
        BOOST_REQUIRE_EQUAL(chunk.required_after_n_chunks, 0);
    }
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

    auto key = model::offset(1);
    retry_chain_node fib(never_abort, 300s, 200ms);
    iobuf segment_bytes = generate_segment(model::offset(1), 300);

    auto m = chunk_read_baseline(*this, key, fib, segment_bytes.copy());
    auto meta = *m.get(key);

    partition_probe probe(manifest_ntp);

    auto segment = ss::make_lw_shared<remote_segment>(
      api.local(),
      cache.local(),
      bucket,
      m.generate_segment_path(meta),
      m.get_ntp(),
      meta,
      fib,
      probe);

    segment_chunks chunk_api{*segment, segment->max_hydrated_chunks()};
    auto close_segment = ss::defer([&segment] { segment->stop().get(); });

    segment->hydrate().get();
    chunk_api.start().get();

    storage::offset_translator_state ot_state(m.get_ntp());

    storage::log_reader_config reader_config(
      model::offset{1}, model::offset{1000000}, ss::default_priority_class());
    reader_config.max_bytes = std::numeric_limits<size_t>::max();

    std::vector<std::unique_ptr<remote_segment_batch_reader>> readers{};
    readers.reserve(10);

    for (auto i = 0; i < 1000; ++i) {
        readers.push_back(std::make_unique<remote_segment_batch_reader>(
          segment, reader_config, probe, ssx::semaphore_units()));
    }

    auto all_readers_done = [&readers] {
        return std::all_of(
          readers.cbegin(), readers.cend(), [](const auto& reader) {
              return reader->is_eof();
          });
    };

    while (!all_readers_done()) {
        std::vector<
          ss::future<result<ss::circular_buffer<model::record_batch>>>>
          reads;
        reads.reserve(readers.size());
        for (auto& reader : readers) {
            reads.push_back(reader->read_some(model::no_timeout, ot_state));
        }

        auto results = ss::when_all_succeed(reads.begin(), reads.end()).get();
        BOOST_REQUIRE(
          std::all_of(results.begin(), results.end(), [](const auto& result) {
              return !result.has_error();
          }));
    }

    for (auto& reader : readers) {
        reader->stop().get();
    }
}

FIXTURE_TEST(test_chunk_prefetch, cloud_storage_fixture) {
    config::shard_local_cfg().cloud_storage_cache_chunk_size.set_value(
      static_cast<uint64_t>(128_KiB));

    const uint16_t prefetch = 1;
    config::shard_local_cfg().cloud_storage_chunk_prefetch.set_value(
      static_cast<uint16_t>(prefetch));

    const auto key = model::offset(1);
    retry_chain_node fib(never_abort, 300s, 200ms);
    const iobuf segment_bytes = generate_segment(model::offset(1), 300);

    const auto m = chunk_read_baseline(*this, key, fib, segment_bytes.copy());
    const auto meta = *m.get(key);
    partition_probe probe(manifest_ntp);
    remote_segment segment(
      api.local(),
      cache.local(),
      bucket,
      m.generate_segment_path(meta),
      m.get_ntp(),
      meta,
      fib,
      probe);

    segment_chunks chunk_api{segment, segment.max_hydrated_chunks()};

    auto close_segment = ss::defer([&segment] { segment.stop().get(); });

    segment.hydrate().get();
    chunk_api.start().get();

    const auto& coarse_index = segment.get_coarse_index();

    // hydrate one chunk, it should trigger prefetch of the next chunk
    chunk_api.hydrate_chunk(0).get();
    const auto& chunk = chunk_api.get(0);
    BOOST_REQUIRE(chunk.current_state == chunk_state::hydrated);
    BOOST_REQUIRE(chunk.handle.has_value());

    auto it = coarse_index.begin();
    using dit = std::filesystem::recursive_directory_iterator;
    for (auto i = 0; i < prefetch; ++i, ++it) {
        const auto& chunk = chunk_api.get(it->second);

        // The prefetched chunk is not hydrated yet
        BOOST_REQUIRE(chunk.current_state == chunk_state::not_available);
        BOOST_REQUIRE(!chunk.handle.has_value());

        // The file is present in cache dir
        auto found = std::find_if(
          dit{tmp_directory.get_path()}, dit{}, [&it](const auto& entry) {
              vlog(test_log.info, "looking at {}", entry.path());
              return entry.path().native().ends_with(
                fmt::format("_chunks/{}", it->second));
          });

        BOOST_REQUIRE(found != dit{});
    }

    const auto& requests_made = get_requests();
    const std::regex log_file_expr{".*-.*log(\\.\\d+)?$"};

    // Assert the byte range is valid
    {
        const auto begin_expected = 0;
        const auto end_expected = it->second - 1;

        const auto is_matching_url = [&log_file_expr](const auto& req) {
            return req.method == "GET"
                   && std::regex_match(
                     req.url.begin(), req.url.end(), log_file_expr);
        };
        const auto segment_get_request = std::find_if(
          requests_made.cbegin(), requests_made.cend(), is_matching_url);
        BOOST_REQUIRE(segment_get_request != requests_made.cend());

        // There is only one request to get the byte range
        BOOST_REQUIRE(
          std::find_if(
            std::next(segment_get_request),
            requests_made.cend(),
            is_matching_url)
          == requests_made.cend());

        const auto header = segment_get_request->header("Range");
        BOOST_REQUIRE(header.has_value());

        // The byte range covers both the original chunk + the prefetch
        const auto [fst, snd] = parse_byte_header(header.value());
        BOOST_REQUIRE_EQUAL(begin_expected, fst);
        BOOST_REQUIRE_EQUAL(end_expected, snd);
    }

    // Assert prefetch hydration does not make any http calls
    {
        const auto next_offset = coarse_index.begin()->second;

        const auto count = std::count_if(
          requests_made.cbegin(),
          requests_made.cend(),
          [&log_file_expr](const auto& req) {
              return req.method == "GET"
                     && std::regex_match(
                       req.url.begin(), req.url.end(), log_file_expr);
          });

        chunk_api.hydrate_chunk(next_offset).get();

        // No calls made to hydrate the prefetched chunk
        BOOST_REQUIRE_EQUAL(
          count,
          std::count_if(
            requests_made.cbegin(),
            requests_made.cend(),
            [&log_file_expr](const auto& req) {
                return req.method == "GET"
                       && std::regex_match(
                         req.url.begin(), req.url.end(), log_file_expr);
            }));

        // Its status is now updated
        const auto& prefetched_chunk = chunk_api.get(next_offset);
        BOOST_REQUIRE(prefetched_chunk.current_state == chunk_state::hydrated);
        BOOST_REQUIRE(prefetched_chunk.handle.has_value());
    }

    // The next chunk after prefetch needs to be downloaded
    {
        const auto next_offset = std::next(coarse_index.begin())->second;

        const auto count = std::count_if(
          requests_made.cbegin(),
          requests_made.cend(),
          [&log_file_expr](const auto& req) {
              return req.method == "GET"
                     && std::regex_match(
                       req.url.begin(), req.url.end(), log_file_expr);
          });

        chunk_api.hydrate_chunk(next_offset).get();

        // A call is made for the offset which was not prefetched
        BOOST_REQUIRE_EQUAL(
          count + 1,
          std::count_if(
            requests_made.cbegin(),
            requests_made.cend(),
            [&log_file_expr](const auto& req) {
                return req.method == "GET"
                       && std::regex_match(
                         req.url.begin(), req.url.end(), log_file_expr);
            }));

        const auto& prefetched_chunk = chunk_api.get(next_offset);
        BOOST_REQUIRE(prefetched_chunk.current_state == chunk_state::hydrated);
        BOOST_REQUIRE(prefetched_chunk.handle.has_value());
    }
}
