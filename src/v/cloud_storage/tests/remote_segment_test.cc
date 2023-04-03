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
    auto bucket = cloud_storage_clients::bucket_name("bucket");
    partition_manifest m(manifest_ntp, manifest_revision);
    auto key = model::offset(1);
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
      .sname_format = segment_name_format::v3};
    auto path = m.generate_segment_path(meta);
    offset_index ix{model::offset{0}, kafka::offset{0}, 0, 120};
    iobuf_parser p{ix.to_iobuf()};
    auto body = p.read_bytes(p.bytes_left());
    ss::sstring s{body.begin(), body.end()};
    auto index_path = "/" + path().native() + ".index";
    set_expectations_and_listen({{.url = index_path, .body = std::move(s)}});
    auto upl_res = api.local()
                     .upload_segment(
                       bucket, path, clen, reset_stream, fib, always_continue)
                     .get();
    BOOST_REQUIRE(upl_res == upload_result::success);
    m.add(key, meta);

    remote_segment segment(api.local(), cache.local(), bucket, m, key, fib);
    auto reader_handle
      = segment.data_stream(0, ss::default_priority_class()).get();

    iobuf downloaded;
    auto rds = make_iobuf_ref_output_stream(downloaded);
    ss::copy(reader_handle.stream(), rds).get();
    reader_handle.close().get();

    segment.stop().get();

    BOOST_REQUIRE_EQUAL(downloaded.size_bytes(), segment_bytes.size_bytes());
    BOOST_REQUIRE(downloaded == segment_bytes);

    BOOST_REQUIRE(get_targets().contains(index_path));
    const auto& req = get_targets().find(index_path);
    BOOST_REQUIRE_EQUAL(req->second.method, "GET");
}

FIXTURE_TEST(test_remote_segment_timeout, cloud_storage_fixture) { // NOLINT
    auto conf = get_configuration();
    auto bucket = cloud_storage_clients::bucket_name("bucket");
    partition_manifest m(manifest_ntp, manifest_revision);
    auto name = segment_name("7-8-v1.log");
    auto key = parse_segment_name(name).value();
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
    remote_segment segment(
      api.local(), cache.local(), bucket, m, key.base_offset, fib);
    BOOST_REQUIRE_THROW(
      segment.data_stream(0, ss::default_priority_class()).get(),
      download_exception);
    segment.stop().get();
}

FIXTURE_TEST(
  test_remote_segment_batch_reader_single_batch,
  cloud_storage_fixture) { // NOLINT
    set_expectations_and_listen({});
    auto conf = get_configuration();
    auto bucket = cloud_storage_clients::bucket_name("bucket");
    partition_manifest m(manifest_ntp, manifest_revision);
    auto key = model::offset(1);
    iobuf segment_bytes = generate_segment(model::offset(1), 100);
    partition_manifest::segment_meta meta{
      .is_compacted = false,
      .size_bytes = segment_bytes.size_bytes(),
      .base_offset = model::offset(1),
      .committed_offset = model::offset(100),
      .base_timestamp = {},
      .max_timestamp = {},
      .delta_offset = model::offset_delta(0),
      .ntp_revision = manifest_revision};
    auto path = m.generate_segment_path(meta);
    uint64_t clen = segment_bytes.size_bytes();
    auto reset_stream = make_reset_fn(segment_bytes);
    retry_chain_node fib(never_abort, 1000ms, 200ms);
    auto upl_res = api.local()
                     .upload_segment(
                       bucket, path, clen, reset_stream, fib, always_continue)
                     .get();
    BOOST_REQUIRE(upl_res == upload_result::success);
    m.add(key, meta);

    storage::log_reader_config reader_config(
      model::offset(1), model::offset(1), ss::default_priority_class());
    auto segment = ss::make_lw_shared<remote_segment>(
      api.local(), cache.local(), bucket, m, key, fib);
    partition_probe probe(manifest_ntp);
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
    auto bucket = cloud_storage_clients::bucket_name("bucket");

    partition_manifest m(manifest_ntp, manifest_revision);
    auto key = model::offset(1);
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
    auto upl_res = fixture.api.local()
                     .upload_segment(
                       bucket, path, clen, reset_stream, fib, always_continue)
                     .get();
    BOOST_REQUIRE(upl_res == upload_result::success);
    m.add(key, meta);

    // pick offsets for fetch request
    model::offset begin = headers.at(ix_begin).base_offset;
    model::offset end = headers.at(ix_end).last_offset();

    storage::log_reader_config reader_config(
      begin, end, ss::default_priority_class());
    reader_config.max_bytes = std::numeric_limits<size_t>::max();
    auto segment = ss::make_lw_shared<remote_segment>(
      fixture.api.local(), fixture.cache.local(), bucket, m, key, fib);
    partition_probe probe(manifest_ntp);
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
    auto bucket = cloud_storage_clients::bucket_name("bucket");

    partition_manifest m(manifest_ntp, manifest_revision);
    auto key = model::offset(1);
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
    m.add(key, meta);

    auto segment = ss::make_lw_shared<remote_segment>(
      api.local(), cache.local(), bucket, m, key, fib);

    partition_probe probe(manifest_ntp);
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
