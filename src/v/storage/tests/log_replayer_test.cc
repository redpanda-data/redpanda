// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/record_utils.h"
#include "model/tests/random_batch.h"
#include "random/generators.h"
#include "seastarx.h"
#include "storage/disk_log_appender.h"
#include "storage/log_replayer.h"
#include "storage/logger.h"
#include "storage/segment.h"
#include "storage/segment_appender_utils.h"
#include "storage/segment_index.h"
#include "storage/segment_reader.h"
#include "utils/file_sanitizer.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <memory>

using namespace storage; // NOLINT

struct context {
    ss::lw_shared_ptr<segment> _seg;
    std::optional<log_replayer> replayer_opt;
    ss::sstring base_name = "test."
                            + random_generators::gen_alphanum_string(20);

    void initialize(model::offset base) {
        auto fd = ss::open_file_dma(
                    base_name, ss::open_flags::create | ss::open_flags::rw)
                    .get0();
        auto fidx = ss::open_file_dma(
                      base_name + ".index",
                      ss::open_flags::create | ss::open_flags::rw)
                      .get0();
        fd = ss::file(ss::make_shared(file_io_sanitizer(std::move(fd))));
        fidx = ss::file(ss::make_shared(file_io_sanitizer(std::move(fidx))));

        auto appender = std::make_unique<segment_appender>(
          fd,
          segment_appender::options(
            ss::default_priority_class(), 1, config::mock_binding(16_KiB)));
        auto indexer = segment_index(
          base_name + ".index", std::move(fidx), base, 4096);
        auto reader = segment_reader(
          base_name, 128_KiB, 10, debug_sanitize_files::no);
        reader.load_size().get();
        _seg = ss::make_lw_shared<segment>(
          segment::offset_tracker(model::term_id(0), base),
          std::move(reader),
          std::move(indexer),
          std::move(appender),
          std::nullopt,
          std::nullopt);
        replayer_opt = log_replayer(*_seg);
    }

    ~context() { _seg->close().get(); }

    void write_garbage() { do_write_garbage(base_name); }

    void write_garbage_index() { do_write_garbage(base_name + ".index"); }

    void do_write_garbage(ss::sstring name) {
        auto fd = ss::open_file_dma(
                    name, ss::open_flags::create | ss::open_flags::rw)
                    .get0();
        fd = ss::file(ss::make_shared(file_io_sanitizer(std::move(fd))));
        auto out = ss::make_file_output_stream(std::move(fd)).get0();
        const auto b = random_generators::gen_alphanum_string(100);
        out.write(b.data(), b.size()).get();
        out.flush().get();
        out.close().get();
    }
    void write(ss::circular_buffer<model::record_batch>& batches) {
        do_write(
          [&batches](segment_appender& appender) {
              for (auto& b : batches) {
                  b.header().header_crc = model::internal_header_only_crc(
                    b.header());
                  storage::write(appender, b).get();
              }
          },
          batches.begin()->base_offset());
    }

    template<typename Writer>
    void do_write(Writer&& w, model::offset base) {
        initialize(base);
        w(_seg->appender());
        _seg->flush().get();
        _seg->reader().set_file_size(_seg->appender().file_byte_offset());
    }

    log_replayer& replayer() { return *replayer_opt; }
};

SEASTAR_THREAD_TEST_CASE(test_can_recover_single_batch) {
    context ctx;
    auto batches = model::test::make_random_batches(model::offset(1), 1);
    auto last_offset = batches.back().last_offset();
    ctx.write(batches);
    storage::log_replayer::checkpoint recovered
      = ctx.replayer().recover_in_thread(ss::default_priority_class());
    BOOST_REQUIRE(bool(recovered));
    BOOST_CHECK_EQUAL(recovered.last_offset.value(), last_offset);
}

SEASTAR_THREAD_TEST_CASE(test_unrecovered_single_batch) {
    {
        context ctx;
        auto batches = model::test::make_random_batches(model::offset(1), 1);
        batches.back().header().crc = 10;
        ctx.write(batches);
        auto recovered = ctx.replayer().recover_in_thread(
          ss::default_priority_class());
        BOOST_CHECK(!bool(recovered));
    }
    {
        context ctx;
        auto batches = model::test::make_random_batches(model::offset(1), 1);
        batches.back().header().first_timestamp = model::timestamp(10);
        ctx.write(batches);
        auto recovered = ctx.replayer().recover_in_thread(
          ss::default_priority_class());
        BOOST_CHECK(!bool(recovered));
    }
}

SEASTAR_THREAD_TEST_CASE(test_malformed_segment) {
    context ctx;
    ctx.write_garbage();
    ctx.initialize(model::offset(0));
    auto recovered = ctx.replayer().recover_in_thread(
      ss::default_priority_class());
    BOOST_CHECK(!bool(recovered));
}

SEASTAR_THREAD_TEST_CASE(test_can_recover_multiple_batches) {
    context ctx;
    auto batches = model::test::make_random_batches(model::offset(1), 10);
    auto last_offset = batches.back().last_offset();
    ctx.write(batches);
    auto recovered = ctx.replayer().recover_in_thread(
      ss::default_priority_class());
    BOOST_CHECK(bool(recovered));
    BOOST_CHECK_EQUAL(recovered.last_offset.value(), last_offset);
}

SEASTAR_THREAD_TEST_CASE(test_unrecovered_multiple_batches) {
    {
        // bad crc test
        context ctx;
        auto batches = model::test::make_random_batches(model::offset(1), 10);
        batches.back().header().crc = 10;
        auto last_offset = (batches.end() - 2)->last_offset();
        ctx.write(batches);
        auto recovered = ctx.replayer().recover_in_thread(
          ss::default_priority_class());
        BOOST_CHECK(bool(recovered));
        BOOST_CHECK_EQUAL(recovered.last_offset.value(), last_offset);
    }
    {
        // timestamp test
        context ctx;
        auto batches = model::test::make_random_batches(model::offset(1), 10);
        batches.back().header().first_timestamp = model::timestamp(10);
        auto last_offset = (batches.end() - 2)->last_offset();
        ctx.write(batches);
        auto recovered = ctx.replayer().recover_in_thread(
          ss::default_priority_class());
        BOOST_CHECK(bool(recovered));
        BOOST_CHECK_EQUAL(recovered.last_offset.value(), last_offset);
    }
}
SEASTAR_THREAD_TEST_CASE(test_reset_index) {
    // bad crc test
    context ctx;
    ctx.write_garbage_index(); // key
    auto batches = model::test::make_random_batches(model::offset(1), 10);
    auto last_offset = batches.back().last_offset();
    ctx.write(batches);
    auto recovered = ctx.replayer().recover_in_thread(
      ss::default_priority_class());
    BOOST_CHECK(bool(recovered));
    BOOST_CHECK_EQUAL(recovered.last_offset.value(), last_offset);
    storage::stlog.info("Recovered segment:{}", ctx._seg);
    BOOST_CHECK(ctx._seg->index().needs_persistence());
}
