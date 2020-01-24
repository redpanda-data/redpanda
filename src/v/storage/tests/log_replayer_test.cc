#include "random/generators.h"
#include "storage/disk_log_appender.h"
#include "storage/log_replayer.h"
#include "storage/log_segment_appender_utils.h"
#include "storage/log_segment_reader.h"
#include "storage/segment_offset_index.h"
#include "storage/tests/random_batch.h"
#include "utils/file_sanitizer.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <memory>

using namespace storage; // NOLINT

struct context {
    std::unique_ptr<segment> _seg;
    std::optional<log_replayer> replayer_opt;
    ss::sstring base_name = "test."
                            + random_generators::gen_alphanum_string(20);

    void initialize(model::offset base) {
        auto fd = ss::open_file_dma(
                    base_name, ss::open_flags::create | ss::open_flags::rw)
                    .get0();
        auto fidx = ss::open_file_dma(
                      base_name + ".offset_index",
                      ss::open_flags::create | ss::open_flags::rw)
                      .get0();
        fd = ss::file(ss::make_shared(file_io_sanitizer(std::move(fd))));
        fidx = ss::file(ss::make_shared(file_io_sanitizer(std::move(fidx))));

        auto appender = std::make_unique<log_segment_appender>(
          fd, log_segment_appender::options(ss::default_priority_class()));
        auto indexer = std::make_unique<segment_offset_index>(
          base_name + ".offset_index", std::move(fidx), base, 4096);
        auto reader = ss::make_lw_shared<log_segment_reader>(
          base_name,
          ss::open_file_dma(base_name, ss::open_flags::ro).get0(),
          model::term_id(0),
          base,
          appender->file_byte_offset(),
          128);
        _seg = std::make_unique<segment>(
          reader, std::move(indexer), std::move(appender));
        replayer_opt = log_replayer(*_seg);
    }
    ~context() { _seg->close().get(); }
    void write_garbage() {
        auto fd = ss::open_file_dma(
                    base_name, ss::open_flags::create | ss::open_flags::rw)
                    .get0();
        fd = ss::file(ss::make_shared(file_io_sanitizer(std::move(fd))));
        auto out = ss::make_file_output_stream(std::move(fd));
        auto b = test::make_buffer(100);
        out.write(b.get(), b.size()).get();
        out.flush().get();
        out.close().get();
    }

    void write(std::vector<model::record_batch>& batches) {
        do_write(
          [&batches](log_segment_appender& appender) {
              for (auto& b : batches) {
                  storage::write(appender, b).get();
              }
          },
          batches.begin()->base_offset());
    }

    template<typename Writer>
    void do_write(Writer&& w, model::offset base) {
        initialize(base);
        w(*_seg->appender());
        _seg->flush().get();
        _seg->reader()->set_last_visible_byte_offset(
          _seg->appender()->file_byte_offset());
    }

    log_replayer& replayer() { return *replayer_opt; }
};

SEASTAR_THREAD_TEST_CASE(test_can_recover_single_batch) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1), 1);
    auto last_offset = batches.back().last_offset();
    ctx.write(batches);
    auto recovered = ctx.replayer().recover_in_thread(
      ss::default_priority_class());
    BOOST_REQUIRE(bool(recovered));
    BOOST_CHECK(bool(recovered.last_valid_offset()));
    BOOST_CHECK_EQUAL(recovered.last_valid_offset().value(), last_offset);
}

SEASTAR_THREAD_TEST_CASE(test_unrecovered_single_batch) {
    {
        context ctx;
        auto batches = test::make_random_batches(model::offset(1), 1);
        batches.back().get_header_for_testing().crc = 10;
        ctx.write(batches);
        auto recovered = ctx.replayer().recover_in_thread(
          ss::default_priority_class());
        BOOST_CHECK(!bool(recovered));
        BOOST_CHECK(!bool(recovered.last_valid_offset()));
    }
    {
        context ctx;
        auto batches = test::make_random_batches(model::offset(1), 1);
        batches.back().get_header_for_testing().first_timestamp
          = model::timestamp(10);
        ctx.write(batches);
        auto recovered = ctx.replayer().recover_in_thread(
          ss::default_priority_class());
        BOOST_CHECK(!bool(recovered));
        BOOST_CHECK(!bool(recovered.last_valid_offset()));
    }
}

SEASTAR_THREAD_TEST_CASE(test_malformed_segment) {
    context ctx;
    ctx.write_garbage();
    ctx.initialize(model::offset(0));
    auto recovered = ctx.replayer().recover_in_thread(
      ss::default_priority_class());
    BOOST_CHECK(!bool(recovered));
    BOOST_CHECK(!bool(recovered.last_valid_offset()));
}

SEASTAR_THREAD_TEST_CASE(test_can_recover_multiple_batches) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1), 10);
    auto last_offset = batches.back().last_offset();
    ctx.write(batches);
    auto recovered = ctx.replayer().recover_in_thread(
      ss::default_priority_class());
    BOOST_CHECK(bool(recovered));
    BOOST_CHECK(bool(recovered.last_valid_offset()));
    BOOST_CHECK_EQUAL(recovered.last_valid_offset().value(), last_offset);
}

SEASTAR_THREAD_TEST_CASE(test_unrecovered_multiple_batches) {
    {
        context ctx;
        auto batches = test::make_random_batches(model::offset(1), 10);
        batches.back().get_header_for_testing().crc = 10;
        auto last_offset = (batches.end() - 2)->last_offset();
        ctx.write(batches);
        auto recovered = ctx.replayer().recover_in_thread(
          ss::default_priority_class());
        BOOST_CHECK(!bool(recovered));
        BOOST_CHECK(bool(recovered.last_valid_offset()));
        BOOST_CHECK_EQUAL(recovered.last_valid_offset().value(), last_offset);
    }
    {
        context ctx;
        auto batches = test::make_random_batches(model::offset(1), 10);
        batches.back().get_header_for_testing().first_timestamp
          = model::timestamp(10);
        auto last_offset = (batches.end() - 2)->last_offset();
        ctx.write(batches);
        auto recovered = ctx.replayer().recover_in_thread(
          ss::default_priority_class());
        BOOST_CHECK(!bool(recovered));
        BOOST_CHECK(bool(recovered.last_valid_offset()));
        BOOST_CHECK_EQUAL(recovered.last_valid_offset().value(), last_offset);
    }
}
