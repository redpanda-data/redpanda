#include "seastarx.h"
#include "storage/log_replayer.h"
#include "storage/log_segment_reader.h"
#include "storage/log_writer.h"
#include "storage/tests/random_batch.h"
#include "utils/file_sanitizer.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace storage; // NOLINT

struct context {
    segment_reader_ptr log_seg;
    std::optional<log_replayer> replayer_opt;

    void write_garbage() {
        do_write(
          [](log_segment_appender& appender) {
              auto b = test::make_buffer(100);
              appender.append(b.get(), b.size()).get();
          },
          model::offset(0));
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
        auto fd
          = open_file_dma("test", open_flags::create | open_flags::rw).get0();
        fd = file(make_shared(file_io_sanitizer(std::move(fd))));
        auto appender = log_segment_appender(
          fd, log_segment_appender::options(seastar::default_priority_class()));
        w(appender);
        appender.flush().get();
        log_seg = make_lw_shared<log_segment_reader>(
          "test",
          std::move(fd),
          model::term_id(0),
          base,
          appender.file_byte_offset(),
          128);
        replayer_opt = log_replayer(log_seg);
    }

    log_replayer& replayer() { return *replayer_opt; }

    ~context() { log_seg->close().get(); }
};

SEASTAR_THREAD_TEST_CASE(test_can_recover_single_batch) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1), 1);
    auto last_offset = batches.back().last_offset();
    ctx.write(batches);
    auto recovered = ctx.replayer().recover_in_thread(default_priority_class());
    BOOST_CHECK(bool(recovered));
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
          default_priority_class());
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
          default_priority_class());
        BOOST_CHECK(!bool(recovered));
        BOOST_CHECK(!bool(recovered.last_valid_offset()));
    }
}

SEASTAR_THREAD_TEST_CASE(test_malformed_segment) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1), 1);
    batches.back().get_header_for_testing().crc = 10;
    ctx.write_garbage();
    auto recovered = ctx.replayer().recover_in_thread(default_priority_class());
    BOOST_CHECK(!bool(recovered));
    BOOST_CHECK(!bool(recovered.last_valid_offset()));
}

SEASTAR_THREAD_TEST_CASE(test_can_recover_multiple_batches) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1), 10);
    auto last_offset = batches.back().last_offset();
    ctx.write(batches);
    auto recovered = ctx.replayer().recover_in_thread(default_priority_class());
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
          default_priority_class());
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
          default_priority_class());
        BOOST_CHECK(!bool(recovered));
        BOOST_CHECK(bool(recovered.last_valid_offset()));
        BOOST_CHECK_EQUAL(recovered.last_valid_offset().value(), last_offset);
    }
}
