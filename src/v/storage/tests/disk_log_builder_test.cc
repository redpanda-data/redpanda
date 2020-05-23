#include "seastarx.h"
#include "storage/tests/disk_log_builder_fixture.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/fixture.h"

#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>

FIXTURE_TEST(kitchen_sink, log_builder_fixture) {
    using namespace storage; // NOLINT

    auto batch = test::make_random_batch(model::offset(107), 1, false);

    b | start() | add_segment(0)
      | add_random_batch(0, 100, maybe_compress_batches::yes)
      | add_random_batch(100, 2, maybe_compress_batches::yes) | add_segment(102)
      | add_random_batch(102, 2, maybe_compress_batches::yes) | add_segment(104)
      | add_random_batches(104, 3) | add_batch(std::move(batch));

    auto stats = get_stats().get0();

    b | stop();
    BOOST_TEST(stats.seg_count == 3);
    BOOST_TEST(stats.batch_count == 7);
    BOOST_TEST(stats.record_count >= 105);
}

static void do_write_zeroes(ss::sstring name) {
    auto fd = ss::open_file_dma(
                name, ss::open_flags::create | ss::open_flags::rw)
                .get0();
    auto out = ss::make_file_output_stream(std::move(fd));
    ss::temporary_buffer<char> b(4096);
    std::memset(b.get_write(), 0, 4096);
    out.write(b.get(), b.size()).get();
    out.flush().get();
    out.close().get();
}

static void do_write_garbage(ss::sstring name) {
    auto fd = ss::open_file_dma(
                name, ss::open_flags::create | ss::open_flags::rw)
                .get0();
    auto out = ss::make_file_output_stream(std::move(fd));
    const auto b = random_generators::gen_alphanum_string(100);
    out.write(b.data(), b.size()).get();
    out.flush().get();
    out.close().get();
}

FIXTURE_TEST(test_valid_segment_name_with_zeroes_data, log_builder_fixture) {
    using namespace storage; // NOLINT
    auto ntp = model::ntp(
      model::ns("test.ns"), model::topic("zeroes"), model::partition_id(66));
    const ss::sstring dir = fmt::format(
      "{}/{}", b.get_log_config().base_dir, ntp.path());
    // 1. write valid segment names in the namespace with 0 data so crc passes
    recursive_touch_directory(dir).get();
    do_write_zeroes(fmt::format("{}/270-1850-v1.log", dir));
    do_write_zeroes(fmt::format("{}/270-1850-v1.base_index", dir));
    do_write_garbage(fmt::format("{}/271-1850-v1.log", dir));
    do_write_garbage(fmt::format("{}/271-1850-v1.base_index", dir));

    b | start(ntp);
    auto stats = get_stats().get0();
    b | stop();

    BOOST_REQUIRE(
      !ss::file_exists(fmt::format("{}/270-1850-v1.log", dir)).get0());
    BOOST_TEST(stats.seg_count == 0);
    BOOST_TEST(stats.batch_count == 0);
    BOOST_TEST(stats.record_count >= 0);
}
