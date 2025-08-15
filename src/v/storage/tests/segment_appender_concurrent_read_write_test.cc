/*
 *
 */
#include "absl/crc/crc32c.h"
#include "random/generators.h"
#include "storage/segment_appender.h"
#include "storage/storage_resources.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

/*
 * TODO: make contention at head of append log be higher. Right now the appender
 * runs far ahead of the reader.
 */

constexpr auto file_name = "test.txt";
constexpr auto fallocate_size = 20_MiB;
constexpr auto max_append_size = 8_KiB;
constexpr auto runtime = std::chrono::seconds(10);
constexpr auto num_chunks = 10;

/*
 * Track the CRC of file data in range [0, size).
 */
struct crc_checkpoint {
    absl::crc32c_t crc{0};
    size_t size{0};
};

namespace {

/*
 * Extend an existing CRC with data from a file. The starting offset is given by
 * size field in the CRC checkpoint being extended.
 */
seastar::future<crc_checkpoint>
extend_crc(crc_checkpoint crc, seastar::file fh, size_t size) {
    seastar::file_input_stream_options options;
    auto input = seastar::make_file_input_stream(
      std::move(fh), crc.size, size - crc.size, options);

    while (true) {
        auto data = co_await input.read();
        if (data.empty()) {
            break;
        }
        crc.crc = absl::ExtendCrc32c(
          crc.crc, std::string_view(data.get(), data.size()));
        crc.size += data.size();
    }

    co_return crc;
}

} // namespace

/*
 * Track the file size up to which it is safe to read.
 */
struct file_size_tracker : storage::segment_appender::callbacks {
    void committed_physical_offset(size_t size) override { file_size = size; }
    size_t file_size{0};
};

struct append_generator {
    explicit append_generator(const seastar::abort_source* asrc)
      : asrc(asrc)
      , resources(config::mock_binding(+fallocate_size)) {}

    seastar::future<> init() {
        auto fh = co_await seastar::open_file_dma(
          file_name,
          seastar::open_flags::create | seastar::open_flags::rw
            | seastar::open_flags::truncate);

        appender = std::make_unique<storage::segment_appender>(
          std::move(fh),
          storage::segment_appender::options(
            num_chunks, std::nullopt, resources));

        appender->set_callbacks(&file_size_tracker);
    }

    seastar::future<> start() {
        while (!asrc->abort_requested()) {
            auto data = generate_data(max_append_size);
            crc.crc = absl::ExtendCrc32c(crc.crc, data);
            crc.size += data.size();
            co_await appender->append(data.data(), data.size());
        }

        co_await appender->close();
    }

    seastar::sstring generate_data(size_t max_size) {
        const auto size = random_generators::get_int<size_t>(1, max_size);
        return random_generators::gen_alphanum_string(size);
    }

    crc_checkpoint crc;
    file_size_tracker file_size_tracker;
    std::unique_ptr<storage::segment_appender> appender;
    const seastar::abort_source* asrc;
    storage::storage_resources resources;
};

struct flush_generator {
    flush_generator(
      const seastar::abort_source* asrc, storage::segment_appender* appender)
      : asrc(asrc)
      , appender(appender) {}

    seastar::future<> start() {
        while (!asrc->abort_requested()) {
            co_await appender->flush();
        }
    }

    const seastar::abort_source* asrc;
    storage::segment_appender* appender;
};

struct read_generator {
    read_generator(
      const seastar::abort_source* asrc,
      const crc_checkpoint* appender_crc,
      const file_size_tracker* file_size_tracker)
      : asrc(asrc)
      , appender_crc(appender_crc)
      , file_size_tracker(file_size_tracker) {}

    seastar::future<> start() {
        auto fh = co_await seastar::open_file_dma(
          file_name,
          ss::open_flags::ro | ss::open_flags::create,
          ss::file_open_options{});

        auto next_crc = *appender_crc;
        crc_checkpoint curr_crc;

        while (!asrc->abort_requested()) {
            if (next_crc.size > file_size_tracker->file_size) {
                co_await seastar::yield();
                continue;
            }

            curr_crc = co_await extend_crc(curr_crc, fh, next_crc.size);
            ASSERT_EQ_CORO(curr_crc.size, next_crc.size);
            ASSERT_EQ_CORO(curr_crc.crc, next_crc.crc);
            fmt::print(
              "{} {}: {} {}\n",
              curr_crc.crc,
              next_crc.crc,
              curr_crc.size,
              file_size_tracker->file_size);
            next_crc = *appender_crc;
        }
    }

    const seastar::abort_source* asrc;
    const crc_checkpoint* appender_crc;
    const file_size_tracker* file_size_tracker;
};

TEST_CORO(segment_appender_concurrent_read_write_test, test_for_corruption) {
    seastar::abort_source asrc;
    append_generator appender(&asrc);
    co_await appender.init();
    flush_generator flusher(&asrc, appender.appender.get());
    read_generator reader(&asrc, &appender.crc, &appender.file_size_tracker);

    // run workload generators for a while then shutdown
    auto generators = seastar::when_all(
      appender.start(), flusher.start(), reader.start());
    co_await seastar::sleep(runtime);
    asrc.request_abort();
    co_await std::move(generators);

    // compute crc over entire file after generators are shutdown
    ASSERT_EQ_CORO(appender.crc.size, appender.file_size_tracker.file_size);
    auto fh = co_await seastar::open_file_dma(file_name, ss::open_flags::ro);
    auto computed_crc = co_await extend_crc(
      crc_checkpoint{}, std::move(fh), appender.crc.size);
    ASSERT_EQ_CORO(computed_crc.crc, appender.crc.crc);
    ASSERT_EQ_CORO(computed_crc.size, appender.crc.size);
}
