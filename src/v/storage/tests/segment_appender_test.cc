#include "bytes/iostream.h"
#include "storage/segment_appender.h"
#include "test_utils/random_bytes.h"
#include "test_utils/test.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>

static ss::logger tst_log("test-logger");

struct write_op {
    explicit write_op(size_t s)
      : size(s) {}
    explicit write_op(iobuf d)
      : data(std::move(d))
      , size(data->size_bytes()) {}
    std::optional<iobuf> data;
    size_t size;
};

struct flush_op {
    explicit flush_op(bool wait_for_flush)
      : wait_for_flush(wait_for_flush) {}
    bool wait_for_flush = false;
};

struct verify_op {};

struct truncate_op {
    explicit truncate_op(size_t n)
      : truncate_offset(n) {}
    size_t truncate_offset;
};

using operation = std::variant<write_op, flush_op, verify_op, truncate_op>;

struct SegmentAppenderFixture : seastar_test {
public:
    ss::future<> SetUpAsync() override {
        auto file = co_await ss::open_file_dma(
          "test_segment.log",
          ss::open_flags::rw | ss::open_flags::create
            | ss::open_flags::truncate,
          ss::file_open_options{});

        storage::segment_appender::options opts(std::nullopt, resources, stats);
        appender = std::make_unique<storage::segment_appender>(
          std::move(file), opts);
    }

    ss::future<> TearDownAsync() override {
        vlog(
          tst_log.debug,
          "Total appended size: {} bytes",
          reference.size_bytes());
        co_await ss::remove_file(file_name);
    }

    ss::future<> append_data(const char* data, size_t size) {
        co_await appender->append(data, size);
        reference.append(data, size);
    }

    ss::future<> append_data(const iobuf& data) {
        vlog(tst_log.debug, "Appending iobuf of size {}", data.size_bytes());
        co_await appender->append(data.copy());
        reference.append(data.copy());
    }

    ss::future<bool> file_content_equal_to_reference() {
        auto file = co_await ss::open_file_dma(
          file_name, ss::open_flags::ro, ss::file_open_options{});
        size_t file_size = co_await file.size();
        vassert(
          reference.size_bytes() == file_size,
          "File size {} does not match reference size {}",
          file_size,
          reference.size_bytes());

        ss::input_stream<char> in = ss::make_file_input_stream(
          std::move(file), 0, ss::file_input_stream_options{});
        auto ref_stream = make_iobuf_input_stream(reference.share());
        uint64_t offset = 0;
        while (!ref_stream.eof()) {
            auto ref_data = co_await ref_stream.read_exactly(4_KiB);
            auto file_data = co_await in.read_exactly(ref_data.size());
            vassert(
              ref_data == file_data, "Data mismatch at offset {}", offset);
            offset += ref_data.size();
        }
        co_return true;
    }

    ss::future<bool> reference_range_equal_to_file() {
        auto file = co_await ss::open_file_dma(
          file_name, ss::open_flags::ro, ss::file_open_options{});
        size_t file_size = co_await file.size();
        vassert(
          reference.size_bytes() <= file_size,
          "File size {} does not match reference size {}",
          file_size,
          reference.size_bytes());

        ss::input_stream<char> in = ss::make_file_input_stream(
          std::move(file), 0, ss::file_input_stream_options{});
        auto ref_stream = make_iobuf_input_stream(reference.share());
        uint64_t offset = 0;
        while (!ref_stream.eof()) {
            auto ref_data = co_await ref_stream.read_exactly(4_KiB);
            auto file_data = co_await in.read_exactly(ref_data.size());
            vassert(
              ref_data == file_data, "Data mismatch at offset {}", offset);
            offset += ref_data.size();
        }
        co_return true;
    }
    ss::future<> do_write(const write_op& w) {
        vlog(tst_log.debug, "[write] {} bytes", w.size);
        if (w.data) {
            co_await append_data(*w.data);
        } else {
            co_await append_data(tests::random_iobuf(w.size));
        }
    }

    ss::future<> execute_operation(operation op) {
        co_await ss::visit(
          op,
          [this](const write_op& w) { return do_write(w); },
          [this](const flush_op& f_op) {
              vlog(tst_log.debug, "[flush] wait: {}", f_op.wait_for_flush);
              auto f = ss::with_gate(
                gate, [this]() mutable { return appender->flush(); });
              if (f_op.wait_for_flush) {
                  return f;
              }

              return ss::now();
          },
          [this](const verify_op&) {
              vlog(tst_log.debug, "[verify]");
              return reference_range_equal_to_file().discard_result();
          },
          [this](const truncate_op& t_op) {
              vlog(tst_log.debug, "[truncate] size: {}", t_op.truncate_offset);
              auto to_trim = reference.size_bytes() - t_op.truncate_offset;
              reference.trim_back(to_trim);
              return appender->truncate(t_op.truncate_offset);
          });
    }

    ss::future<> execute_operations(chunked_vector<operation> ops) {
        for (auto& op : ops) {
            co_await execute_operation(std::move(op));
        }
    }

    ss::future<> execute_concurrent_flush_and_writes(
      size_t write_size, size_t total_bytes_to_write) {
        using namespace std::chrono_literals;
        bool writes_done = false;
        chunked_vector<ss::future<>> flush_futures;
        auto flusher = ss::do_until(
          [&] { return writes_done; },
          [&] {
              flush_futures.push_back(appender->flush());
              return ss::sleep(5us);
          });

        size_t counter = 0;
        auto writer = ss::do_until(
          [&] { return counter >= total_bytes_to_write; },
          [&] {
              return execute_operation(write_op(write_size)).then([&] {
                  counter += write_size;
              });
          });

        co_await std::move(writer);
        writes_done = true;
        co_await std::move(flusher);
        co_await ss::when_all_succeed(
          flush_futures.begin(), flush_futures.end());
    }

    std::string_view file_name = "test_segment.log";
    storage::storage_resources resources;
    ss::lw_shared_ptr<storage::segment_appender::stats> stats
      = ss::make_lw_shared<storage::segment_appender::stats>();
    std::unique_ptr<storage::segment_appender> appender;
    ss::gate gate;
    iobuf reference;
};

TEST_F(SegmentAppenderFixture, AppendMixedData) {
    chunked_vector<operation> ops;
    ops.emplace_back(write_op(64));
    ops.emplace_back(flush_op(true));
    ops.emplace_back(write_op(1024));
    ops.emplace_back(flush_op(true));
    ops.emplace_back(verify_op{});
    ops.emplace_back(write_op(12));
    ops.emplace_back(write_op(13));
    ops.emplace_back(flush_op(false));
    ops.emplace_back(write_op(45));
    ops.emplace_back(write_op(256));
    ops.emplace_back(flush_op(true));
    execute_operations(std::move(ops)).get();
    appender->close().get();
    ASSERT_TRUE(file_content_equal_to_reference().get());
}

TEST_F(SegmentAppenderFixture, AppendAllSizesUpTo1MiB) {
    std::vector<operation> ops;
    for (auto i = 1; i <= 4096; i += 1) {
        execute_operation(write_op(i)).get();
    }

    appender->close().get();
    ASSERT_TRUE(file_content_equal_to_reference().get());
}

TEST_F(SegmentAppenderFixture, TestLargeAppends) {
    std::vector<operation> ops;
    for (size_t i = 1; i <= 128 * 16_KiB; i += 16_KiB) {
        execute_operation(write_op(i)).get();
    }

    appender->close().get();
    ASSERT_TRUE(file_content_equal_to_reference().get());
}

TEST_F(SegmentAppenderFixture, TestTruncation) {
    chunked_vector<operation> ops;
    // append 1 MiB in 64 KiB chunks
    for (size_t i = 0; i < 16; ++i) {
        ops.emplace_back(write_op(64_KiB));
    }
    ops.emplace_back(flush_op(true));
    // truncate to 512 KiB
    ops.emplace_back(truncate_op(512_KiB));
    ops.emplace_back(verify_op{});
    // append another 256 KiB
    for (size_t i = 0; i < 4; ++i) {
        ops.emplace_back(write_op(64_KiB));
    }

    execute_operations(std::move(ops)).get();
    appender->close().get();
    ASSERT_TRUE(file_content_equal_to_reference().get());
}

TEST_F(SegmentAppenderFixture, TestFlushesAreMerged) {
    chunked_vector<operation> ops;
    // append 1 MiB in 16 KiB chunks with flushes in between
    for (size_t i = 0; i < 64; ++i) {
        ops.emplace_back(write_op(16_KiB));
    }
    for (auto i = 0; i < 64; ++i) {
        ops.emplace_back(flush_op(false));
    }
    ops.emplace_back(flush_op(true));
    execute_operations(std::move(ops)).get();
    EXPECT_GE(stats->fsyncs, 1);
    // TODO: fix possible redundant flushes in segment appender
    // EXPECT_LE(appender->get_stats().fsyncs, 2);
    appender->close().get();
    ASSERT_TRUE(file_content_equal_to_reference().get());
}

TEST_F(SegmentAppenderFixture, TestConcurrentFlushes) {
    execute_concurrent_flush_and_writes(1, 16_KiB).get();
    ASSERT_GT(stats->bytes_copied_in_chunk_remainder, 0);
    appender->close().get();
    ASSERT_TRUE(file_content_equal_to_reference().get());
    ASSERT_EQ(reference.size_bytes(), 16_KiB);
}

TEST_F(SegmentAppenderFixture, TestConcurrentFlushesPageBoundaryWrites) {
    execute_concurrent_flush_and_writes(4_KiB, 1_MiB).get();
    ASSERT_EQ(stats->bytes_copied_in_chunk_remainder, 0);
    appender->close().get();
    ASSERT_TRUE(file_content_equal_to_reference().get());
    ASSERT_GE(reference.size_bytes(), 1_MiB);
}

TEST_F(SegmentAppenderFixture, TestConcurrentFlushesSmallWritesShifted) {
    // write 8 KiB to shift the chunk internal pointer
    execute_operation(write_op(8_KiB)).get();
    // now execute concurrent flushes with 1 byte writes
    execute_concurrent_flush_and_writes(1, 16_KiB).get();
    ASSERT_GT(stats->bytes_copied_in_chunk_remainder, 0);
    appender->close().get();
    ASSERT_TRUE(file_content_equal_to_reference().get());
    ASSERT_GE(reference.size_bytes(), 24_KiB);
}

using chunk = storage::segment_appender_chunk;
namespace {

chunk make_chunk(size_t chunk_size) {
    return chunk(chunk_size, storage::alignment(4_KiB));
}

size_t append_to_chunk(chunk& c, size_t size) {
    std::vector<char> data(size, '1');
    return c.append(data.data(), data.size());
}
} // namespace

TEST(SegmentAppenderChunk, test_copying_reminder) {
    auto chunk_1 = make_chunk(16_KiB);
    append_to_chunk(chunk_1, 10_KiB); // append 10 KiB
    auto chunk_2 = make_chunk(16_KiB);
    // whole chunk should be copied to chunk_2 as no data was flushed
    chunk_2.copy_remainder_from(chunk_1);

    ASSERT_EQ(chunk_2.size(), 10_KiB);

    chunk_1.flush();
    auto chunk_3 = make_chunk(16_KiB);
    // only last 2 KiB should be
    chunk_3.copy_remainder_from(chunk_1);

    ASSERT_EQ(chunk_3.size(), 2_KiB);
    ASSERT_EQ(chunk_3.flushed_pos(), 2_KiB);

    append_to_chunk(chunk_1, 3); // append 3 bytes
    auto chunk_4 = make_chunk(16_KiB);
    chunk_4.copy_remainder_from(chunk_1);
    // flushed position is preserved, only last 2 KiB + 3 bytes appended
    ASSERT_EQ(chunk_4.flushed_pos(), 2_KiB);
    ASSERT_EQ(chunk_4.size(), 2_KiB + 3);
}
