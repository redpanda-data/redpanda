#include "bytes/iostream.h"
#include "storage/segment_appender.h"
#include "test_utils/random_bytes.h"
#include "test_utils/test.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/seastar.hh>

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

        storage::segment_appender::options opts(std::nullopt, resources);
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
            co_await append_data(w.data.value());
        } else {
            co_await append_data(tests::random_iobuf(w.size));
        }
    }

    ss::future<> execute_operation(const operation& op) {
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

    ss::future<> execute_operations(const std::vector<operation>& ops) {
        for (const auto& op : ops) {
            co_await execute_operation(op);
        }
    }

    std::string_view file_name = "test_segment.log";
    storage::storage_resources resources;
    std::unique_ptr<storage::segment_appender> appender;
    ss::gate gate;
    iobuf reference;
};

TEST_F(SegmentAppenderFixture, AppendMixedData) {
    std::vector<operation> ops;
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
    execute_operations(ops).get();
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
    std::vector<operation> ops;
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

    execute_operations(ops).get();
    appender->close().get();
    ASSERT_TRUE(file_content_equal_to_reference().get());
}

TEST_F(SegmentAppenderFixture, TestFlushesAreMerged) {
    std::vector<operation> ops;
    // append 1 MiB in 16 KiB chunks with flushes in between
    for (size_t i = 0; i < 64; ++i) {
        ops.emplace_back(write_op(16_KiB));
    }
    for (auto i = 0; i < 64; ++i) {
        ops.emplace_back(flush_op(false));
    }
    ops.emplace_back(flush_op(true));
    execute_operations(ops).get();
    EXPECT_GE(appender->get_stats().fsyncs, 1);
    // TODO: fix possible redundant flushes in segment appender
    EXPECT_LE(appender->get_stats().fsyncs, 2);
    appender->close().get();
    ASSERT_TRUE(file_content_equal_to_reference().get());
}
