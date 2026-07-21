#include "bytes/iostream.h"
#include "config/configuration.h"
#include "container/chunked_vector.h"
#include "random/generators.h"
#include "storage/chunk_cache.h"
#include "storage/segment_appender.h"
#include "storage/storage_resources.h"
#include "test_utils/random_bytes.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>

#include <deque>

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

        resources.start().get();
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

namespace {

/// Wraps a real file and holds every dma write until the test releases
/// it. A held write keeps the corresponding segment_appender inflight
/// entry in the DISPATCHED state, which lets tests order appends,
/// flushes, and write completions deterministically.
class gated_file final : public ss::file_impl {
public:
    explicit gated_file(ss::file f)
      : _file(std::move(f)) {
        _memory_dma_alignment = _file.memory_dma_alignment();
        _disk_read_dma_alignment = _file.disk_read_dma_alignment();
        _disk_write_dma_alignment = _file.disk_write_dma_alignment();
        _disk_overwrite_dma_alignment = _file.disk_overwrite_dma_alignment();
    }

    ss::future<size_t> write_dma(
      uint64_t pos,
      const void* buffer,
      size_t len,
      ss::io_intent* intent) final {
        if (!_gate_closed) {
            return get_file_impl(_file)->write_dma(pos, buffer, len, intent);
        }
        auto w = ss::make_lw_shared<held_write>(pos, buffer, len, intent);
        _held.push_back(w);
        ++_writes_arrived;
        _cv.broadcast();
        return w->result.get_future();
    }

    /// Resolves once at least n dma writes have arrived (over the
    /// lifetime of the file, including already-released ones).
    ss::future<> wait_until_writes_arrived(size_t n) {
        return _cv.wait([this, n] { return _writes_arrived >= n; });
    }

    /// Performs the oldest held write against the real file and
    /// completes the future the appender is waiting on.
    ss::future<> release_next_write() {
        EXPECT_FALSE(_held.empty());
        auto w = _held.front();
        _held.pop_front();
        return get_file_impl(_file)
          ->write_dma(w->pos, w->buffer, w->len, w->intent)
          .then_wrapped([w](ss::future<size_t> f) {
              if (f.failed()) {
                  w->result.set_exception(f.get_exception());
              } else {
                  w->result.set_value(f.get());
              }
          });
    }

    /// Stop holding writes; subsequent dma writes pass straight through.
    void open_gate() {
        EXPECT_TRUE(_held.empty());
        _gate_closed = false;
    }

    size_t held_writes() const { return _held.size(); }

    ss::future<size_t> write_dma(
      uint64_t pos, std::vector<iovec> iov, ss::io_intent* intent) final {
        return get_file_impl(_file)->write_dma(pos, std::move(iov), intent);
    }

    ss::future<size_t> read_dma(
      uint64_t pos, void* buffer, size_t len, ss::io_intent* intent) final {
        return get_file_impl(_file)->read_dma(pos, buffer, len, intent);
    }

    ss::future<size_t> read_dma(
      uint64_t pos, std::vector<iovec> iov, ss::io_intent* intent) final {
        return get_file_impl(_file)->read_dma(pos, std::move(iov), intent);
    }

    ss::future<ss::temporary_buffer<uint8_t>> dma_read_bulk(
      uint64_t offset, size_t range_size, ss::io_intent* intent) final {
        return get_file_impl(_file)->dma_read_bulk(offset, range_size, intent);
    }

    ss::future<> flush() final { return get_file_impl(_file)->flush(); }

    ss::future<struct stat> stat() final {
        return get_file_impl(_file)->stat();
    }

    ss::future<> truncate(uint64_t length) final {
        return get_file_impl(_file)->truncate(length);
    }

    ss::future<> discard(uint64_t offset, uint64_t length) final {
        return get_file_impl(_file)->discard(offset, length);
    }

    ss::future<> allocate(uint64_t position, uint64_t length) final {
        return get_file_impl(_file)->allocate(position, length);
    }

    ss::future<uint64_t> size() final { return get_file_impl(_file)->size(); }

    ss::future<> close() final { return get_file_impl(_file)->close(); }

    std::unique_ptr<ss::file_handle_impl> dup() final {
        return get_file_impl(_file)->dup();
    }

    ss::subscription<ss::directory_entry> list_directory(
      std::function<ss::future<>(ss::directory_entry de)> next) final {
        return get_file_impl(_file)->list_directory(std::move(next));
    }

private:
    struct held_write {
        held_write(
          uint64_t pos, const void* buffer, size_t len, ss::io_intent* intent)
          : pos(pos)
          , buffer(buffer)
          , len(len)
          , intent(intent) {}
        uint64_t pos;
        const void* buffer;
        size_t len;
        ss::io_intent* intent;
        ss::promise<size_t> result;
    };

    ss::file _file;
    bool _gate_closed{true};
    std::deque<ss::lw_shared_ptr<held_write>> _held;
    size_t _writes_arrived{0};
    ss::condition_variable _cv;
};

} // namespace

struct SegmentAppenderGatedWriteFixture : seastar_test {
    using chunk_ptr = ss::lw_shared_ptr<storage::segment_appender::chunk>;

    ss::future<> SetUpAsync() override {
        // The inactive-appender timer dispatches pending head bytes on its
        // own; push it out of the way so only explicit appends and flushes
        // drive the appender. Scoped so the override is reset on teardown and
        // does not leak into other tests in this binary.
        _test_cfg.get("segment_appender_flush_timeout_ms")
          .set_value(std::chrono::milliseconds(600'000));

        auto file = co_await ss::open_file_dma(
          file_name,
          ss::open_flags::rw | ss::open_flags::create
            | ss::open_flags::truncate,
          ss::file_open_options{});

        co_await resources.start();
        _gated = ss::make_shared<gated_file>(std::move(file));
        storage::segment_appender::options opts(std::nullopt, resources, stats);
        appender = std::make_unique<storage::segment_appender>(
          ss::file(_gated), opts);
    }

    ss::future<> TearDownAsync() override {
        co_await ss::remove_file(file_name);
    }

    gated_file& gated() { return *_gated; }

    /// Take chunks out of the cache until it is exhausted, so the next
    /// chunk request blocks. The probing get() that observes exhaustion
    /// becomes the first semaphore waiter; keep it parked so it doesn't
    /// steal a chunk when the cache is refilled unexpectedly.
    void hoard_all_chunks() {
        while (true) {
            auto fut = resources.chunks().get();
            if (!fut.available()) {
                _parked_get = std::move(fut);
                break;
            }
            _hostages.push_back(fut.get());
        }
    }

    void return_all_chunks() {
        for (auto& c : _hostages) {
            resources.chunks().add(c);
        }
        _hostages.clear();
        if (_parked_get) {
            _hostages.push_back(_parked_get->get());
            _parked_get.reset();
        }
    }

    ss::sstring read_file_contents() {
        auto f = ss::open_file_dma(file_name, ss::open_flags::ro).get();
        size_t sz = f.size().get();
        auto in = ss::make_file_input_stream(std::move(f));
        auto buf = in.read_exactly(sz).get();
        in.close().get();
        return {buf.get(), buf.size()};
    }

    scoped_config _test_cfg;
    std::string_view file_name = "test_segment_gated.log";
    storage::storage_resources resources;
    ss::lw_shared_ptr<storage::segment_appender::stats> stats
      = ss::make_lw_shared<storage::segment_appender::stats>();
    ss::shared_ptr<gated_file> _gated;
    std::unique_ptr<storage::segment_appender> appender;
    chunked_vector<chunk_ptr> _hostages;
    std::optional<ss::future<chunk_ptr>> _parked_get;
};

// Reproduces the assert found by Antithesis in the
// append_concurrent_with_prefix_truncate storage test:
//
//   segment_appender.cc: 'file_byte_offset() <= _stable_offset'
//   No inflight writes but eof 15651 > stable offset 15590
//
// The race: do_append()'s remainder-copy branch (taken when the head
// chunk has a DISPATCHED inflight write) nulls _head and then suspends
// waiting for a chunk from the cache, while _bytes_flush_pending still
// accounts bytes that were appended to the old head behind the
// dispatched write. If the inflight write completes during that
// suspension, _inflight drains and the stable offset stops short of
// file_byte_offset(). A concurrent flush() then finds no head chunk, no
// inflight writes, and eof > stable offset: the vassert aborts the
// process (and even without the assert, the flush would fsync without
// those pending bytes).
//
// The choreography below builds that state deterministically:
//   - write #1 is held in DISPATCHED state by the gated file; it pins
//     the head-write sequencing semaphore,
//   - flush #2 therefore leaves write #2 QUEUED, and an append lands
//     61 bytes in the same chunk behind it,
//   - once write #2 is DISPATCHED (and held), the next append takes the
//     remainder-copy branch and blocks on the exhausted chunk cache
//     with _head == nullptr and _bytes_flush_pending == 61,
//   - releasing write #2 drains _inflight, and the subsequent flush()
//     hits the assert.
TEST_F(
  SegmentAppenderGatedWriteFixture, FlushWhileHeadSwapBlockedOnChunkCache) {
    const auto data = random_generators::gen_alphanum_string(221);

    // First write: appended, then dispatched by flush #1 and held by the
    // gated file. It covers file bytes [0, 100).
    appender->append(data.data(), 100).get();
    auto flush1 = appender->flush();
    gated().wait_until_writes_arrived(1).get();
    EXPECT_EQ(appender->file_byte_offset(), 100);

    // The head chunk now has a DISPATCHED write, so this append swaps to
    // a fresh chunk via the remainder-copy branch and lands [100, 150)
    // there as pending bytes.
    appender->append(data.data() + 100, 50).get();
    EXPECT_EQ(appender->file_byte_offset(), 150);
    EXPECT_GT(stats->bytes_copied_in_chunk_remainder, 0);

    // Flush #2 dispatches write #2 for [100, 150). The head-write
    // semaphore is still owned by held write #1, so write #2 stays
    // QUEUED...
    auto flush2 = appender->flush();

    // ...which lets this append put [150, 211) into the same chunk,
    // behind write #2.
    appender->append(data.data() + 150, 61).get();
    EXPECT_EQ(appender->file_byte_offset(), 211);

    // Release write #1: flush #1 completes, and write #2 becomes
    // DISPATCHED and is held by the gated file.
    gated().release_next_write().get();
    flush1.get();
    gated().wait_until_writes_arrived(2).get();
    EXPECT_EQ(gated().held_writes(), 1);

    // The head chunk has a DISPATCHED write again, so the next append
    // takes the remainder-copy branch and blocks waiting for a chunk
    // from the exhausted cache. Unfixed code nulls _head before waiting,
    // even though it still accounts for 61 pending bytes.
    hoard_all_chunks();
    auto blocked_append = appender->append(data.data() + 211, 10);
    EXPECT_FALSE(blocked_append.available());

    // Release write #2: _inflight drains completely and flush #2
    // completes. Fixed code keeps the old head visible so the 61 pending
    // bytes can still be flushed while the append is blocked.
    gated().release_next_write().get();
    flush2.get();
    EXPECT_FALSE(blocked_append.available());
    EXPECT_EQ(appender->file_byte_offset(), 211);

    // BUG: on unfixed code this aborts with
    //   'file_byte_offset() <= _stable_offset'
    //   No inflight writes but eof 211 > stable offset 150
    auto flush3 = appender->flush();
    gated().wait_until_writes_arrived(3).get();
    EXPECT_EQ(gated().held_writes(), 1);

    // Unblock the head swap while write #3 still owns the old head. The
    // replacement must make write #3 responsible for recycling that chunk.
    return_all_chunks();
    blocked_append.get();

    gated().release_next_write().get();
    flush3.get();
    gated().open_gate();

    appender->flush().get();
    appender->close().get();

    EXPECT_EQ(read_file_contents(), data);
}
