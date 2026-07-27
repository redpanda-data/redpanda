#include "bytes/iostream.h"
#include "config/configuration.h"
#include "container/chunked_vector.h"
#include "random/generators.h"
#include "storage/chunk_cache.h"
#include "storage/segment_appender.h"
#include "storage/storage_resources.h"
#include "test_utils/async.h"
#include "test_utils/manual_file.h"
#include "test_utils/random_bytes.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
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
        // Drain background flush_op(false) flushes before close().
        co_await gate.close();
        // Close here so no test body has to, and a failed assertion doesn't
        // trip ~segment_appender's unclosed-appender abort. Skip the content
        // check on failure: the file state is expected to be off.
        if (appender) {
            co_await appender->close();
            if (!HasFailure()) {
                EXPECT_TRUE(co_await file_content_equal_to_reference());
            }
        }
        co_await ss::remove_file(file_name);
    }

    ss::future<> append_data(const char* data, size_t size) {
        reference.append(data, size);
        co_await appender->append(data, size);
    }

    ss::future<> append_data(const iobuf& data) {
        vlog(tst_log.debug, "Appending iobuf of size {}", data.size_bytes());
        reference.append(data.copy());
        co_await appender->append(data.copy());
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
}

TEST_F(SegmentAppenderFixture, AppendAllSizesUpTo1MiB) {
    std::vector<operation> ops;
    for (auto i = 1; i <= 4096; i += 1) {
        execute_operation(write_op(i)).get();
    }
}

TEST_F(SegmentAppenderFixture, TestLargeAppends) {
    std::vector<operation> ops;
    for (size_t i = 1; i <= 128 * 16_KiB; i += 16_KiB) {
        execute_operation(write_op(i)).get();
    }
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
}

TEST_F(SegmentAppenderFixture, TestConcurrentFlushes) {
    execute_concurrent_flush_and_writes(1, 16_KiB).get();
    ASSERT_GT(stats->bytes_copied_in_chunk_remainder, 0);
    ASSERT_EQ(reference.size_bytes(), 16_KiB);
}

TEST_F(SegmentAppenderFixture, TestConcurrentFlushesPageBoundaryWrites) {
    execute_concurrent_flush_and_writes(4_KiB, 1_MiB).get();
    ASSERT_EQ(stats->bytes_copied_in_chunk_remainder, 0);
    ASSERT_GE(reference.size_bytes(), 1_MiB);
}

TEST_F(SegmentAppenderFixture, TestConcurrentFlushesSmallWritesShifted) {
    // write 8 KiB to shift the chunk internal pointer
    execute_operation(write_op(8_KiB)).get();
    // now execute concurrent flushes with 1 byte writes
    execute_concurrent_flush_and_writes(1, 16_KiB).get();
    ASSERT_GT(stats->bytes_copied_in_chunk_remainder, 0);
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

namespace manual_file = tests::manual_file;

struct SegmentAppenderManualFileFixture : seastar_test {
    using chunk_ptr = ss::lw_shared_ptr<storage::segment_appender::chunk>;

    ss::future<> SetUpAsync() override {
        // Push the inactive-appender timer out of the way so only explicit
        // appends and flushes drive the appender.
        _test_cfg.get("segment_appender_flush_timeout_ms")
          .set_value(std::chrono::milliseconds(600'000));

        co_await resources.start();
        storage::segment_appender::options opts(std::nullopt, resources, stats);
        appender = std::make_unique<storage::segment_appender>(
          manual_file::make_file(_device, dma_alignment), opts);
    }

    ss::future<> TearDownAsync() override {
        if (!appender) {
            // SetUpAsync() failed before creating it.
            co_return;
        }
        // Drain before close(): automate everything, un-starve any append
        // parked on the chunk cache, and let it finish. No-ops when the
        // body ran to completion.
        if (!_driver) {
            start_driver(manual_file::io_kind_all);
        }
        _driver->automate(manual_file::io_kind_all);
        co_await return_all_chunks();
        co_await tests::drain_task_queue();
        // Close before the assertions so a failed expectation doesn't trip
        // ~segment_appender's unclosed abort.
        co_await appender->close();
        co_await _driver->stop();
        EXPECT_TRUE(_device.closed);
        EXPECT_EQ(_device.pending_count(), 0);
        // Content checks are meaningless after a failure.
        if (HasFailure()) {
            co_return;
        }
        if (reference.empty()) {
            EXPECT_EQ(_device.submitted(manual_file::io_kind::write), 0)
              << "the test wrote to the device but left `reference` unset, "
                 "skipping content verification";
            co_return;
        }
        EXPECT_EQ(_device.volatile_size(), reference.size());
        EXPECT_EQ(_device.durable_size(), reference.size());
        if (
          _device.volatile_size() != reference.size()
          || _device.durable_size() != reference.size()) {
            co_return;
        }
        // Walk span by span: one view covers at most one device page.
        for (size_t at = 0; at < reference.size();) {
            const auto span = _device.volatile_span(at).substr(
              0, reference.size() - at);
            EXPECT_EQ(
              span, std::string_view(reference).substr(at, span.size()));
            at += span.size();
        }
        expect_durable_prefix(reference.size());
    }

    manual_file::device& dev() { return _device; }

    /// Attach the driver, declaring the kinds it runs; the rest stay parked
    /// for the test to complete.
    manual_file::driver& start_driver(manual_file::io_kind automated) {
        _driver.emplace(_device, automated);
        return *_driver;
    }

    /// Durable content must hold the first \p n bytes of `reference`. A
    /// prefix check: an in-flight write's dma may carry later bytes.
    void expect_durable_prefix(size_t n) {
        if (dev().durable_size() < n) {
            ADD_FAILURE() << "durable size " << dev().durable_size()
                          << " short of the expected prefix " << n;
            return;
        }
        for (size_t at = 0; at < n;) {
            const auto span = dev().durable_span(at).substr(0, n - at);
            EXPECT_EQ(
              span, std::string_view(reference).substr(at, span.size()));
            at += span.size();
        }
    }

    /// Exhaust the chunk cache so the next chunk request blocks. The
    /// probing get() stays parked as the first waiter, so a stray refill
    /// cannot feed the appender. Returns the cumulative hostage count:
    /// every chunk the cache can hand out, less what the appender holds.
    size_t hoard_all_chunks() {
        vassert(!_parked_get, "hoard with a probing get() already parked");
        while (true) {
            auto fut = resources.chunks().get();
            if (!fut.available()) {
                _parked_get = std::move(fut);
                break;
            }
            _hostages.push_back(fut.get());
        }
        return _hostages.size();
    }

    ss::future<> return_all_chunks() {
        for (auto& c : _hostages) {
            resources.chunks().add(c);
        }
        _hostages.clear();
        if (_parked_get) {
            _hostages.push_back(co_await std::move(*_parked_get));
            _parked_get.reset();
        }
    }

    scoped_config _test_cfg;
    static constexpr uint32_t dma_alignment = 4096;
    storage::storage_resources resources;
    ss::lw_shared_ptr<storage::segment_appender::stats> stats
      = ss::make_lw_shared<storage::segment_appender::stats>();
    // Destroyed after `appender`, whose ss::file references it.
    manual_file::device _device{tst_log};
    std::optional<manual_file::driver> _driver;
    std::unique_ptr<storage::segment_appender> appender;
    /// Expected device content, verified at teardown.
    ss::sstring reference;
    chunked_vector<chunk_ptr> _hostages;
    std::optional<ss::future<chunk_ptr>> _parked_get;
};

// Reproduces an assert found by Antithesis:
//
//   segment_appender.cc: 'file_byte_offset() <= _stable_offset'
//   No inflight writes but eof 15651 > stable offset 15590
//
// do_append()'s remainder-copy branch nulled _head while suspended on
// the exhausted chunk cache, with _bytes_flush_pending still accounting
// bytes from the old head. Once the in-flight write completed, a
// concurrent flush() found no head, no inflight writes, and eof >
// stable offset.
//
// Writes are manual at the device; the driver runs everything else.
TEST_F(
  SegmentAppenderManualFileFixture, FlushWhileHeadSwapBlockedOnChunkCache) {
    using manual_file::io_kind;
    using manual_file::io_kind_all;
    auto& driver = start_driver(io_kind_all & ~io_kind::write);
    reference = random_generators::gen_alphanum_string(221);

    // Write #1: dispatched by flush #1 and parked at the device.
    appender->append(reference.data(), 100).get();
    ASSERT_EQ(appender->file_byte_offset(), 100);
    auto flush1 = appender->flush();
    dev().wait_submitted(io_kind::write, 1).get();

    // The head has a DISPATCHED write, so this append remainder-copies
    // the old head's 100 unaligned bytes into a fresh chunk.
    appender->append(reference.data() + 100, 50).get();
    ASSERT_EQ(appender->file_byte_offset(), 150);
    ASSERT_EQ(stats->bytes_copied_in_chunk_remainder, 100);

    // Parked write #1 still owns the head-write semaphore, so flush #2
    // leaves write #2 QUEUED...
    auto flush2 = appender->flush();
    // Drain the reactor: a wrongly dispatched write #2 would have
    // arrived by now.
    tests::drain_task_queue().get();
    ASSERT_EQ(dev().pending_count(io_kind::write), 1);

    // ...so this append lands in the same chunk behind it.
    appender->append(reference.data() + 150, 61).get();
    ASSERT_EQ(appender->file_byte_offset(), 211);

    // Completing write #1 lets write #2 dispatch and park.
    dev().complete_oldest(io_kind::write);
    flush1.get();
    dev().wait_submitted(io_kind::write, 2).get();
    ASSERT_EQ(dev().pending_count(io_kind::write), 1);
    // flush #1 resolved: [0, 100) must be durable.
    expect_durable_prefix(100);

    // With the cache exhausted, the next remainder-copy append blocks
    // waiting for a chunk. Unfixed code nulls _head here with 61 bytes
    // still pending.
    hoard_all_chunks();
    auto blocked_append = appender->append(reference.data() + 211, 10);
    tests::drain_task_queue().get();
    ASSERT_FALSE(blocked_append.available());

    // Completing write #2 drains _inflight while the append is blocked.
    dev().complete_oldest(io_kind::write);
    flush2.get();
    // The append still waits: hoard_all_chunks()'s parked get() would
    // absorb a wrongly released chunk. The remainder-copy count below is
    // what covers the release path.
    tests::drain_task_queue().get();
    ASSERT_FALSE(blocked_append.available());
    EXPECT_EQ(appender->file_byte_offset(), 211);
    // flush #2 promised [0, 150).
    expect_durable_prefix(150);

    // BUG: unfixed code aborts here: eof 211 > stable offset 150.
    auto flush3 = appender->flush();
    dev().wait_submitted(io_kind::write, 3).get();
    ASSERT_EQ(dev().pending_count(io_kind::write), 1);

    // Unblock the head swap while write #3 still owns the old head: the
    // resumed append must copy the remainder without touching the
    // in-flight chunk.
    return_all_chunks().get();
    blocked_append.get();
    ASSERT_EQ(stats->bytes_copied_in_chunk_remainder, 100 + 211);

    dev().complete_oldest(io_kind::write);
    flush3.get();
    // flush #3 resolved: the 61 re-dispatched bytes must be durable.
    // Not 221: a flush covers file_byte_offset() at call time, and the
    // blocked append's bytes were not accepted yet.
    expect_durable_prefix(211);

    // From here the device runs itself; teardown closes the appender and
    // verifies content against `reference`.
    driver.automate(io_kind_all);
}

// The head swap's release path when nothing references the old head:
// write #1 completes while the append waits for a chunk, so the resumed
// swap recycles the old chunk itself.
TEST_F(
  SegmentAppenderManualFileFixture, HeadSwapRecyclesChunkWhenInflightDrained) {
    using manual_file::io_kind;
    using manual_file::io_kind_all;
    auto& driver = start_driver(io_kind_all & ~io_kind::write);
    reference = random_generators::gen_alphanum_string(150);

    appender->append(reference.data(), 100).get();
    ASSERT_EQ(appender->file_byte_offset(), 100);
    auto flush1 = appender->flush();
    dev().wait_submitted(io_kind::write, 1).get();

    // The remainder-copy append blocks on the exhausted cache with parked
    // write #1 still covering the head.
    const auto hoarded = hoard_all_chunks();
    auto blocked_append = appender->append(reference.data() + 100, 50);
    tests::drain_task_queue().get();
    ASSERT_FALSE(blocked_append.available());

    // Write #1 completes while the append waits, draining _inflight.
    dev().complete_oldest(io_kind::write);
    flush1.get();
    tests::drain_task_queue().get();
    ASSERT_FALSE(blocked_append.available());

    // The resumed swap finds no write referencing the old head and
    // recycles it after copying the remainder out.
    return_all_chunks().get();
    blocked_append.get();
    ASSERT_EQ(appender->file_byte_offset(), 150);
    ASSERT_EQ(stats->bytes_copied_in_chunk_remainder, 100);

    // Hoarding again nets the same count -- everything less the
    // appender's one head chunk -- so the swap recycled the old head
    // rather than leaking it.
    ASSERT_EQ(hoard_all_chunks(), hoarded);

    driver.automate(io_kind_all);
}

// Reproduces last-page corruption: the guard against appending into a
// chunk under dma consulted only _inflight.back(), so a newer QUEUED
// write for the same chunk hid the older DISPATCHED one and the append
// landed in the page its dma was still reading.
TEST_F(
  SegmentAppenderManualFileFixture, AppendWhileQueuedWriteHidesInflightDma) {
    using manual_file::io_kind;
    using manual_file::io_kind_all;
    auto& driver = start_driver(io_kind_all & ~io_kind::write);
    reference = random_generators::gen_alphanum_string(221);

    // Write #1: dispatched by flush #1 and parked; pins the head-write
    // semaphore.
    appender->append(reference.data(), 100).get();
    auto flush1 = appender->flush();
    dev().wait_submitted(io_kind::write, 1).get();

    // Remainder-copies into a fresh chunk (head has a dispatched write).
    appender->append(reference.data() + 100, 50).get();
    ASSERT_EQ(appender->file_byte_offset(), 150);

    // flush #2 leaves write #2 QUEUED behind parked write #1...
    auto flush2 = appender->flush();
    // ...so this append lands in the same chunk behind it.
    appender->append(reference.data() + 150, 61).get();
    ASSERT_EQ(appender->file_byte_offset(), 211);

    // Completing write #1 lets write #2 dispatch and park; its dma reads
    // the page the 61 bytes share.
    dev().complete_oldest(io_kind::write);
    flush1.get();
    dev().wait_submitted(io_kind::write, 2).get();

    // flush #3 queues write #3 for the 61 bytes: _inflight.back() is now
    // QUEUED for the head chunk, hiding dispatched write #2.
    auto flush3 = appender->flush();
    tests::drain_task_queue().get();
    ASSERT_EQ(dev().pending_count(io_kind::write), 1);

    // BUG: unfixed code consults only _inflight.back(), appends in place
    // at 211 -- inside write #2's in-flight dma page -- and aborts on the
    // snapshot check when write #2 completes below. Fixed code
    // remainder-copies into a fresh chunk.
    appender->append(reference.data() + 211, 10).get();
    ASSERT_EQ(appender->file_byte_offset(), 221);
    ASSERT_EQ(stats->bytes_copied_in_chunk_remainder, 100 + 211);

    dev().complete_oldest(io_kind::write);
    flush2.get();
    expect_durable_prefix(150);

    // Write #2's completion released the head-write semaphore; write #3
    // dispatches and parks.
    dev().wait_submitted(io_kind::write, 3).get();
    dev().complete_oldest(io_kind::write);
    flush3.get();
    expect_durable_prefix(211);

    // Teardown writes the final [211, 221) and verifies content.
    driver.automate(io_kind_all);
}

// The guard's permit: a dispatched write that ended on a page boundary
// leaves nothing to protect above it, so the next append lands in place
// while the dma is in flight -- no remainder copy, no chunk consumed.
// The old guard's copy branch would copy a zero-byte remainder here,
// which the counter cannot distinguish; taking a chunk from the
// exhausted cache is what tells them apart. Completing write #1 checks
// its buffer against the dispatch-time snapshot, so an inexact recorded
// dma extent aborts.
TEST_F(SegmentAppenderManualFileFixture, AppendPastInflightDmaEndStaysInPlace) {
    using manual_file::io_kind;
    using manual_file::io_kind_all;
    auto& driver = start_driver(io_kind_all & ~io_kind::write);
    reference = random_generators::gen_alphanum_string(4_KiB + 100);

    // Write #1 covers exactly one page: its dma ends on the boundary.
    appender->append(reference.data(), 4_KiB).get();
    ASSERT_EQ(appender->file_byte_offset(), 4_KiB);
    auto flush1 = appender->flush();
    dev().wait_submitted(io_kind::write, 1).get();

    // In place: needs no chunk from the exhausted cache.
    hoard_all_chunks();
    auto in_place_append = appender->append(reference.data() + 4_KiB, 100);
    tests::drain_task_queue().get();
    ASSERT_TRUE(in_place_append.available());
    in_place_append.get();
    ASSERT_EQ(appender->file_byte_offset(), 4_KiB + 100);
    ASSERT_EQ(stats->bytes_copied_in_chunk_remainder, 0);

    dev().complete_oldest(io_kind::write);
    flush1.get();
    expect_durable_prefix(4_KiB);

    // Teardown writes the trailing 100 bytes and verifies content.
    driver.automate(io_kind_all);
}
