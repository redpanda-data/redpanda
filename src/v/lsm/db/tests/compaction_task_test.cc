/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iobuf.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/compaction_task.h"
#include "lsm/db/snapshot.h"
#include "lsm/db/tests/db_test_base.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/io/persistence.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/coroutine/generator.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <memory>

using namespace lsm::db;

using lsm::internal::operator""_level;
using lsm::internal::operator""_file_id;
using lsm::internal::operator""_key;

namespace {

// Tracks the close/destruction lifecycle of a single output writer handed out
// by the fault-injecting persistence layer below.
struct writer_close_tracker {
    bool appended = false;
    bool closed = false;
    bool destroyed_without_close = false;
};

// A writer that fails its first append with ss::timed_out_error, mimicking a
// cloud staging file that times out waiting for a cache reservation. Crucially
// the exception is *not* an lsm::base_exception, so it escapes the compaction
// loop's catch and exercises the coroutine-unwind cleanup path.
//
// On destruction it records whether close() was ever called. In production the
// equivalent leak aborts via the seastar output_stream "was this stream
// properly closed?" assertion; here we observe it without crashing the test.
class timing_out_file_writer final : public lsm::io::sequential_file_writer {
public:
    explicit timing_out_file_writer(writer_close_tracker* tracker)
      : _tracker(tracker) {}

    ~timing_out_file_writer() override {
        if (!_closed) {
            _tracker->destroyed_without_close = true;
        }
    }

    ss::future<> append(iobuf) override {
        _tracker->appended = true;
        return ss::make_exception_future<>(ss::timed_out_error());
    }

    ss::future<> close() override {
        _closed = true;
        _tracker->closed = true;
        return ss::now();
    }

    fmt::iterator format_to(fmt::iterator it) const override {
        return fmt::format_to(it, "{{timing_out_file_writer}}");
    }

private:
    bool _closed = false;
    writer_close_tracker* _tracker;
};

// Wraps an in-memory data_persistence. Reads, removals, and (when not armed)
// writes delegate to the inner layer so we can build real input SSTs. Once
// fail_next_writers() is called, every newly opened writer fails its first
// append, letting us drive the compaction task into its error-unwind path.
class fault_injecting_data_persistence final
  : public lsm::io::data_persistence {
public:
    fault_injecting_data_persistence()
      : _inner(lsm::io::make_memory_data_persistence()) {}

    void fail_next_writers() { _fail_writers = true; }
    const writer_close_tracker& last_output_writer() const {
        return _last_output_writer;
    }

    ss::future<lsm::io::optional_pointer<lsm::io::random_access_file_reader>>
    open_random_access_reader(lsm::internal::file_handle h) override {
        return _inner->open_random_access_reader(h);
    }

    ss::future<std::unique_ptr<lsm::io::sequential_file_writer>>
    open_sequential_writer(lsm::internal::file_handle h) override {
        if (_fail_writers) {
            _last_output_writer = {};
            co_return std::make_unique<timing_out_file_writer>(
              &_last_output_writer);
        }
        co_return co_await _inner->open_sequential_writer(h);
    }

    ss::future<> remove_file(lsm::internal::file_handle h) override {
        return _inner->remove_file(h);
    }

    ss::coroutine::experimental::generator<lsm::internal::file_handle>
    list_files() override {
        auto gen = _inner->list_files();
        while (auto fh = co_await gen()) {
            co_yield fh->get();
        }
    }

    ss::future<> close() override { return _inner->close(); }

private:
    std::unique_ptr<lsm::io::data_persistence> _inner;
    bool _fail_writers = false;
    writer_close_tracker _last_output_writer;
};

class CompactionTaskTest : public db_test_base {
public:
    CompactionTaskTest()
      : db_test_base(std::make_unique<fault_injecting_data_persistence>()) {
        // A single L0 file is enough to trigger compaction, keeping the setup
        // minimal.
        _options->level_one_compaction_trigger = 1;
        // Force the output builder to flush (and thus append to the writer) on
        // the very first key so the injected failure happens inside the
        // compaction loop, while compaction_state::builder is still set.
        _options->sst_block_size = 1;
        _options->levels = lsm::internal::options::make_levels(
          {
            .max_total_bytes = 10000,
            .max_file_size = 1000,
          },
          /*multiplier=*/10,
          /*max_level=*/6_level);
    }

    fault_injecting_data_persistence& faults() {
        return static_cast<fault_injecting_data_persistence&>(
          *_data_persistence);
    }

protected:
    snapshot_list _snapshots;
};

} // namespace

// Regression test for a compaction lifetime bug: when an output writer fails
// mid-compaction with a non-lsm exception (e.g. a cloud staging file timing
// out under cache pressure), the compaction coroutine unwinds. If the in-flight
// builder is destroyed without first being closed, its staging file/output
// stream is torn down without close() and aborts the process.
//
// The task must instead always close the in-flight output writer before its
// state is destroyed, and surface the original failure.
TEST_F(CompactionTaskTest, InFlightOutputWriterClosedWhenAppendFails) {
    // One L0 file plus an overlapping L1 file produce a non-trivial L0->L1
    // compaction (a trivial move would never open an output builder).
    add_sst(
      {.id = 1_file_id, .level = 0_level, .keys = {"d@1"_key, "e@1"_key}});
    add_sst(
      {.id = 10_file_id, .level = 1_level, .keys = {"a@1"_key, "z@1"_key}});

    // Arm the persistence so the compaction's output writer fails on append.
    faults().fail_next_writers();

    auto maybe_c = version_set().pick_compaction();
    ASSERT_TRUE(maybe_c);
    auto& c = *maybe_c;
    ASSERT_FALSE(c->is_trivial_move());

    ss::abort_source as;
    // The injected failure must surface as the task's result...
    EXPECT_THROW(
      run_compaction_task(
        &faults(), &_snapshots, &version_set(), _options, c.get(), &as)
        .get(),
      ss::timed_out_error);

    // ...and the in-flight output writer must have been closed rather than
    // leaked during the coroutine unwind.
    const auto& w = faults().last_output_writer();
    ASSERT_TRUE(w.appended);
    EXPECT_TRUE(w.closed);
    EXPECT_FALSE(w.destroyed_without_close);
}
