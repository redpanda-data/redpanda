/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "io/io_queue.h"
#include "io/page.h"
#include "io/persistence.h"
#include "random/generators.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>

#include <gtest/gtest.h>

#include <deque>
#include <map>

namespace experimental::io::testing_details {
class io_queue_accessor {
public:
    static auto get_file(io::io_queue* queue) { return queue->file_; }
};
} // namespace experimental::io::testing_details

namespace io = experimental::io;

/**
 * Generate random data for use in tests. If seed is unspecified then a
 * pre-seeded random generator will be used.
 */
seastar::future<seastar::temporary_buffer<char>> make_random_data(
  size_t size,
  std::optional<uint64_t> alignment = std::nullopt,
  std::optional<uint64_t> seed = std::nullopt);

/**
 * Generate a page with the given offset and random data. The seed is passed to
 * the random number generator. If no seed is given then a pre-seeded engine
 * will be used.
 *
 * Hard-coded as 4K pages with 4K alignment.
 */
seastar::lw_shared_ptr<io::page>
make_page(uint64_t offset, std::optional<uint64_t> seed = std::nullopt);

/**
 * Sleep for a random number of milliseconds chosen from the range (min, max).
 */
void sleep_ms(unsigned min, unsigned max);

/**
 * A workload generator that can be used to submit random reads and writes to a
 * io queue. Reads are automatically generated for regions previously written.
 * Random data is generated, but the generation is deterministic using the block
 * offset as the seed. This allows multiple inflight I/Os to the same block to
 * result in the same final state, simplifying the accounting needed for testing
 * correctness.
 */
class io_queue_workload_generator {
public:
    /*
     * Writes are generated to positions bounded by max_file_size. The test ends
     * after there have been num_io_ops reads and writes completed. A random
     * delay between (min_ms, max_ms) milliseconds will be inserted between
     * generating a new operation.
     */
    io_queue_workload_generator(
      size_t max_file_size,
      size_t num_io_ops,
      std::function<void(io::page&)> submit_read,
      std::function<void(io::page&)> submit_write,
      unsigned min_ms = 0,
      unsigned max_ms = 10);

    seastar::future<> run();

    /*
     * Call this when a request has completed. It updates the state of the
     * requests that are being tracked.
     */
    void handle_complete(io::page& page);

    // Completed reads
    [[nodiscard]] const auto& reads() const { return completed_reads; }

    // Completed writes
    [[nodiscard]] const auto& writes() const { return completed_writes; }

private:
    seastar::future<> generate_writes();
    seastar::future<> generate_reads();
    [[nodiscard]] io::page* select_random_write() const;

    seastar::gate gate;

    size_t max_file_size;
    size_t num_io_ops;
    std::function<void(io::page&)> submit_read;
    std::function<void(io::page&)> submit_write;
    unsigned min_ms;
    unsigned max_ms;

    std::deque<seastar::lw_shared_ptr<io::page>> submitted_reads;
    std::deque<seastar::lw_shared_ptr<io::page>> submitted_writes;
    std::deque<io::page*> completed_reads;
    std::map<uint64_t, io::page*> completed_writes;

    /*
     * for small files tests there may not be enough unique page offsets in the
     * backing file in order to use the size of the completed_writes map to
     * bound the test size (e.g. 1000 writes to a 16 KB file) would have a
     * completed_writes map of only size 4 * 4K no matter how long the test ran.
     */
    size_t completed_writes_count{0};
};

/*
 * Injects random failures into the storage layer (e.g. fail next open()) and
 * the file object backing an io_queue (e.g. fail next close()).
 */
class io_queue_fault_injector {
public:
    /*
     * A random delay between (min_ms, max_ms) milliseconds will be inserted
     * between faults.
     */
    io_queue_fault_injector(
      io::persistence* storage,
      io::io_queue* queue,
      unsigned min_ms = 1,
      unsigned max_ms = 50);

    void start();
    seastar::future<> stop();

private:
    io::persistence* storage;
    io::io_queue* queue;
    unsigned min_ms;
    unsigned max_ms;

    bool stop_{false};
    seastar::future<> injector{seastar::make_ready_future<>()};
};

/**
 * Helper test fixture that constructs a persistence instance.
 */
class StorageTest : public ::testing::Test {
public:
    void SetUp() override {
        if (disk_persistence()) {
            storage_ = std::make_unique<io::disk_persistence>();
        } else {
            storage_ = std::make_unique<io::memory_persistence>();
        }
    }

    io::persistence* storage() { return storage_.get(); }

private:
    [[nodiscard]] virtual bool disk_persistence() const = 0;

    std::unique_ptr<io::persistence> storage_;
};

/**
 * Test that two input streams yield the same data.
 */
testing::AssertionResult
EqualInputStreams(seastar::input_stream<char>&, seastar::input_stream<char>&);
