/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "base/units.h"
#include "io/io_queue.h"
#include "io/tests/common.h"
#include "random/generators.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>

#include <gtest/gtest.h>

namespace io = experimental::io;

/*
 * params
 *  - disk or memory backend
 *  - max file size
 *  - num io operations
 */
class IOQueueTest
  : public StorageTest
  , public ::testing::WithParamInterface<std::tuple<bool, size_t, size_t>> {
public:
    [[nodiscard]] static size_t file_size() { return std::get<1>(GetParam()); }
    [[nodiscard]] static size_t num_io_ops() { return std::get<2>(GetParam()); }

private:
    [[nodiscard]] bool disk_persistence() const override {
        return std::get<0>(GetParam());
    }
};

/*
 * helper combining io_queue, workload generator, and fault injection.
 */
struct queue_tester {
    queue_tester(
      io::persistence* storage,
      std::filesystem::path path,
      size_t file_size,
      size_t num_io_ops)
      : queue(
          storage,
          std::move(path),
          [this](io::page& page) noexcept { generator.handle_complete(page); })
      , generator(
          file_size,
          num_io_ops,
          [this](auto& page) { queue.submit_read(page); },
          [this](auto& page) { queue.submit_write(page); })
      , faulter(storage, &queue) {}

    void run() {
        // start
        faulter.start();
        open_close_looper = open_close_loop();
        queue.start();
        generator.run().get();

        // stop
        stop_ = true;
        faulter.stop().get();
        queue.stop().get();
        std::move(open_close_looper).get();
    }

    /*
     * injects open() and close() calls during the test.
     */
    seastar::future<> open_close_loop() {
        return seastar::async([this] {
            while (!stop_) {
                try {
                    queue.open().get();
                } catch (const std::exception& ex) {
                    std::ignore = ex;
                }
                sleep_ms(10, 50);
                if (queue.opened()) {
                    queue.close().get();
                    sleep_ms(10, 50);
                }
            }
        });
    }

    io::io_queue queue;
    io_queue_workload_generator generator;
    io_queue_fault_injector faulter;

    bool stop_{false};
    seastar::future<> open_close_looper{seastar::make_ready_future<>()};
};

TEST_P(IOQueueTest, WriteRead) {
    queue_tester qt(storage(), "foo", file_size(), num_io_ops());
    storage()->create(qt.queue.path()).get();
    qt.run();

    ASSERT_GE(qt.generator.reads().size(), num_io_ops());
    ASSERT_FALSE(qt.generator.writes().empty());

    for (const auto& read : qt.generator.reads()) {
        EXPECT_TRUE(qt.generator.writes().contains(read->offset()));
        auto* write = qt.generator.writes().at(read->offset());
        EXPECT_EQ(read->data(), write->data());
    }

    try {
        seastar::remove_file(qt.queue.path().string()).get();
    } catch (const std::exception& ex) {
        std::ignore = ex;
    }
}

INSTANTIATE_TEST_SUITE_P(
  IOQueue,
  IOQueueTest,
  ::testing::Combine(
    ::testing::Bool(),                 // disk backend?
    ::testing::Values(16_KiB, 10_MiB), // backing file size
    ::testing::Values(10, 3000)));     // number of io operations
