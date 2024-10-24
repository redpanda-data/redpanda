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
#include "base/units.h"
#include "io/scheduler.h"
#include "io/tests/common.h"
#include "random/generators.h"
#include "test_utils/test.h"

#include <seastar/core/gate.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/later.hh>

#include <boost/range/irange.hpp>

namespace experimental::io::testing_details {
class scheduler_queue_accessor {
public:
    static auto get_queue(io::scheduler::queue* queue) {
        return &queue->io_queue_;
    }
};
} // namespace experimental::io::testing_details

namespace io = experimental::io;

/*
 * params
 *  - disk or memory backend
 *  - max file size
 *  - num io operations
 *  - num of queues
 *  - max number of open files
 */
class SchedulerTest
  : public StorageTest
  , public ::testing::WithParamInterface<
      std::tuple<bool, size_t, size_t, size_t, size_t>> {
public:
    [[nodiscard]] static size_t file_size() { return std::get<1>(GetParam()); }
    [[nodiscard]] static size_t num_io_ops() { return std::get<2>(GetParam()); }
    [[nodiscard]] static size_t num_queues() { return std::get<3>(GetParam()); }
    [[nodiscard]] static size_t open_files() { return std::get<4>(GetParam()); }

    void SetUp() override {
        StorageTest::SetUp();
        scheduler_ = std::make_unique<io::scheduler>(open_files());
    }

    void TearDown() override {
        for (auto& file : cleanup_files_) {
            try {
                seastar::remove_file(file.string()).get();
            } catch (const std::exception& ex) {
                std::ignore = ex;
            }
        }
    }

    io::scheduler* scheduler() { return scheduler_.get(); }

    void add_cleanup_file(std::filesystem::path file) {
        cleanup_files_.push_back(std::move(file));
    }

private:
    [[nodiscard]] bool disk_persistence() const override {
        return std::get<0>(GetParam());
    }

    std::unique_ptr<io::scheduler> scheduler_;
    std::vector<std::filesystem::path> cleanup_files_;
};

/*
 * combines scheduler, queue, load generator, and fault injection.
 */
struct scheduler_tester {
    scheduler_tester(
      io::scheduler* scheduler,
      io::persistence* storage,
      std::filesystem::path path,
      size_t file_size,
      size_t num_io_ops)
      : scheduler(scheduler)
      , queue(
          storage,
          std::move(path),
          [this](auto& page) noexcept { generator.handle_complete(page); })
      , generator(
          file_size,
          num_io_ops,
          [this, scheduler](auto& page) {
              scheduler->submit_read(&queue, &page);
          },
          [this, scheduler](auto& page) {
              scheduler->submit_write(&queue, &page);
          })
      , faulter{
          storage,
          io::testing_details::scheduler_queue_accessor::get_queue(&queue),
          10,
          100 // min,max delay between faults
        } {}

    seastar::future<> run() {
        scheduler->add_queue(&queue);
        faulter.start();
        return generator.run();
    }

    void stop() {
        faulter.stop().get();
        io::scheduler::remove_queue(&queue).get();
    }

    io::scheduler* scheduler;
    io::scheduler::queue queue;
    io_queue_workload_generator generator;
    io_queue_fault_injector faulter;
};

/*
 * This test uses a load generator to run reads and writes against multiple I/O
 * queues while limiting the maximum number of open file handles. Faults are
 * injected during execution.
 */
TEST_P(SchedulerTest, WriteRead) {
    // paths for test files. one per queue.
    std::vector<std::filesystem::path> paths;
    for (auto i : boost::irange(num_queues())) {
        paths.emplace_back(fmt::format("foo.{}", i));
        add_cleanup_file(paths.back());
    }

    // test driver for each queue
    std::vector<std::unique_ptr<scheduler_tester>> drivers;
    drivers.reserve(paths.size());
    for (const auto& path : paths) {
        storage()->create(path).get();
        drivers.push_back(std::make_unique<scheduler_tester>(
          scheduler(), storage(), path, file_size(), num_io_ops()));
    }

    // start the driver for each io queue
    std::vector<seastar::future<>> runners;
    runners.reserve(drivers.size());
    for (auto& driver : drivers) {
        runners.push_back(driver->run());
    }

    seastar::when_all_succeed(runners.begin(), runners.end()).get();

    for (auto& driver : drivers) {
        driver->stop();
    }

    // verify
    for (auto& driver : drivers) {
        const auto& gen = driver->generator;
        ASSERT_GE(gen.reads().size(), num_io_ops());
        ASSERT_FALSE(gen.writes().empty());
        for (const auto& read : gen.reads()) {
            EXPECT_TRUE(gen.writes().contains(read->offset()));
            auto* write = gen.writes().at(read->offset());
            EXPECT_EQ(read->data(), write->data());
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
  Scheduler,
  SchedulerTest,
  ::testing::Combine(
    ::testing::Bool(),              // disk backend?
    ::testing::Values(10_MiB),      // backing file size
    ::testing::Values(600),         // number of io operations
    ::testing::Range<size_t>(1, 3), // num submission queues
    ::testing::Values(1, 2, 6)));   // max open files

/*
 * memory persistence is disabled in this test. some ci environments have
 * limited memory and this has exhausted it.
 */
INSTANTIATE_TEST_SUITE_P(
  SchedulerManyQueues,
  SchedulerTest,
  ::testing::Combine(
    ::testing::Bool(),           // disk backend
    ::testing::Values(1_MiB),    // backing file size
    ::testing::Values(500),      // number of io operations
    ::testing::Values(60),       // num submission queues
    ::testing::Values(60, 61))); // max open files
