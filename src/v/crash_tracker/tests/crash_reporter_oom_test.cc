/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "config/node_config.h"
#include "crash_tracker/recorder.h"
#include "resource_mgmt/memory_sampling.h"
#include "test_utils/tmp_dir.h"
#include "utils.h"
#include "utils/human.h"

#include <seastar/core/memory.hh>

#include <gtest/gtest.h>

namespace crash_tracker {
class CrashReporterOOMTest : public testing::Test {
public:
    void SetUp() override {
        _helper.SetUp();
        get_recorder().start().get();
    }
    void TearDown() override {
        get_recorder().stop().get();
        get_recorder().reset();
        _helper.TearDown();
    }

private:
    RecorderTestHelper _helper;
};

TEST_F(CrashReporterOOMTest, TestOOMResults) {
    ASSERT_TRUE(get_recorder().get_recorded_crashes().get().empty());
    auto cb = memory_sampling::get_oom_diagnostics_callback();
    auto writer = [](std::string_view) {
        // discard results;
    };
    auto stats = ss::memory::stats();
    cb(writer);
    auto total_mem = stats.total_memory();
    auto free_mem = stats.free_memory();
    auto failed_allocs = stats.failed_allocations();
    auto used_mem_str = human::bytes(static_cast<double>(total_mem - free_mem));
    auto free_mem_str = human::bytes(static_cast<double>(free_mem));
    auto total_mem_str = human::bytes(static_cast<double>(total_mem));

    const auto expected = fmt::format(
      R"(Dumping seastar memory diagnostics
Used memory:   {used_memory}
Free memory:   {free_memory}
Total memory:  {total_memory}
Hard failures: {hard_failures}

Top-N alloc sites:
)",
      fmt::arg("used_memory", used_mem_str),
      fmt::arg("free_memory", free_mem_str),
      fmt::arg("total_memory", total_mem_str),
      fmt::arg("hard_failures", failed_allocs));

    auto crashes = get_recorder()
                     .get_recorded_crashes(
                       recorder::include_malformed_files::no,
                       recorder::include_current::yes)
                     .get();
    ASSERT_FALSE(crashes.empty());
    ASSERT_EQ(crashes.size(), 1);
    const auto& crash = crashes[0];
    EXPECT_EQ(crash.crash->type, crash_type::oom);
    EXPECT_EQ(crash.crash->crash_message.c_str(), expected);
}
} // namespace crash_tracker
