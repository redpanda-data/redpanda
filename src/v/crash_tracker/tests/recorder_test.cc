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
#include "crash_tracker/types.h"
#include "test_utils/tmp_dir.h"
#include "utils.h"

#include <seastar/core/memory.hh>

#include <gtest/gtest.h>

#include <exception>
#include <stdexcept>

namespace crash_tracker {

class RecorderTest : public testing::Test {
public:
    void SetUp() override { _helper.SetUp(); }
    void TearDown() override { _helper.TearDown(); }

private:
    RecorderTestHelper _helper;
};

TEST_F(RecorderTest, TestFileCleanup) {
    const auto test_eptr = std::make_exception_ptr(std::runtime_error{""});

    // Observe no recorded crashes before the first start
    auto crashes = get_test_recorder().get_recorded_crashes().get();
    ASSERT_EQ(crashes.size(), 0);

    // Simulate lots of crashed restarts to generate crash reports on disk
    for (size_t i = 0; i < recorder::crash_files_to_keep + 5; i++) {
        auto rec = get_test_recorder();
        rec.start().get();
        rec.record_crash_exception(test_eptr);
    }

    // Run one more restart and observe that old crash files are cleaned up
    auto rec = get_test_recorder();
    rec.start().get();

    crashes = rec.get_recorded_crashes().get();
    ASSERT_EQ(crashes.size(), recorder::crash_files_to_keep);
}

TEST_F(RecorderTest, TestLargeException) {
    auto large_exception_msg = ss::sstring(5000, 'E');
    const auto test_eptr = std::make_exception_ptr(
      std::runtime_error{large_exception_msg});

    auto rec = get_test_recorder();
    rec.start().get();
    rec.record_crash_exception(test_eptr);

    auto crashes = get_test_recorder().get_recorded_crashes().get();
    ASSERT_EQ(crashes.size(), 1);
    ASSERT_TRUE(crashes[0].crash.has_value());
    ASSERT_EQ(
      crashes[0].crash.value().crash_message.length(),
      crash_description::string_buffer_reserve);
}

TEST_F(RecorderTest, TestUploadMarkers) {
    constexpr auto n_reports = 5;

    // Generate some crash reports
    const auto test_eptr = std::make_exception_ptr(std::runtime_error{""});
    for (size_t i = 0; i < n_reports; i++) {
        auto rec = get_test_recorder();
        rec.start().get();
        rec.record_crash_exception(test_eptr);
    }

    auto rec = get_test_recorder();
    rec.start().get();

    // Verify upload markers work as expected
    auto crashes = rec.get_recorded_crashes().get();
    for (auto& report : crashes) {
        ASSERT_EQ(report.is_uploaded().get(), false);
        report.mark_uploaded().get();
        ASSERT_EQ(report.is_uploaded().get(), true);
    }
}

TEST_F(RecorderTest, TestUninitializedCrashTracker) {
    auto rec = get_test_recorder();

    // Assume that there is an exception while starting the recorder. This could
    // happen for example if the data directory is not writable while the
    // recorder tries to create the crash reports directory.

    // Now simulate that we attempt to record this exception. This should be
    // gracefully skipped, since the crash recorder is not initialized.
    const auto test_eptr = std::make_exception_ptr(
      std::runtime_error{"Test filesystem error"});
    ASSERT_NO_THROW(rec.record_crash_exception(test_eptr));
}

namespace {

size_t count_upload_markers() {
    size_t result = 0;
    for (const auto& entry : std::filesystem::directory_iterator(
           config::node().crash_report_dir_path())) {
        if (entry.path().string().ends_with(recorder::upload_marker_suffix)) {
            result++;
        }
    }
    return result;
}

} // namespace

TEST_F(RecorderTest, TestUploadMarkerCleanup) {
    const auto test_eptr = std::make_exception_ptr(std::runtime_error{""});

    // Simulate lots of crashed restarts to generate crash reports on disk
    for (size_t i = 0; i < recorder::crash_files_to_keep + 5; i++) {
        auto rec = get_test_recorder();
        rec.start().get();

        // Mark all earlier crash reports as uploaded before each crash
        auto crashes = rec.get_recorded_crashes().get();
        for (auto& report : crashes) {
            report.mark_uploaded().get();
        }

        rec.record_crash_exception(test_eptr);
    }
    // Note: the crash report generated in the last iteration was not been
    // marked uploaded

    // Run one more restart and observe that old crash files and crash markers
    // are cleaned up
    auto rec = get_test_recorder();
    rec.start().get();

    // Verify that all but the latest report has been marked uploaded
    auto crashes = rec.get_recorded_crashes().get();
    ASSERT_EQ(crashes.size(), recorder::crash_files_to_keep);
    for (size_t i = 0; i < crashes.size(); ++i) {
        const auto expect_uploaded = (i != (crashes.size() - 1));
        ASSERT_EQ(crashes[i].is_uploaded().get(), expect_uploaded);
    }

    // Verify that there are no dangling upload markers
    ASSERT_EQ(count_upload_markers(), recorder::crash_files_to_keep - 1);
}

class ParametrizedRecorderTest
  : public testing::TestWithParam<recorder::recorded_signo> {
public:
    void SetUp() override { _helper.SetUp(); }
    void TearDown() override { _helper.TearDown(); }

private:
    RecorderTestHelper _helper;
};

TEST_P(ParametrizedRecorderTest, TestNoAlloc) {
    auto rec = get_test_recorder();
    rec.start().get();

    // Note: memory stats are only tracked in release mode
    const auto stats = ss::memory::stats();
    auto mallocs = [&]() {
        return ss::memory::stats().mallocs() - stats.mallocs();
    };

    // Verify that writing out a crash report does not allocate
    rec.record_crash_sighandler(GetParam());
    ASSERT_EQ(mallocs(), 0);
}

INSTANTIATE_TEST_SUITE_P(
  AllCrashSignals,
  ParametrizedRecorderTest,
  testing::Values(
    recorder::recorded_signo::sigsegv,
    recorder::recorded_signo::sigabrt,
    recorder::recorded_signo::sigill));

} // namespace crash_tracker
