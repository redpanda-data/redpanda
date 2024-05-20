// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/random.h"
#include "container/fragmented_vector.h"
#include "storage/mvlog/file.h"
#include "test_utils/gtest_utils.h"
#include "test_utils/test.h"

#include <seastar/core/seastar.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/file.hh>

#include <gtest/gtest.h>

#include <exception>
#include <filesystem>

using namespace storage::experimental::mvlog;
using namespace experimental;

class FileTest : public seastar_test {
public:
    FileTest()
      : base_dir_(get_test_directory())
      , file_mgr_(std::make_unique<file_manager>()) {}

    ss::future<> SetUpAsync() override {
        co_await ss::recursive_touch_directory(base_dir_);
    }

    ss::future<> TearDownAsync() override {
        for (auto& file : files_) {
            co_await file->close();
        }
        // NOTE: death tests may clean themselves up in the test body to avoid
        // leaving behind directories from child processes.
        if (co_await ss::file_exists(base_dir_)) {
            co_await ss::recursive_remove_directory(
              std::filesystem::path{base_dir_});
        }
    }

    std::filesystem::path test_path(ss::sstring component) {
        return {fmt::format("{}/{}", base_dir_, component)};
    }

    // Creates the file and tracks for close.
    ss::future<file*> tracked_create_file(std::filesystem::path path) {
        auto fut = co_await ss::coroutine::as_future(
          file_mgr_->create_file(path));
        if (fut.failed()) {
            std::rethrow_exception(fut.get_exception());
        }
        auto f = std::move(fut.get());
        auto* ret = f.get();
        files_.emplace_back(std::move(f));
        co_return ret;
    }

    // Opens the file and tracks for close.
    ss::future<file*> tracked_open_file(std::filesystem::path path) {
        auto fut = co_await ss::coroutine::as_future(
          file_mgr_->open_file(path));
        if (fut.failed()) {
            std::rethrow_exception(fut.get_exception());
        }
        auto f = std::move(fut.get());
        auto* ret = f.get();
        files_.emplace_back(std::move(f));
        co_return ret;
    }

protected:
    const ss::sstring base_dir_;
    std::unique_ptr<file_manager> file_mgr_;
    chunked_vector<std::unique_ptr<file>> files_;
};

TEST_F_CORO(FileTest, TestCreate) {
    const auto path = test_path("foo");
    auto created = co_await tracked_create_file(path);
    ASSERT_EQ_CORO(created->filepath(), path);

    // Shouldn't be able to create the file if it already exists.
    ASSERT_THROW_CORO(
      co_await tracked_create_file(path), std::filesystem::filesystem_error);
}

TEST_F_CORO(FileTest, TestOpen) {
    const auto path = test_path("foo");
    ASSERT_THROW_CORO(
      co_await tracked_open_file(path), std::filesystem::filesystem_error);

    auto created = co_await tracked_create_file(path);
    auto opened = co_await tracked_open_file(path);

    // The file manager isn't smart enough to differentiate multiple handles to
    // the same file.
    ASSERT_NE_CORO(created, opened);

    // But they should indeed be pointing at the same file.
    ASSERT_EQ_CORO(created->filepath(), path);
    ASSERT_EQ_CORO(opened->filepath(), path);
}

TEST_F_CORO(FileTest, TestRemove) {
    const auto path = test_path("foo");
    auto created = co_await tracked_create_file(path);
    ASSERT_EQ_CORO(created->filepath(), path);

    auto path_str = path.string();
    ASSERT_TRUE_CORO(co_await ss::file_exists(path_str));
    co_await file_mgr_->remove_file(path);

    ASSERT_FALSE_CORO(co_await ss::file_exists(path_str));
}

TEST_F_CORO(FileTest, TestAppend) {
    const auto path = test_path("foo");
    auto created = co_await tracked_create_file(path);

    iobuf expected_buf;
    for (int i = 0; i < 10; i++) {
        auto buf = random_generators::make_iobuf();
        co_await created->append(buf.copy());
        expected_buf.append(std::move(buf));
        ASSERT_EQ_CORO(created->size(), expected_buf.size_bytes());

        // Read the file and check that it matches what has been appended.
        iobuf actual_stream_buf;
        auto stream = created->make_stream(0, created->size());
        actual_stream_buf.append(co_await stream.read_exactly(created->size()));

        ASSERT_EQ_CORO(actual_stream_buf, expected_buf) << fmt::format(
          "{}\nvs\n{}",
          actual_stream_buf.hexdump(1024),
          expected_buf.hexdump(1024));
    }
}

using FileDeathTest = FileTest;
TEST_F(FileDeathTest, TestAppendAfterClose) {
    const auto path = test_path("foo");
    auto created = file_mgr_->create_file(path).get();
    created->close().get();

    // Clean up the files in the test body. Death tests may not run teardown.
    ss::recursive_remove_directory(std::filesystem::path{base_dir_}).get();

    auto buf = random_generators::make_iobuf();
    ASSERT_DEATH(created->append(buf.copy()).get(), "file has been closed");
}

TEST_F(FileDeathTest, TestStreamAfterClose) {
    const auto path = test_path("foo");
    auto created = file_mgr_->create_file(path).get();
    auto buf = random_generators::make_iobuf();
    created->append(buf.copy()).get();
    created->close().get();

    // Clean up the files in the test body. Death tests may not run teardown.
    ss::recursive_remove_directory(std::filesystem::path{base_dir_}).get();

    ASSERT_DEATH(
      created->make_stream(0, created->size()), "file has been closed");
}

TEST_F_CORO(FileTest, TestReadStream) {
    const auto path = test_path("foo");
    auto file = co_await tracked_create_file(path);
    const auto num_appends = 10;
    const auto append_len = 10;
    const auto file_size = append_len * num_appends;
    iobuf expected_full;
    for (int i = 0; i < num_appends; i++) {
        auto buf = random_generators::make_iobuf(append_len);
        co_await file->append(buf.copy());
        expected_full.append(std::move(buf));
    }
    ASSERT_EQ_CORO(file->size(), file_size);

    // Do an exhaustive sweep of ranges in the file.
    for (int start = 0; start < file_size; start++) {
        for (int end = start; end < file_size; end++) {
            const auto len = end - start;
            auto str = file->make_stream(start, len);
            iobuf actual_stream_buf;
            actual_stream_buf.append(co_await str.read_exactly(len));
            ASSERT_EQ_CORO(actual_stream_buf.size_bytes(), len);

            auto expected_buf = expected_full.share(start, len);
            ASSERT_EQ_CORO(actual_stream_buf, expected_buf) << fmt::format(
              "{}\nvs\n{}",
              actual_stream_buf.hexdump(1024),
              expected_buf.hexdump(1024));
        }
    }
}

TEST_F_CORO(FileTest, TestReadStreamNearEnd) {
    const auto path = test_path("foo");
    auto file = co_await tracked_create_file(path);
    const auto file_size = 128;
    auto buf = random_generators::make_iobuf(file_size);
    co_await file->append(buf.copy());
    ASSERT_EQ_CORO(file->size(), file_size);

    for (int i = 0; i < 5; i++) {
        // All reads starting past the end should be empty.
        auto past_end_str = file->make_stream(file_size, i);
        auto read_buf = co_await past_end_str.read();
        ASSERT_TRUE_CORO(read_buf.empty());
    }

    for (int i = 0; i < 5; i++) {
        // Reads ending past the end should be truncated.
        auto len_past_end = i + 10;
        auto past_end_str = file->make_stream(file_size - i, len_past_end);
        auto read_buf = co_await past_end_str.read_exactly(len_past_end);

        // The resulting read should only read to the end of what is appended.
        ASSERT_EQ_CORO(read_buf.size(), i);
    }
}
