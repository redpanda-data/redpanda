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

#include "container/fragmented_vector.h"
#include "debug_bundle/metadata.h"
#include "utils/uuid.h"

#include <seastar/util/process.hh>

#include <gtest/gtest.h>

#include <variant>

namespace db = debug_bundle;

TEST(metadata_test, test_serialize_deserialize) {
    using namespace std::chrono_literals;
    auto test_time = db::clock::now();
    auto stop_time = test_time + 1s;
    db::job_id_t job_id{uuid_t::create()};
    std::filesystem::path path{"/tmp/debug.zip"};
    std::filesystem::path process_output_path{"/tmp/debug.out"};
    bytes test_bytes{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    ss::experimental::process::wait_status status
      = ss::experimental::process::wait_exited{1};

    db::metadata m{
      test_time,
      stop_time,
      job_id,
      path,
      process_output_path,
      test_bytes,
      status};

    iobuf buf;
    serde::write(buf, std::move(m));

    iobuf_parser parser(std::move(buf));

    auto m2 = serde::read<db::metadata>(parser);

    EXPECT_EQ(
      m2.created_at,
      std::chrono::duration_cast<std::chrono::microseconds>(
        test_time.time_since_epoch())
        .count());
    EXPECT_EQ(m2.get_created_at(), test_time);
    EXPECT_EQ(m2.get_finished_at(), stop_time);
    EXPECT_EQ(m2.job_id, job_id);
    EXPECT_EQ(m2.debug_bundle_file_path, path);
    EXPECT_EQ(m2.process_output_file_path, process_output_path);
    EXPECT_EQ(m2.sha256_checksum, test_bytes);
    auto test_status = m2.get_wait_status();
    ASSERT_TRUE(std::holds_alternative<ss::experimental::process::wait_exited>(
      test_status));
    EXPECT_EQ(
      std::get<ss::experimental::process::wait_exited>(test_status).exit_code,
      std::get<ss::experimental::process::wait_exited>(status).exit_code);
}
