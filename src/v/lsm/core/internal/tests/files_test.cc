/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "lsm/core/internal/files.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {
using namespace lsm::internal;
using ::testing::Optional;
} // namespace

TEST(Files, SstFileName) {
    EXPECT_EQ(
      "00000000000000000001-00000000000000000000.sst",
      std::string(sst_file_name({1_file_id, 0_db_epoch})));
    EXPECT_EQ(
      "00000000000000000123-00000000000000000001.sst",
      std::string(sst_file_name({123_file_id, 1_db_epoch})));
    EXPECT_EQ(
      "00000000000000000000-00000000000000000123.sst",
      std::string(sst_file_name({0_file_id, 123_db_epoch})));
    EXPECT_EQ(
      "18446744073709551615-18446744073709551615.sst",
      std::string(sst_file_name({
        file_id::max(),
        database_epoch::max(),
      })));
}

TEST(Files, ParseFilename_SstFile) {
    EXPECT_THAT(
      parse_sst_file_name(sst_file_name({1_file_id, 2_db_epoch})),
      Optional(file_handle{1_file_id, 2_db_epoch}));

    EXPECT_THAT(
      parse_sst_file_name(sst_file_name({123_file_id, 432_db_epoch})),
      Optional(file_handle{123_file_id, 432_db_epoch}));

    EXPECT_THAT(
      parse_sst_file_name(
        sst_file_name(file_handle{file_id::max(), database_epoch::max()})),
      Optional(file_handle{file_id::max(), database_epoch::max()}));
}

TEST(Files, ParseFilename_Invalid) {
    // No extension
    EXPECT_EQ(std::nullopt, parse_sst_file_name("noextension"));

    // Unknown extension
    EXPECT_EQ(std::nullopt, parse_sst_file_name("file.txt"));
    EXPECT_EQ(std::nullopt, parse_sst_file_name("00000000000000000001.log"));

    // Missing splits
    EXPECT_EQ(std::nullopt, parse_sst_file_name("00000000000000000001.sst"));

    // Extra split
    EXPECT_EQ(
      std::nullopt,
      parse_sst_file_name(
        "00000000000000000001-00000000000000000001-00000000000000000001.sst"));

    // Invalid numeric ID for sst
    EXPECT_EQ(std::nullopt, parse_sst_file_name("notanumber-123.sst"));
    EXPECT_EQ(std::nullopt, parse_sst_file_name("4321-abc123.sst"));

    // Empty string
    EXPECT_EQ(std::nullopt, parse_sst_file_name(""));

    // Just extension
    EXPECT_EQ(std::nullopt, parse_sst_file_name(".sst"));
}
