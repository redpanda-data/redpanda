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

#include "bytes/iobuf_parser.h"
#include "serde/parquet/file_io.h"

#include <gtest/gtest.h>

using namespace serde::parquet;

TEST(FileIo, IobufReadRange) {
    auto data = iobuf::from("hello world parquet");
    auto file_size = static_cast<int64_t>(data.size_bytes());
    iobuf_file_io io(std::move(data));
    EXPECT_EQ(io.size(), file_size);
    auto result = io.read(6, 5).get();
    iobuf_parser parser(std::move(result));
    EXPECT_EQ(parser.read_string_unsafe(5), "world");
}

TEST(FileIo, IobufReadFullFile) {
    auto data = iobuf::from("complete");
    iobuf_file_io io(std::move(data));
    auto result = io.read(0, io.size()).get();
    EXPECT_EQ(result.size_bytes(), 8);
}

TEST(FileIo, IobufReadTail) {
    auto data = iobuf::from("abcdefghPAR1");
    iobuf_file_io io(std::move(data));
    auto tail = io.read(io.size() - 4, 4).get();
    iobuf_parser parser(std::move(tail));
    EXPECT_EQ(parser.read_string_unsafe(4), "PAR1");
}
