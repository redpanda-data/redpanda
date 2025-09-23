// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "absl/container/flat_hash_set.h"
#include "bytes/iobuf_parser.h"
#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/test/compatibility_common.h"
#include "pandaproxy/schema_registry/test/protobuf_utils.h"
#include "pandaproxy/schema_registry/types.h"
#include "test_utils/runfiles.h"
#include "utils/file_io.h"

#include <fmt/core.h>
#include <gtest/gtest.h>

#include <utility>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;
namespace ppstu = pp::schema_registry::test_utils;

namespace {

std::filesystem::path test_dir() {
    return test_utils::get_runfile_path(
      "src/v/pandaproxy/schema_registry/test/testdata/protobuf");
}

enum class SchemaType { input, sanitized, normalized };

std::string_view to_string_view(const SchemaType st) {
    switch (st) {
    case SchemaType::input:
        return "input";
    case SchemaType::sanitized:
        return "sanitized";
    case SchemaType::normalized:
        return "normalized";
    }
}

ss::sstring read_schema(const std::string_view test_case, const SchemaType pt) {
    const auto proto_file = fmt::format(
      "{}_{}.proto", test_case, to_string_view(pt));
    const auto proto_path = test_dir() / proto_file;
    return read_fully_to_string(proto_path).get();
};

} // namespace

class ProtoRendering : public testing::TestWithParam<std::string> {};

TEST_P(ProtoRendering, test_protobuf_rendering) {
    const auto& test_case = GetParam();
    const auto input = read_schema(test_case, SchemaType::input);

    const auto sanitized_expected = read_schema(
      test_case, SchemaType::sanitized);
    const auto sanitized_processed = ppstu::sanitize(input);
    EXPECT_EQ(sanitized_expected, sanitized_processed);

    const auto normalized_expected = read_schema(
      test_case, SchemaType::normalized);
    const auto normalized_processed = ppstu::normalize(input);
    EXPECT_EQ(normalized_expected, normalized_processed);

    // These are to verify that the processed schemas are valid schemas.
    // We don't want to run these if the above fail because
    // they produce lot's of visual noise.
    if (sanitized_expected == sanitized_processed) {
        EXPECT_EQ(sanitized_processed, ppstu::sanitize(sanitized_processed));
    }
    if (normalized_expected == normalized_processed) {
        EXPECT_EQ(normalized_processed, ppstu::normalize(normalized_processed));
    }
}

TEST_P(ProtoRendering, test_protobuf_serialized_mode) {
    const auto& test_case = GetParam();
    const auto input = read_schema(test_case, SchemaType::sanitized);

    // Validate that a round trip to serialized and
    // back results in the starting schema
    auto schema_b64 = ppstu::sanitize(
      input, pps::normalize::no, pps::output_format::serialized);
    const auto schema_text = ppstu::sanitize(schema_b64, pps::normalize::no);
    EXPECT_EQ(input, schema_text);
}

INSTANTIATE_TEST_SUITE_P(
  Protobuf,
  ProtoRendering,
  testing::Values(
    "empty_proto",
    "syntax_proto2",
    "syntax_proto3",
    "imports",
    "package",
    "file_options",
    "enum_proto2",
    "enum_proto3",
    // These are covering message and field
    "message_proto2",
    "message_proto3",
    "service_proto2",
    "service_proto3",
    "extension_ranges",
    "composite_extension",
    "nested_extension",
    "map_entry_hack",
    "repeated_custom_options",
    "enum_ordering",
    "message_ordering",
    "service_ordering",
    "oneof_ordering"));
