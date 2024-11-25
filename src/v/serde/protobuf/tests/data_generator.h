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
#pragma once

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor_database.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>

#include <memory>
#include <optional>

namespace testing {

struct protobuf_generator_config {
    std::optional<size_t> elements_in_collection{};
    std::pair<size_t, size_t> string_length_range{0, 32};
    int max_nesting_level{10};
    // Enable randomly not setting values for optional fields in the message.
    bool randomize_optional_fields{false};
};

class protobuf_generator {
public:
    explicit protobuf_generator(protobuf_generator_config c)
      : _config(c) {}

    std::unique_ptr<google::protobuf::Message>
    generate_protobuf_message(const google::protobuf::Descriptor* d) {
        return generate_protobuf_message_impl(1, d);
    }

private:
    std::unique_ptr<google::protobuf::Message> generate_protobuf_message_impl(
      int level, const google::protobuf::Descriptor* d);
    protobuf_generator_config _config;
    google::protobuf::DynamicMessageFactory _factory{};
};
} // namespace testing
