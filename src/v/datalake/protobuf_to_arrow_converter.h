/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once
#include "bytes/iobuf.h"
#include "datalake/errors.h"
#include "datalake/logger.h"
#include "datalake/proto_to_arrow_interface.h"
#include "datalake/proto_to_arrow_scalar.h"
#include "datalake/proto_to_arrow_struct.h"

#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/tokenizer.h>

#include <memory>
#include <stdexcept>

namespace datalake {

struct error_collector : ::google::protobuf::io::ErrorCollector {
    void AddError(int line, int column, const std::string& message) override;
    void AddWarning(int line, int column, const std::string& message) override;
};

/** Top-level interface for parsing Protobuf messages to an Arrow table

This class deserializes protobuf messages and passes the deserialized messages
to an instance of proto_to_arrow_struct to recursively parse the structured
message.
*/
class proto_to_arrow_converter {
public:
    explicit proto_to_arrow_converter(const ss::sstring& schema);

    [[nodiscard]] arrow_converter_status
    add_message(iobuf&& serialized_message);

    [[nodiscard]] arrow_converter_status finish_batch();

    std::shared_ptr<arrow::Table> build_table();

    std::shared_ptr<arrow::Schema> build_schema();

private:
    FRIEND_TEST(ArrowWriter, EmptyMessageTest);
    FRIEND_TEST(ArrowWriter, SimpleMessageTest);
    FRIEND_TEST(ArrowWriter, NestedMessageTest);
    FRIEND_TEST(ArrowWriter, InvalidMessagetest);

    [[nodiscard]] arrow_converter_status
    initialize_protobuf_schema(const ss::sstring& schema);

    [[nodiscard]] arrow_converter_status initialize_struct_converter();

    /// Parse the message to a protobuf message.
    /// Return nullptr on error.
    // TODO: Add a version of this method that takes an iobuf
    // and creates a `google::protobuf::io::ZeroCopyInputStream`.
    // We can then call ParseFromBoundedZeroCopyStream
    std::unique_ptr<google::protobuf::Message> parse_message(iobuf&& message);
    const google::protobuf::Descriptor* message_descriptor();

private:
    google::protobuf::DescriptorPool _protobuf_descriptor_pool;
    google::protobuf::FileDescriptorProto _file_descriptor_proto;
    google::protobuf::DynamicMessageFactory _factory;
    const google::protobuf::FileDescriptor* _file_desc{};

    std::unique_ptr<detail::proto_to_arrow_struct> _struct_converter;
    error_collector error_collector;
};

} // namespace datalake
