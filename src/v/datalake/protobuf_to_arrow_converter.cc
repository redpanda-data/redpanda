/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/protobuf_to_arrow_converter.h"

#include "base/vlog.h"
#include "bytes/streambuf.h"
#include "datalake/errors.h"
#include "datalake/logger.h"
#include "datalake/proto_to_arrow_struct.h"

#include <arrow/api.h>
#include <arrow/array/builder_base.h>
#include <arrow/array/builder_nested.h>
#include <arrow/chunked_array.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <google/protobuf/arena.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/unknown_field_set.h>

#include <memory>
#include <stdexcept>

namespace datalake {

proto_to_arrow_converter::proto_to_arrow_converter(const ss::sstring& schema) {
    // TODO: Most of the complex logic should go in a factory method.
    auto status = initialize_protobuf_schema(schema);
    if (status == arrow_converter_status::ok) {
        status = initialize_struct_converter();
    }
    switch (status) {
    case arrow_converter_status::ok:
        break;
    case arrow_converter_status::parse_error:
        throw initialization_error("Unable to parse protobuf schema");
        break;
    case arrow_converter_status::internal_error:
        throw initialization_error(
          "Internal error constructing proto_to_arrow_converter");
        break;
    }
}

arrow_converter_status
proto_to_arrow_converter::add_message(iobuf&& serialized_message) {
    std::unique_ptr<google::protobuf::Message> message = parse_message(
      std::move(serialized_message));
    if (message == nullptr) {
        return arrow_converter_status::parse_error;
    }

    if (!_struct_converter->add_struct_message(message.get()).ok()) {
        return arrow_converter_status::internal_error;
    }
    return arrow_converter_status::ok;
}

arrow_converter_status proto_to_arrow_converter::finish_batch() {
    if (!_struct_converter->finish_chunk().ok()) {
        return arrow_converter_status::internal_error;
    }
    return arrow_converter_status::ok;
}
std::shared_ptr<arrow::Table> proto_to_arrow_converter::build_table() {
    auto table_result = arrow::Table::FromChunkedStructArray(
      _struct_converter->take_row_group());
    if (table_result.ok()) {
        return table_result.ValueUnsafe();
    } else {
        return nullptr;
    }
}

std::shared_ptr<arrow::Schema> proto_to_arrow_converter::build_schema() {
    return arrow::schema(_struct_converter->get_field_vector());
}

arrow_converter_status proto_to_arrow_converter::initialize_protobuf_schema(
  const ss::sstring& schema) {
    google::protobuf::io::ArrayInputStream proto_input_stream(
      schema.c_str(), static_cast<int>(schema.size()));
    google::protobuf::io::Tokenizer tokenizer(
      &proto_input_stream, &error_collector);

    google::protobuf::compiler::Parser parser;
    parser.RecordErrorsTo(&error_collector);
    if (!parser.Parse(&tokenizer, &_file_descriptor_proto)) {
        return arrow_converter_status::parse_error;
    }

    if (!_file_descriptor_proto.has_name()) {
        _file_descriptor_proto.set_name("DefaultMessageName");
    }

    _file_desc = _protobuf_descriptor_pool.BuildFile(_file_descriptor_proto);
    if (_file_desc == nullptr) {
        return arrow_converter_status::internal_error;
    }
    return arrow_converter_status::ok;
}
arrow_converter_status proto_to_arrow_converter::initialize_struct_converter() {
    const google::protobuf::Descriptor* message_desc = message_descriptor();
    if (!message_desc) {
        return arrow_converter_status::internal_error;
    }

    _struct_converter = std::make_unique<detail::proto_to_arrow_struct>(
      message_desc);
    if (!_struct_converter) {
        return arrow_converter_status::internal_error;
    }

    return arrow_converter_status::ok;
}
std::unique_ptr<google::protobuf::Message>
proto_to_arrow_converter::parse_message(iobuf&& message) {
    // Get the message descriptor
    const google::protobuf::Descriptor* message_desc = message_descriptor();
    if (message_desc == nullptr) {
        return nullptr;
    }

    const google::protobuf::Message* prototype_msg = _factory.GetPrototype(
      message_desc);
    if (prototype_msg == nullptr) {
        return nullptr;
    }

    std::unique_ptr<google::protobuf::Message> mutable_msg
      = std::unique_ptr<google::protobuf::Message>(prototype_msg->New());
    if (mutable_msg == nullptr) {
        return nullptr;
    }

    iobuf_istream is{std::move(message)};
    if (!mutable_msg->ParseFromIstream(&is.istream())) {
        datalake_log.debug(
          "Failed to parse protobuf message. Protobuf error string: \"{}\"",
          mutable_msg->InitializationErrorString());
        return nullptr;
    }
    return mutable_msg;
}

const google::protobuf::Descriptor*
datalake::proto_to_arrow_converter::message_descriptor() {
    // A Protobuf schema can contain multiple message types.
    // This code assumes the first message type defined in the file
    // is the main one. (In Kafka, this is considered the common case and
    // optimized)
    // TODO: parse message descriptor path from message
    // https://github.com/confluentinc/confluent-kafka-go/blob/b6a3254310f6d9707f283f0b53ef4d3e1ff48e3b/schemaregistry/serde/protobuf/protobuf.go#L437-L456
    int message_type_count = _file_desc->message_type_count();
    if (message_type_count == 0) {
        // there are no message descriptors in the protobuf schema
        return nullptr;
    }
    return _file_desc->message_type(0);
}

#if PROTOBUF_VERSION < 5027000
void error_collector::AddError(
  int line, int column, const std::string& message) {
    // Warning level because this is an error in the input, not Redpanda
    // itself.
    vlog(datalake_log.warn, "Protobuf Error {}:{} {}", line, column, message);
}
void error_collector::AddWarning(
  int line, int column, const std::string& message) {
    vlog(datalake_log.warn, "Protobuf Warning {}:{} {}", line, column, message);
}
#else
void error_collector::RecordError(
  int line, int column, std::string_view message) {
    // Warning level because this is an error in the input, not Redpanda
    // itself.
    vlog(datalake_log.warn, "Protobuf Error {}:{} {}", line, column, message);
}

void error_collector::RecordWarning(
  int line, int column, std::string_view message) {
    vlog(datalake_log.warn, "Protobuf Warning {}:{} {}", line, column, message);
}
#endif
} // namespace datalake
