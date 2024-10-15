/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/protobuf_translator.h"

#include "base/vlog.h"
#include "datalake/conversion_outcome.h"
#include "datalake/logger.h"
#include "datalake/schema_protobuf.h"
#include "datalake/values_protobuf.h"
#include "iceberg/datatypes.h"

// FIXME: Which of these are really needed
#include <google/protobuf/arena.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/unknown_field_set.h>

#include <stdexcept>

namespace datalake {

protobuf_buffer_translator::protobuf_buffer_translator(
  std::string schema, std::vector<int32_t> message_offsets) {
    // FIXME: In error cases, clean up allocated structures.
    google::protobuf::io::ArrayInputStream proto_input_stream(
      schema.c_str(), schema.size());
    // FIXME: second argument here is an error collector. Set this up so we can
    // properly log protobuf errors.
    google::protobuf::io::Tokenizer tokenizer(&proto_input_stream, nullptr);

    google::protobuf::compiler::Parser parser;
    if (!parser.Parse(&tokenizer, &_file_descriptor_proto)) {
        // FIXME: get error message from error collector
        vlog(datalake_log.error, "Unable to parse protobuf schema");
        throw std::runtime_error("Could not parse protobuf schema");
    }

    if (!_file_descriptor_proto.has_name()) {
        vlog(datalake_log.warn, "Protobuf file descriptor name is not set.");
        _file_descriptor_proto.set_name("empty_message_name");
    }

    _file_desc = _protobuf_descriptor_pool.BuildFile(_file_descriptor_proto);
    if (_file_desc == nullptr) {
        // FIXME: use error collector to get more descriptive error message
        vlog(datalake_log.error, "Could not build descriptor pool");
        throw std::runtime_error("Could not build descriptor pool");
    }

    _message_descriptor = get_descriptor(std::move(message_offsets));
}

conversion_outcome<iceberg::struct_type>
protobuf_buffer_translator::get_schema() {
    return type_to_iceberg(*_message_descriptor);
}

ss::future<optional_value_outcome>
protobuf_buffer_translator::translate_value(iobuf buffer) {
    // FIXME: if we fail to parse the buffer we need the result back to make it
    // schemaless
    return deserialize_protobuf(std::move(buffer), *_message_descriptor);
}

const google::protobuf::Descriptor* protobuf_buffer_translator::get_descriptor(
  std::vector<int32_t> message_offsets) {
    // Get top level message descriptor
    if (message_offsets.size() == 0) {
        // We should not get here: an empty offset list defaults to {0}. This is
        // handled by the code that parses the offset list from the message, so
        // by the time we get here there should be at least one element in the
        // list.
        vlog(datalake_log.error, "Message offset list is empty");
        throw std::runtime_error("Empty message offsets");
    }
    if (
      message_offsets[0] < 0
      || message_offsets[0] >= _file_desc->message_type_count()) {
        vlog(
          datalake_log.error,
          "Invalid offset: {}, message type count: {}",
          message_offsets[0],
          _file_desc->message_type_count());
        throw std::runtime_error("Invalid message offset");
    }

    const google::protobuf::Descriptor* msg_desc = nullptr;
    msg_desc = _file_desc->message_type(message_offsets[0]);
    vlog(
      datalake_log.info,
      "Setting message type {}: {}",
      message_offsets[0],
      msg_desc->name());

    // Skip the first element of message_offsets since that's the top-level
    // record that was handled above.
    for (size_t i = 1; i < message_offsets.size(); i++) {
        int32_t offset = message_offsets[i];
        if (offset < 0 || offset >= msg_desc->nested_type_count()) {
            vlog(
              datalake_log.error,
              "Invalid offset: {}, nested type count: {}",
              offset,
              msg_desc->nested_type_count());
            throw std::runtime_error("Invalid message offset");
        }
        msg_desc = msg_desc->nested_type(offset);
        // FIXME: change this to debug
        vlog(
          datalake_log.info,
          "Setting nested type {}: {}",
          offset,
          msg_desc->name());
    }
    return msg_desc;
}

} // namespace datalake
