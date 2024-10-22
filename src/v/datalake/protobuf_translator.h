/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

// TODO: This is only needed for the schema, change schema to appropriate type
// and remove it
#include "datalake/conversion_outcome.h"
#include "iceberg/datatypes.h"

#include <string>
#include <vector>

// TODO: Which of these are really needed
#include <google/protobuf/arena.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/unknown_field_set.h>
#include <gtest/gtest_prod.h>

namespace datalake {

// This file contains two classes:
// 1. protobuf_buffer_translator:
//    Constructed from a protobuf schema and offsets, provides and
//    iceberg::struct_type and converts iobufs into iceberg::value.
// 2. protobuf_record_translator:
//    Constructed from separate schema/offsets for the key and value, it follows
//    the translator interface to convert entire records:
//    translate_record(iobuf key, iobuf val, int64_t timestap, int64_t offset)

class protobuf_buffer_translator {
public:
    // A protobuf type is not uniquely identified by the schema. Each schema can
    // contain multiple messages, and those messages may be nested. So, in
    // addition to the schema, we need a list of offsets within the schema. E.g.
    // the offsets [1, 4] would indicate that we need to look first at the
    // second message type, and the fifth nested message type within that.

    // TODO: modify this to parse schemas from iobuf instead of std::string
    // That will allow us to avoid large allocations for large schemas
    protobuf_buffer_translator(
      std::string schema, std::vector<int32_t> message_offsets);

    conversion_outcome<iceberg::struct_type> get_schema();

    ss::future<optional_value_outcome> translate_value(iobuf buffer);

private:
    FRIEND_TEST(ProtobufTranslatorTest, BufferTranslator);

    google::protobuf::DescriptorPool _protobuf_descriptor_pool;
    google::protobuf::FileDescriptorProto _file_descriptor_proto;
    google::protobuf::DynamicMessageFactory _factory;
    const google::protobuf::FileDescriptor* _file_desc;
    const google::protobuf::Descriptor* _message_descriptor;

    iceberg::struct_type _iceberg_schema;

    const google::protobuf::Descriptor*
    get_descriptor(std::vector<int32_t> message_offsets);
};
} // namespace datalake
