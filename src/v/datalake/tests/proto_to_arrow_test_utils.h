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
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/circular_buffer.hh>

#include <compression/compression.h>
#include <google/protobuf/message.h>

#include <cstdlib>
#include <sstream>
#include <string>

namespace datalake {
// This is here for now to split up the stack of commits into separate PRs
struct schema_info {
    std::string schema;
};
} // namespace datalake

struct test_data {
    std::string empty_schema = (R"schema(
      syntax = "proto2";
package datalake.proto;

message empty_message {}
    )schema");

    std::string simple_schema = (R"schema(
      syntax = "proto2";
package datalake.proto;

message simple_message {
  optional string label = 1;
  optional int32 number = 3;
  optional int64 big_number = 4;
  optional float float_number = 5;
  optional double double_number = 6;
  optional bool true_or_false = 7;
}
    )schema");

    std::string nested_schema = (R"schema(
syntax = "proto2";
package datalake.proto;

message nested_message {
  optional string label = 1;
  optional int32 number = 2;
  optional inner_message_t inner_message = 3;
}

message inner_message_t {
  optional string inner_label = 1;
  optional int32 inner_number = 2;
}

)schema");

    std::string combined_schema = (R"schema(
syntax = "proto2";
package datalake.proto;

message empty_message {}

message simple_message {
  optional string label = 1;
  optional int32 number = 3;
  optional int64 big_number = 4;
  optional float float_number = 5;
  optional double double_number = 6;
  optional bool true_or_false = 7;
}

message inner_message_t {
  optional string inner_label = 1;
  optional int32 inner_number = 2;
}

message nested_message {
  optional string label = 1;
  optional int32 number = 2;
  optional inner_message_t inner_message = 3;
}
)schema");

    std::string test_message_name = "simple_message";
};

// TODO: Make this a fixture?
struct test_message_builder {
    test_message_builder(test_data data)
      : _proto_input_stream{data.combined_schema.c_str(), int(data.combined_schema.size())}
      , _tokenizer{&_proto_input_stream, nullptr} {
        if (!_parser.Parse(&_tokenizer, &_file_descriptor_proto)) {
            exit(-1);
        }

        if (!_file_descriptor_proto.has_name()) {
            _file_descriptor_proto.set_name("proto_file");
        }

        // Build a descriptor pool
        _file_desc = _pool.BuildFile(_file_descriptor_proto);
        assert(_file_desc != nullptr);
    }

    google::protobuf::Message* generate_unserialized_message_generic(
      const std::string& message_type,
      const std::function<void(google::protobuf::Message*)>& populate_message) {
        // Get the message descriptor
        const google::protobuf::Descriptor* message_desc
          = _file_desc->FindMessageTypeByName(message_type);
        assert(message_desc != nullptr);

        // Parse the actual message
        const google::protobuf::Message* prototype_msg = _factory.GetPrototype(
          message_desc);
        assert(prototype_msg != nullptr);

        google::protobuf::Message* mutable_msg = prototype_msg->New();
        assert(mutable_msg != nullptr);
        populate_message(mutable_msg);

        return mutable_msg;
    }

    iobuf generate_message_generic(
      const std::string& message_type,
      const std::function<void(google::protobuf::Message*)>& populate_message) {
        auto msg = generate_unserialized_message_generic(
          message_type, populate_message);
        std::string ret = msg->SerializeAsString();
        delete msg;
        return iobuf::from(ret);
    }

    google::protobuf::FileDescriptorProto _file_descriptor_proto;
    google::protobuf::compiler::Parser _parser;
    google::protobuf::io::ArrayInputStream _proto_input_stream;
    google::protobuf::io::Tokenizer _tokenizer;
    google::protobuf::DescriptorPool _pool;
    const google::protobuf::FileDescriptor* _file_desc;
    google::protobuf::DynamicMessageFactory _factory;
};

inline iobuf generate_empty_message() {
    test_data test_data;
    test_message_builder builder(test_data);
    return builder.generate_message_generic(
      "empty_message", [](google::protobuf::Message* message) {});
}

iobuf generate_simple_message(const std::string& label, int32_t number) {
    test_data test_data;
    test_message_builder builder(test_data);
    return builder.generate_message_generic(
      "simple_message", [&](google::protobuf::Message* message) {
          auto reflection = message->GetReflection();
          // Have to use field indices here because
          // message->GetReflections()->ListFields() only returns fields
          // that are actually present in the message;
          for (int field_idx = 0;
               field_idx < message->GetDescriptor()->field_count();
               field_idx++) {
              auto field_desc = message->GetDescriptor()->field(field_idx);
              if (field_desc->name() == "label") {
                  reflection->SetString(message, field_desc, label);
              } else if (field_desc->name() == "number") {
                  reflection->SetInt32(message, field_desc, number);
              } else if (field_desc->name() == "big_number") {
                  reflection->SetInt64(message, field_desc, 10 * number);
              } else if (field_desc->name() == "float_number") {
                  reflection->SetFloat(message, field_desc, number / 10.0);
              } else if (field_desc->name() == "double_number") {
                  reflection->SetDouble(message, field_desc, number / 100.0);
              } else if (field_desc->name() == "true_or_false") {
                  reflection->SetBool(message, field_desc, number % 2 == 0);
              }
          }
      });
}

inline google::protobuf::Message* generate_inner_message(
  test_message_builder& builder, const std::string& label, int32_t number) {
    auto ret = builder.generate_unserialized_message_generic(
      "inner_message_t", [&](google::protobuf::Message* message) {
          auto reflection = message->GetReflection();
          // Have to use field indices here because
          // message->GetReflections()->ListFields() only returns fields
          // that are actually present in the message;
          for (int field_idx = 0;
               field_idx < message->GetDescriptor()->field_count();
               field_idx++) {
              auto field_desc = message->GetDescriptor()->field(field_idx);
              if (field_desc->name() == "inner_label") {
                  reflection->SetString(message, field_desc, label);
              } else if (field_desc->name() == "inner_number") {
                  reflection->SetInt32(message, field_desc, number);
              }
          }
      });

    return ret;
}

iobuf generate_nested_message(const std::string& label, int32_t number) {
    test_data test_data;
    test_message_builder builder(test_data);
    auto inner = generate_inner_message(builder, "inner: " + label, -number);

    return builder.generate_message_generic(
      "nested_message", [&](google::protobuf::Message* message) {
          auto reflection = message->GetReflection();
          // Have to use field indices here because
          // message->GetReflections()->ListFields() only returns fields
          // that are actually present in the message;

          for (int field_idx = 0;
               field_idx < message->GetDescriptor()->field_count();
               field_idx++) {
              auto field_desc = message->GetDescriptor()->field(field_idx);
              if (field_desc->name() == "label") {
                  reflection->SetString(message, field_desc, label);
              } else if (field_desc->name() == "number") {
                  reflection->SetInt32(message, field_desc, number);
              } else if (field_desc->name() == "inner_message") {
                  reflection->SetAllocatedMessage(message, inner, field_desc);
              }
          }
      });
}

inline datalake::schema_info get_test_schema() {
    test_data data;
    return {
      .schema = data.nested_schema,
    };
}

struct protobuf_random_batches_generator {
    ss::circular_buffer<model::record_batch>
    operator()(std::optional<model::timestamp> base_ts = std::nullopt) {
        int count = random_generators::get_int(1, 10);
        // bool allow_compression = true;
        model::offset offset(0);
        // model::timestamp ts = base_ts.value_or(model::timestamp::now());

        ss::circular_buffer<model::record_batch> ret;
        ret.reserve(count);
        for (int i = 0; i < count; i++) {
            // TODO: See todo comment in make_random_batches
            // auto b = make_protobuf_batch(offset, allow_compression, ts);
            auto b = make_protobuf_batch(offset);
            offset = b.last_offset() + model::offset(1);
            b.set_term(model::term_id(0));
            ret.push_back(std::move(b));
        }
        return ret;
    }

    model::record_batch make_protobuf_batch(model::offset offset) {
        auto num_records = random_generators::get_int(1, 30);
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, offset);
        for (int i = 0; i < num_records; i++) {
            std::stringstream key_stream;
            key_stream << i;

            iobuf value = generate_simple_message(
              "message # " + key_stream.str(), i);
            builder.add_raw_kv(iobuf::from(key_stream.str()), std::move(value));
        }
        return std::move(builder).build();
    }
};
