#pragma once
#include "datalake/schema_registry_interface.h"
#include "model/record.h"
#include "random/generators.h"

#include <seastar/core/circular_buffer.hh>

#include <compression/compression.h>
#include <google/protobuf/message.h>

#include <cstdlib>
#include <sstream>
#include <string>

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

    static std::string
    generate_schema(int int32_elements, int string_elements) {
        std::stringstream schema;
        schema << R"schema(
        syntax = "proto2";
        package datalake.proto;

        message message_type {

        )schema";

        int elem = 1;

        for (int i = 1; i <= int32_elements; i++) {
            schema << "optional int32 number_" << i << " = " << elem++ << ";\n";
        }

        for (int i = 1; i <= string_elements; i++) {
            schema << "optional string str_" << i << " = " << elem++ << ";\n";
        }

        schema << "}\n";

        return schema.str();
    }

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

    test_message_builder(std::string schema)
      : _proto_input_stream{schema.c_str(), int(schema.size())}
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

    std::string generate_message_generic(
      const std::string& message_type,
      const std::function<void(google::protobuf::Message*)>& populate_message) {
        auto msg = generate_unserialized_message_generic(
          message_type, populate_message);
        std::string ret = msg->SerializeAsString();
        delete msg;
        return ret;
    }

    google::protobuf::FileDescriptorProto _file_descriptor_proto;
    google::protobuf::compiler::Parser _parser;
    google::protobuf::io::ArrayInputStream _proto_input_stream;
    google::protobuf::io::Tokenizer _tokenizer;
    google::protobuf::DescriptorPool _pool;
    const google::protobuf::FileDescriptor* _file_desc;
    google::protobuf::DynamicMessageFactory _factory;
};

inline std::string generate_empty_message() {
    test_data test_data;
    test_message_builder builder(test_data);
    return builder.generate_message_generic(
      "empty_message", [](google::protobuf::Message* message) {});
}

inline std::string
generate_simple_message(const std::string& label, int32_t number) {
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

inline std::string random_string() {
    int min_length = 4;
    int max_length = 11;
    int length = min_length + rand() % (max_length - min_length);
    std::string res;
    for (int i = 0; i < length; i++) {
        res.push_back('a' + rand() % 26);
    }
    return res;
}

inline std::string generate_big_message(const std::string& schema) {
    test_message_builder builder(schema);
    return builder.generate_message_generic(
      "message_type", [&](google::protobuf::Message* message) {
          auto reflection = message->GetReflection();
          // Have to use field indices here because
          // message->GetReflections()->ListFields() only returns fields
          // that are actually present in the message;
          for (int field_idx = 0;
               field_idx < message->GetDescriptor()->field_count();
               field_idx++) {
              auto field_desc = message->GetDescriptor()->field(field_idx);
              if (field_desc->name().starts_with("str_")) {
                  reflection->SetString(message, field_desc, random_string());
              } else if (field_desc->name() == "number_") {
                  reflection->SetInt32(message, field_desc, rand());
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

inline std::string
generate_nested_message(const std::string& label, int32_t number) {
    test_data test_data;
    test_message_builder builder(test_data);
    auto inner = generate_inner_message(builder, "inner: " + label, -number);

    return builder.generate_message_generic(
      "nested_message", [&](google::protobuf::Message* message) {
          auto reflection = message->GetReflection();
          // Have to use field indices here because
          // message->GetReflections()->ListFields() only returns fields
          // thatare actually present in the message;

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
        // return model::test::make_random_batches(
        //   model::offset(0), random_generators::get_int(1, 10), true,
        //   base_ts);
        int count = random_generators::get_int(1, 10);
        bool allow_compression = true;
        model::offset offset(0);
        model::timestamp ts = base_ts.value_or(model::timestamp::now());

        ss::circular_buffer<model::record_batch> ret;
        ret.reserve(count);
        for (int i = 0; i < count; i++) {
            // TODO: See todo comment in make_random_batches
            auto b = make_protobuf_batch(offset, allow_compression, ts);
            offset = b.last_offset() + model::offset(1);
            b.set_term(model::term_id(0));
            ret.push_back(std::move(b));
        }
        return ret;
    }

    model::record_batch make_protobuf_batch(
      model::offset offset, bool allow_compression, model::timestamp ts) {
        // Based on make_random_batch, skip the transaction and idempotence
        // stuff.
        auto num_records = random_generators::get_int(1, 30);
        auto max_ts = model::timestamp(ts() + num_records - 1);
        auto header = model::record_batch_header{
          .size_bytes = 0, // computed later
          .base_offset = offset,
          .type = model::record_batch_type::raft_data,
          .crc = 0, // we-reassign later
          .attrs = model::record_batch_attributes(
            random_generators::get_int<int16_t>(0, allow_compression ? 4 : 0)),
          .last_offset_delta = num_records - 1,
          .first_timestamp = ts,
          .max_timestamp = max_ts,
          .producer_id = -1,
          .producer_epoch = -1,
          .base_sequence = 0,
          .record_count = num_records};

        auto size = model::packed_record_batch_header_size;
        model::record_batch::records_type records;
        auto rs = model::record_batch::uncompressed_records();
        rs.reserve(num_records);
        for (int i = 0; i < num_records; i++) {
            std::stringstream key_stream;
            key_stream << i;

            std::string value = generate_simple_message(
              key_stream.str() + " bottles of beer on the wall", i);
            rs.emplace_back(
              make_record(offset() + i, ts() + i, key_stream.str(), value));
        }

        if (header.attrs.compression() != model::compression::none) {
            iobuf body;
            for (auto& r : rs) {
                model::append_record_to_buffer(body, r);
            }
            rs.clear();
            records = compression::compressor::compress(
              body, header.attrs.compression());
            size += std::get<iobuf>(records).size_bytes();
        } else {
            for (auto& r : rs) {
                size += r.size_bytes();
                size += vint::vint_size(r.size_bytes());
            }
            records = std::move(rs);
        }
        // TODO: expose term setting
        header.ctx = model::record_batch_header::context(
          model::term_id(0), ss::this_shard_id());
        header.size_bytes = size;
        auto batch = model::record_batch(header, std::move(records));
        batch.header().crc = model::crc_record_batch(batch);
        batch.header().header_crc = model::internal_header_only_crc(
          batch.header());
        return batch;
    }

    model::record make_record(
      int ts_delta,
      int offset_delta,
      std::string key_string,
      std::string value_string) {
        iobuf key;
        iobuf value;
        key.append(key_string.data(), key_string.size());
        value.append(value_string.data(), value_string.size());
        // Headers in the storages tests are just random. We can leave them
        // empty.
        std::vector<model::record_header> headers;

        auto size = sizeof(model::record_attributes::type) // attributes
                    + vint::vint_size(ts_delta)            // timestamp delta
                    + vint::vint_size(offset_delta)        // offset delta
                    + vint::vint_size(key.size_bytes())    // size of key-len
                    + key.size_bytes()                     // size of key
                    + vint::vint_size(value.size_bytes())  // size of value
                    + value.size_bytes() // size of value (includes lengths)
                    + vint::vint_size(headers.size());
        for (auto& h : headers) {
            size += vint::vint_size(h.key_size()) + h.key().size_bytes()
                    + vint::vint_size(h.value_size()) + h.value().size_bytes();
        }
        return {
          static_cast<int32_t>(size),
          model::record_attributes(0),
          ts_delta,
          offset_delta,
          static_cast<int32_t>(key.size_bytes()),
          std::move(key),
          static_cast<int32_t>(value.size_bytes()),
          std::move(value),
          std::move(headers)};
    }
};
