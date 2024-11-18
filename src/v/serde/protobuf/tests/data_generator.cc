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

#include "serde/protobuf/tests/data_generator.h"

#include "random/generators.h"

#include <cstdint>
#include <memory>

namespace testing {
ss::sstring random_string(const protobuf_generator_config& config) {
    auto [min, max] = config.string_length_range;
    return random_generators::gen_alphanum_string(
      random_generators::get_int(min, max));
}

std::unique_ptr<google::protobuf::Message>
protobuf_generator::generate_protobuf_message_impl(
  int level, const google::protobuf::Descriptor* d) {
    auto& factory = _factory;
    auto get_elements_count = [level, this]() -> size_t {
        if (level >= _config.max_nesting_level) {
            return 0;
        }
        return _config.elements_in_collection.value_or(
          random_generators::get_int<size_t>(10));
    };

    auto message = std::unique_ptr<google::protobuf::Message>(
      factory.GetPrototype(d)->New());
    const google::protobuf::Reflection* r = message->GetReflection();
    for (int i = 0; i < d->field_count(); i++) {
        const auto* field_d = d->field(i);

        // if its a union type select a random field in it, randomize its value,
        // then jump to the end of all fields in the oneof.
        if (auto oneof_d = field_d->real_containing_oneof()) {
            auto num_fields = oneof_d->field_count();
            field_d = d->field(
              random_generators::get_int(i, i + (num_fields - 1)));
            i += (num_fields - 1);

            if (
              _config.randomize_optional_fields && field_d->is_optional()
              && random_generators::get_int(0, 1) == 1) {
                continue;
            }
        }

        using fdns = google::protobuf::FieldDescriptor;

        if (field_d->is_repeated()) {
            for (size_t i = 0; i < get_elements_count(); i++) {
                switch (field_d->cpp_type()) {
                case fdns::CPPTYPE_INT32: {
                    auto v = random_generators::get_int<int32_t>();
                    r->AddInt32(message.get(), field_d, v);
                } break;
                case fdns::CPPTYPE_INT64: {
                    auto v = random_generators::get_int<int64_t>();
                    r->AddInt64(message.get(), field_d, v);
                } break;
                case fdns::CPPTYPE_UINT32: {
                    auto v = random_generators::get_int<uint32_t>();
                    r->AddUInt32(message.get(), field_d, v);
                } break;
                case fdns::CPPTYPE_UINT64: {
                    auto v = random_generators::get_int<uint64_t>();
                    r->AddUInt64(message.get(), field_d, v);
                } break;
                case fdns::CPPTYPE_DOUBLE: {
                    auto v = random_generators::get_real<double>();
                    r->AddDouble(message.get(), field_d, v);
                } break;
                case fdns::CPPTYPE_FLOAT: {
                    auto v = random_generators::get_real<float>();
                    r->AddFloat(message.get(), field_d, v);
                } break;
                case fdns::CPPTYPE_BOOL: {
                    auto v = random_generators::random_choice({true, false});
                    r->AddBool(message.get(), field_d, v);
                } break;
                case fdns::CPPTYPE_ENUM: {
                    const auto* enum_d = field_d->enum_type();
                    auto ri = random_generators::get_int(
                      0, enum_d->value_count() - 1);
                    r->AddEnum(message.get(), field_d, enum_d->value(ri));
                } break;
                case fdns::CPPTYPE_STRING: {
                    r->AddString(
                      message.get(), field_d, random_string(_config));
                } break;
                case fdns::CPPTYPE_MESSAGE: {
                    // TODO: does this work with maps?
                    auto* sub_messsage = generate_protobuf_message_impl(
                                           level + 1, field_d->message_type())
                                           .release();
                    r->AddAllocatedMessage(
                      message.get(), field_d, sub_messsage);
                } break;
                }
            }
        } else {
            if (
              _config.randomize_optional_fields && field_d->is_optional()
              && random_generators::get_int(0, 1) == 1) {
                continue;
            }

            switch (field_d->cpp_type()) {
            case fdns::CPPTYPE_INT32: {
                auto v = random_generators::get_int<int32_t>();
                r->SetInt32(message.get(), field_d, v);
            } break;
            case fdns::CPPTYPE_INT64: {
                auto v = random_generators::get_int<int64_t>();
                r->SetInt64(message.get(), field_d, v);
            } break;
            case fdns::CPPTYPE_UINT32: {
                auto v = random_generators::get_int<uint32_t>();
                r->SetUInt32(message.get(), field_d, v);
            } break;
            case fdns::CPPTYPE_UINT64: {
                auto v = random_generators::get_int<uint64_t>();
                r->SetUInt64(message.get(), field_d, v);
            } break;
            case fdns::CPPTYPE_DOUBLE: {
                auto v = random_generators::get_real<double>();
                r->SetDouble(message.get(), field_d, v);
            } break;
            case fdns::CPPTYPE_FLOAT: {
                auto v = random_generators::get_real<float>();
                r->SetFloat(message.get(), field_d, v);
            } break;
            case fdns::CPPTYPE_BOOL: {
                auto v = random_generators::random_choice({true, false});
                r->SetBool(message.get(), field_d, v);
            } break;
            case fdns::CPPTYPE_ENUM: {
                const auto* enum_d = field_d->enum_type();
                auto ri = random_generators::get_int(
                  0, enum_d->value_count() - 1);
                r->SetEnum(message.get(), field_d, enum_d->value(ri));
            } break;
            case fdns::CPPTYPE_STRING: {
                r->SetString(message.get(), field_d, random_string(_config));
            } break;
            case fdns::CPPTYPE_MESSAGE: {
                if (
                  field_d->is_optional()
                  && level >= _config.max_nesting_level) {
                    break;
                }
                auto* sub_messsage = generate_protobuf_message_impl(
                                       level + 1, field_d->message_type())
                                       .release();
                r->SetAllocatedMessage(message.get(), sub_messsage, field_d);
            } break;
            }
        }
    }

    return message;
}

} // namespace testing
