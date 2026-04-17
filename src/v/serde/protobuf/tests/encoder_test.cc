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

#include "base/units.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "serde/protobuf/encoder.h"
#include "serde/protobuf/parser.h"

#include <seastar/util/variant_utils.hh>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/util/message_differencer.h>

// TODO: Fix bazelbuild/bazel#4446
// Suppress deprecation warnings from protobuf's generated map_field.h code.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#include "src/v/serde/protobuf/tests/test_messages_edition2023.pb.h"
#pragma clang diagnostic pop
#include "src/v/serde/protobuf/tests/three.pb.h"

#include <gtest/gtest.h>
#include <protobuf_mutator/mutator.h>

#include <memory>
#include <string>

namespace serde::pb {
namespace {

namespace gpb = google::protobuf;

class IgnoreUnknownFields
  : public gpb::util::MessageDifferencer::IgnoreCriteria {
    bool IsIgnored(
      const gpb::Message& /*message1*/,
      const gpb::Message& /*message2*/,
      const gpb::FieldDescriptor* /*field*/,
      const std::vector<
        gpb::util::MessageDifferencer::SpecificField>& /*parent_fields*/)
      override {
        return false;
    }
    bool IsUnknownFieldIgnored(
      const gpb::Message& /*message1*/,
      const gpb::Message& /*message2*/,
      const gpb::util::MessageDifferencer::SpecificField& /*field*/,
      const std::vector<
        gpb::util::MessageDifferencer::SpecificField>& /*parent_fields*/)
      override {
        return true;
    }
};

/// Test fixture that provides roundtrip helpers:
///   original protobuf -> serialize -> parse -> encode -> deserialize
/// then compare the original protobuf with the deserialized one.
class ProtobufEncoderFixture : public testing::Test {
public:
    /// Roundtrip: serialize msg with libprotobuf, parse with our parser,
    /// encode back with our encoder, then deserialize with libprotobuf.
    /// Returns an assertion result comparing original and roundtripped message.
    testing::AssertionResult roundtrip(const gpb::Message& original) {
        auto binpb = original.SerializeAsString();
        iobuf buf;
        buf.append(binpb.data(), binpb.size());

        auto parsed
          = ::serde::pb::parse(std::move(buf), *original.GetDescriptor()).get();
        auto encoded
          = ::serde::pb::encode(*parsed, *original.GetDescriptor()).get();

        auto encoded_str = encoded.linearize_to_string();
        auto roundtripped = std::unique_ptr<gpb::Message>(original.New());
        if (!roundtripped->ParseFromString(
              {encoded_str.data(), encoded_str.size()})) {
            return testing::AssertionFailure()
                   << "failed to parse encoded bytes with libprotobuf";
        }
        return proto_equals(*roundtripped, original);
    }

    /// Roundtrip at the parsed level: serialize msg with libprotobuf, parse
    /// with our parser, encode back, parse again, and compare the two parsed
    /// representations by re-serializing through libprotobuf.
    testing::AssertionResult roundtrip_parsed(const gpb::Message& original) {
        auto binpb = original.SerializeAsString();
        iobuf buf;
        buf.append(binpb.data(), binpb.size());

        auto parsed1
          = ::serde::pb::parse(buf.copy(), *original.GetDescriptor()).get();
        auto encoded
          = ::serde::pb::encode(*parsed1, *original.GetDescriptor()).get();
        auto parsed2 = ::serde::pb::parse(
                         std::move(encoded), *original.GetDescriptor())
                         .get();

        // Convert both parsed results back to protobuf and compare.
        auto msg1 = convert_parsed_to_protobuf(
          *parsed1, original.GetDescriptor());
        auto msg2 = convert_parsed_to_protobuf(
          *parsed2, original.GetDescriptor());
        return proto_equals(*msg1, *msg2);
    }

private:
    testing::AssertionResult
    proto_equals(const gpb::Message& actual, const gpb::Message& expected) {
        gpb::util::MessageDifferencer differ;
        std::string diff;
        differ.ReportDifferencesToString(&diff);
        differ.AddIgnoreCriteria(std::make_unique<IgnoreUnknownFields>());
        if (differ.Compare(actual, expected)) {
            return testing::AssertionSuccess();
        }
        return testing::AssertionFailure() << diff;
    }

    std::unique_ptr<gpb::Message> convert_parsed_to_protobuf(
      const parsed::message& msg, const gpb::Descriptor* desc) {
        auto output = std::unique_ptr<gpb::Message>(
          factory_.GetPrototype(desc)->New());
        for (const auto& [index, value] : msg.fields) {
            convert_field(index, value, output.get());
        }
        return output;
    }

    void convert_field(
      int32_t field_number,
      const parsed::message::field& value,
      gpb::Message* output) {
        auto* reflect = output->GetReflection();
        auto* field = output->GetDescriptor()->FindFieldByNumber(field_number);
        if (field == nullptr) {
            return;
        }
        ss::visit(
          value,
          [=](double v) { reflect->SetDouble(output, field, v); },
          [=](float v) { reflect->SetFloat(output, field, v); },
          [=](int32_t v) {
              if (field->enum_type()) {
                  reflect->SetEnumValue(output, field, v);
              } else {
                  reflect->SetInt32(output, field, v);
              }
          },
          [=](int64_t v) { reflect->SetInt64(output, field, v); },
          [=](uint32_t v) { reflect->SetUInt32(output, field, v); },
          [=](uint64_t v) { reflect->SetUInt64(output, field, v); },
          [=](bool v) { reflect->SetBool(output, field, v); },
          [=, this](const std::unique_ptr<parsed::message>& v) {
              auto pbmsg = convert_parsed_to_protobuf(
                *v, field->message_type());
              reflect->SetAllocatedMessage(output, pbmsg.release(), field);
          },
          [=, this](const parsed::repeated& v) {
              convert_repeated(v, field, output);
          },
          [=](const iobuf& v) {
              iobuf_const_parser parser(v);
              auto str = parser.read_string(v.size_bytes());
              reflect->SetString(output, field, std::move(str));
          },
          [=, this](const parsed::map& v) { convert_map(v, field, output); });
    }

    void convert_repeated(
      const parsed::repeated& list,
      const gpb::FieldDescriptor* field,
      gpb::Message* output) {
        auto* reflect = output->GetReflection();
        ss::visit(
          list.elements,
          [=](const chunked_vector<double>& vec) {
              for (auto v : vec) {
                  reflect->AddDouble(output, field, v);
              }
          },
          [=](const chunked_vector<float>& vec) {
              for (auto v : vec) {
                  reflect->AddFloat(output, field, v);
              }
          },
          [=](const chunked_vector<int32_t>& vec) {
              if (field->enum_type()) {
                  for (auto v : vec) {
                      reflect->AddEnumValue(output, field, v);
                  }
              } else {
                  for (auto v : vec) {
                      reflect->AddInt32(output, field, v);
                  }
              }
          },
          [=](const chunked_vector<int64_t>& vec) {
              for (auto v : vec) {
                  reflect->AddInt64(output, field, v);
              }
          },
          [=](const chunked_vector<uint32_t>& vec) {
              for (auto v : vec) {
                  reflect->AddUInt32(output, field, v);
              }
          },
          [=](const chunked_vector<uint64_t>& vec) {
              for (auto v : vec) {
                  reflect->AddUInt64(output, field, v);
              }
          },
          [=](const chunked_vector<bool>& vec) {
              for (auto v : vec) {
                  reflect->AddBool(output, field, v);
              }
          },
          [=](const chunked_vector<iobuf>& vec) {
              for (const auto& v : vec) {
                  iobuf_const_parser parser(v);
                  auto str = parser.read_string(v.size_bytes());
                  reflect->AddString(output, field, std::move(str));
              }
          },
          [=,
           this](const chunked_vector<std::unique_ptr<parsed::message>>& vec) {
              for (const auto& v : vec) {
                  auto msg = convert_parsed_to_protobuf(
                    *v, field->message_type());
                  reflect->AddAllocatedMessage(output, field, msg.release());
              }
          });
    }

    void convert_map(
      const parsed::map& map,
      const gpb::FieldDescriptor* field,
      gpb::Message* output) {
        const auto* entry_desc = field->message_type();
        auto* reflect = output->GetReflection();
        for (const auto& [k, v] : map.entries) {
            auto entry = std::unique_ptr<gpb::Message>(
              factory_.GetPrototype(entry_desc)->New());
            auto key_field = entry_desc->map_key();
            ss::visit(
              k,
              [](std::monostate) {},
              [&, this](const iobuf& val) {
                  convert_field(
                    key_field->number(),
                    parsed::message::field(val.copy()),
                    entry.get());
              },
              [&, this](const auto& val) {
                  convert_field(
                    key_field->number(),
                    parsed::message::field(val),
                    entry.get());
              });
            auto val_field = entry_desc->map_value();
            ss::visit(
              v,
              [](std::monostate) {},
              [&, this](const iobuf& val) {
                  convert_field(
                    val_field->number(),
                    parsed::message::field(val.copy()),
                    entry.get());
              },
              [&, this](const std::unique_ptr<parsed::message>& val) {
                  if (val_field->message_type() == nullptr) {
                      throw std::runtime_error(
                        fmt::format(
                          "expected message type got: {}",
                          val_field->DebugString()));
                  }
                  auto msg = convert_parsed_to_protobuf(
                    *val, val_field->message_type());
                  entry->GetReflection()->SetAllocatedMessage(
                    entry.get(), msg.release(), val_field);
              },
              [&, this](const auto& val) {
                  convert_field(
                    val_field->number(),
                    parsed::message::field(val),
                    entry.get());
              });
            reflect->AddAllocatedMessage(output, field, entry.release());
        }
    }

    gpb::DynamicMessageFactory factory_;
};

TEST_F(ProtobufEncoderFixture, EncodeSimpleMessage) {
    pbthree::SearchRequest msg;
    msg.set_query("what is the best kafka alternative?");
    msg.set_page_number(42);
    msg.set_results_per_page(100);
    EXPECT_TRUE(roundtrip(msg));
}

TEST_F(ProtobufEncoderFixture, EncodeEmptyMessage) {
    pbthree::SearchRequest msg;
    EXPECT_TRUE(roundtrip(msg));
}

TEST_F(ProtobufEncoderFixture, EncodeNestedMessage) {
    pbthree::SearchResponse msg;
    auto* r1 = msg.add_results();
    r1->set_url("http://redpanda.com");
    r1->set_title("fastest queue in the west");
    r1->add_snippets("fastest");
    r1->add_snippets("queue");
    auto* r2 = msg.add_results();
    r2->set_url("http://docs.redpanda.com");
    r2->set_title("Redpanda docs");
    EXPECT_TRUE(roundtrip(msg));
}

TEST_F(ProtobufEncoderFixture, EncodeRecursiveMessage) {
    pbthree::Node msg;
    auto* left = msg.mutable_left();
    left->mutable_left()->set_value(99);
    left->mutable_right()->set_value(101);
    msg.mutable_right()->set_value(3000);
    EXPECT_TRUE(roundtrip(msg));
}

TEST_F(ProtobufEncoderFixture, EncodeWithRepeated) {
    pbthree::Packed msg;
    msg.add_test(1);
    msg.add_test(2);
    msg.add_test(3);
    msg.add_test(100);
    msg.add_test(-50);
    EXPECT_TRUE(roundtrip(msg));
}

TEST_F(ProtobufEncoderFixture, EncodeWithMap) {
    pbthree::Map msg;
    (*msg.mutable_meta())["foo"] = "bar";
    (*msg.mutable_meta())["baz"] = "qux";
    (*msg.mutable_meta())["empty"] = "";
    EXPECT_TRUE(roundtrip(msg));
}

TEST_F(ProtobufEncoderFixture, EncodeWithMapIntKeys) {
    pbthree::Entries msg;
    (*msg.mutable_entry())["hello"] = 42;
    (*msg.mutable_entry())["world"] = 99;
    EXPECT_TRUE(roundtrip(msg));
}

TEST_F(ProtobufEncoderFixture, EncodeWithOneof) {
    {
        pbthree::Version4 msg;
        msg.set_foo("hello oneof");
        EXPECT_TRUE(roundtrip(msg));
    }
    {
        pbthree::Version4 msg;
        msg.set_data(true);
        EXPECT_TRUE(roundtrip(msg));
    }
}

TEST_F(ProtobufEncoderFixture, EncodeAllScalarTypes) {
    protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
    msg.set_optional_int32(-42);
    msg.set_optional_int64(-1234567890123LL);
    msg.set_optional_uint32(42);
    msg.set_optional_uint64(1234567890123ULL);
    msg.set_optional_sint32(-100);
    msg.set_optional_sint64(-200);
    msg.set_optional_fixed32(0xDEADBEEF);
    msg.set_optional_fixed64(0xCAFEBABEDEADBEEFULL);
    msg.set_optional_sfixed32(-999);
    msg.set_optional_sfixed64(-1234567890LL);
    msg.set_optional_float(3.14f);
    msg.set_optional_double(2.718281828);
    msg.set_optional_bool(true);
    msg.set_optional_string("hello world");
    msg.set_optional_bytes("binary_data");
    EXPECT_TRUE(roundtrip(msg));
}

TEST_F(ProtobufEncoderFixture, EncodeNestedMessageTypes) {
    protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
    auto* nested = msg.mutable_optional_nested_message();
    nested->set_a(42);
    msg.mutable_optional_foreign_message()->set_c(99);
    EXPECT_TRUE(roundtrip(msg));
}

TEST_F(ProtobufEncoderFixture, EncodeEnumFields) {
    protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
    msg.set_optional_nested_enum(
      protobuf_test_messages::editions::TestAllTypesEdition2023::BAR);
    msg.set_optional_foreign_enum(
      protobuf_test_messages::editions::FOREIGN_BAZ);
    EXPECT_TRUE(roundtrip(msg));
}

TEST_F(ProtobufEncoderFixture, EncodeNegativeEnum) {
    protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
    msg.set_optional_nested_enum(
      protobuf_test_messages::editions::TestAllTypesEdition2023::NEG);
    EXPECT_TRUE(roundtrip(msg));
}

TEST_F(ProtobufEncoderFixture, EncodeRepeatedScalars) {
    protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
    for (int i = 0; i < 5; ++i) {
        msg.add_repeated_int32(i * 10);
        msg.add_repeated_int64(i * 100LL);
        msg.add_repeated_uint32(i * 1000U);
        msg.add_repeated_uint64(i * 10000ULL);
        msg.add_repeated_sint32(i * -1);
        msg.add_repeated_sint64(i * -10LL);
        msg.add_repeated_fixed32(i + 100);
        msg.add_repeated_fixed64(i + 200);
        msg.add_repeated_sfixed32(i - 50);
        msg.add_repeated_sfixed64(i - 100);
        msg.add_repeated_float(static_cast<float>(i) * 1.5f);
        msg.add_repeated_double(static_cast<double>(i) * 2.5);
        msg.add_repeated_bool(i % 2 == 0);
        msg.add_repeated_string("str_" + std::to_string(i));
        msg.add_repeated_bytes("bytes_" + std::to_string(i));
    }
    EXPECT_TRUE(roundtrip(msg));
}

TEST_F(ProtobufEncoderFixture, EncodeRepeatedNestedMessages) {
    protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
    for (int i = 0; i < 3; ++i) {
        auto* nested = msg.add_repeated_nested_message();
        nested->set_a(i * 42);
    }
    EXPECT_TRUE(roundtrip(msg));
}

TEST_F(ProtobufEncoderFixture, EncodePackedFields) {
    protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
    for (int i = 0; i < 5; ++i) {
        msg.add_packed_int32(i * 10);
        msg.add_packed_int64(i * 100LL);
        msg.add_packed_uint32(i * 1000U);
        msg.add_packed_uint64(i * 10000ULL);
        msg.add_packed_sint32(i * -1);
        msg.add_packed_sint64(i * -10LL);
        msg.add_packed_fixed32(i + 100);
        msg.add_packed_fixed64(i + 200);
        msg.add_packed_sfixed32(i - 50);
        msg.add_packed_sfixed64(i - 100);
        msg.add_packed_float(static_cast<float>(i) * 1.5f);
        msg.add_packed_double(static_cast<double>(i) * 2.5);
        msg.add_packed_bool(i % 2 == 0);
    }
    EXPECT_TRUE(roundtrip(msg));
}

TEST_F(ProtobufEncoderFixture, EncodeUnpackedFields) {
    protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
    for (int i = 0; i < 3; ++i) {
        msg.add_unpacked_int32(i * 10);
        msg.add_unpacked_int64(i * 100LL);
        msg.add_unpacked_uint32(i * 1000U);
        msg.add_unpacked_uint64(i * 10000ULL);
        msg.add_unpacked_sint32(i * -1);
        msg.add_unpacked_sint64(i * -10LL);
        msg.add_unpacked_fixed32(i + 100);
        msg.add_unpacked_fixed64(i + 200);
        msg.add_unpacked_sfixed32(i - 50);
        msg.add_unpacked_sfixed64(i - 100);
        msg.add_unpacked_float(static_cast<float>(i) * 1.5f);
        msg.add_unpacked_double(static_cast<double>(i) * 2.5);
        msg.add_unpacked_bool(i % 2 == 0);
    }
    EXPECT_TRUE(roundtrip(msg));
}

TEST_F(ProtobufEncoderFixture, EncodeMapFields) {
    protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
    (*msg.mutable_map_int32_int32())[1] = 10;
    (*msg.mutable_map_int32_int32())[2] = 20;
    (*msg.mutable_map_int64_int64())[100LL] = 200LL;
    (*msg.mutable_map_uint32_uint32())[1000U] = 2000U;
    (*msg.mutable_map_uint64_uint64())[10000ULL] = 20000ULL;
    (*msg.mutable_map_string_string())["key1"] = "value1";
    (*msg.mutable_map_string_string())["key2"] = "value2";
    (*msg.mutable_map_string_bytes())["bkey"] = "\x00\x01\x02";
    (*msg.mutable_map_bool_bool())[true] = false;
    (*msg.mutable_map_bool_bool())[false] = true;
    EXPECT_TRUE(roundtrip(msg));
}

TEST_F(ProtobufEncoderFixture, EncodeMapWithNestedMessage) {
    protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
    auto& map = *msg.mutable_map_string_nested_message();
    map["first"].set_a(42);
    map["second"].set_a(99);
    EXPECT_TRUE(roundtrip(msg));
}

TEST_F(ProtobufEncoderFixture, EncodeOneofFields) {
    {
        protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
        msg.set_oneof_uint32(12345);
        EXPECT_TRUE(roundtrip(msg));
    }
    {
        protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
        msg.set_oneof_string("oneof test");
        EXPECT_TRUE(roundtrip(msg));
    }
    {
        protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
        msg.set_oneof_bool(true);
        EXPECT_TRUE(roundtrip(msg));
    }
    {
        protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
        msg.set_oneof_uint64(9876543210ULL);
        EXPECT_TRUE(roundtrip(msg));
    }
    {
        protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
        msg.set_oneof_float(1.23f);
        EXPECT_TRUE(roundtrip(msg));
    }
    {
        protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
        msg.set_oneof_double(4.56);
        EXPECT_TRUE(roundtrip(msg));
    }
    {
        protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
        msg.mutable_oneof_nested_message()->set_a(42);
        EXPECT_TRUE(roundtrip(msg));
    }
    {
        protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
        msg.set_oneof_bytes("oneof bytes");
        EXPECT_TRUE(roundtrip(msg));
    }
}

TEST_F(ProtobufEncoderFixture, RoundtripPreservesData) {
    // Build a message with many different field types and verify
    // that parse -> encode -> parse produces identical results.
    protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
    msg.set_optional_int32(-42);
    msg.set_optional_int64(-100);
    msg.set_optional_uint32(42);
    msg.set_optional_uint64(100);
    msg.set_optional_sint32(-50);
    msg.set_optional_sint64(-200);
    msg.set_optional_fixed32(0xABCD);
    msg.set_optional_fixed64(0xABCDEF01);
    msg.set_optional_sfixed32(-999);
    msg.set_optional_sfixed64(-12345);
    msg.set_optional_float(3.14f);
    msg.set_optional_double(2.718);
    msg.set_optional_bool(true);
    msg.set_optional_string("test roundtrip");
    msg.set_optional_bytes("test_bytes");
    msg.mutable_optional_nested_message()->set_a(7);
    msg.set_optional_nested_enum(
      protobuf_test_messages::editions::TestAllTypesEdition2023::BAZ);
    msg.add_repeated_int32(1);
    msg.add_repeated_int32(2);
    msg.add_repeated_int32(3);
    msg.add_repeated_string("a");
    msg.add_repeated_string("b");
    auto* nested = msg.add_repeated_nested_message();
    nested->set_a(100);
    (*msg.mutable_map_string_string())["k"] = "v";
    msg.set_oneof_string("chosen");

    EXPECT_TRUE(roundtrip(msg));
    EXPECT_TRUE(roundtrip_parsed(msg));
}

TEST_F(ProtobufEncoderFixture, NegativeInts) {
    {
        protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
        msg.set_optional_int32(-1);
        EXPECT_TRUE(roundtrip(msg));
    }
    {
        protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
        msg.set_optional_sint32(-1);
        EXPECT_TRUE(roundtrip(msg));
    }
    {
        protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
        msg.set_optional_sfixed32(-1);
        EXPECT_TRUE(roundtrip(msg));
    }
    {
        protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
        msg.set_optional_int64(-1);
        EXPECT_TRUE(roundtrip(msg));
    }
    {
        protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
        msg.set_optional_sint64(-1);
        EXPECT_TRUE(roundtrip(msg));
    }
    {
        protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
        msg.set_optional_sfixed64(-1);
        EXPECT_TRUE(roundtrip(msg));
    }
}

class ProtobufEncoderFuzzer : public ProtobufEncoderFixture {
public:
    ProtobufEncoderFuzzer() { mutator_.Seed(testing::FLAGS_gtest_random_seed); }

    void mutate(gpb::Message* msg) { mutator_.Mutate(msg, 3_MiB); }

private:
    protobuf_mutator::Mutator mutator_;
};

TEST_F(ProtobufEncoderFuzzer, AllTypesRoundtrip) {
    protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
    constexpr int num_iterations = 20;
    for (int i = 0; i < num_iterations; ++i) {
        mutate(&msg);
        EXPECT_TRUE(roundtrip(msg)) << "iteration: " << i;
    }
}

TEST_F(ProtobufEncoderFuzzer, AllTypesParsedRoundtrip) {
    protobuf_test_messages::editions::TestAllTypesEdition2023 msg;
    constexpr int num_iterations = 20;
    for (int i = 0; i < num_iterations; ++i) {
        mutate(&msg);
        EXPECT_TRUE(roundtrip_parsed(msg)) << "iteration: " << i;
    }
}

} // namespace
} // namespace serde::pb
