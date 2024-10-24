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

#include "base/units.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "gtest/gtest.h"
#include "serde/protobuf/parser.h"
// TODO: Fix bazelbuild/bazel#4446
#include "src/v/serde/protobuf/tests/test_messages_edition2023.pb.h"
#include "src/v/serde/protobuf/tests/three.pb.h"
#include "src/v/serde/protobuf/tests/two.pb.h"

#include <seastar/util/variant_utils.hh>

#include <fmt/core.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>
#include <protobuf_mutator/mutator.h>

#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <variant>

namespace serde::pb {
namespace {

namespace pb = google::protobuf;

class IgnoreUnknownFields
  : public pb::util::MessageDifferencer::IgnoreCriteria {
    bool IsIgnored(
      const pb::Message& /* message1 */,
      const pb::Message& /* message2 */,
      const pb::FieldDescriptor* /* field */,
      const std::vector<
        pb::util::MessageDifferencer::SpecificField>& /* parent_fields */)
      override {
        return false;
    }
    bool IsUnknownFieldIgnored(
      const pb::Message& /* message1 */,
      const pb::Message& /* message2 */,
      const pb::util::MessageDifferencer::SpecificField& /* field */,
      const std::vector<
        pb::util::MessageDifferencer::SpecificField>& /* parent_fields */)
      override {
        return true;
    }
};

class ProtobufParserFixture : public testing::Test {
public:
    std::unique_ptr<pb::Message> construct_message(std::string_view txtpb) {
        static const std::unordered_map<std::string, const pb::FileDescriptor*>
          global_protos = {
            {"two.proto", pbtwo::SearchRequest::descriptor()->file()},
            {"three.proto", pbthree::SearchRequest::descriptor()->file()},
            {"test_messages_edition2023.proto",
             protobuf_test_messages::editions::TestAllTypesEdition2023::
               descriptor()
                 ->file()},
          };
        for (const auto& [name, file] : global_protos) {
            auto comment = fmt::format("# proto-file: {}", name);
            if (txtpb.find(comment) == std::string_view::npos) {
                continue;
            }
            for (int i = 0; i < file->message_type_count(); ++i) {
                auto* descriptor = file->message_type(i);
                auto comment = fmt::format(
                  "# proto-message: {}", descriptor->name());
                if (txtpb.find(comment) != std::string_view::npos) {
                    return construct_message(descriptor, txtpb);
                }
            }
            throw std::runtime_error("unable to find proto message");
        }
        throw std::runtime_error("unable to find proto file");
    }

    std::unique_ptr<pb::Message> construct_message(
      const pb::Descriptor* descriptor, std::string_view txtpb) {
        const pb::Message* prototype = factory_.GetPrototype(descriptor);
        auto output = std::unique_ptr<pb::Message>(prototype->New());
        if (!pb::TextFormat::ParseFromString(txtpb, output.get())) {
            throw std::runtime_error("Parsing txtpb failed");
        }
        return output;
    }

    std::unique_ptr<parsed::message> parse_raw(std::string_view txtpb) {
        auto expected = construct_message(txtpb);
        iobuf out;
        auto binpb = expected->SerializeAsString();
        out.append(binpb.data(), binpb.size());
        return ::serde::pb::parse(std::move(out), *expected->GetDescriptor())
          .get();
    }

    testing::AssertionResult parse(std::string_view txtpb) {
        auto expected = construct_message(txtpb);
        iobuf out;
        auto binpb = expected->SerializeAsString();
        out.append(binpb.data(), binpb.size());
        auto parsed = ::serde::pb::parse(
                        std::move(out), *expected->GetDescriptor())
                        .get();
        auto actual = convert_to_protobuf(*parsed, expected->GetDescriptor());
        return proto_equals(*actual, *expected);
    }

    template<typename T>
    testing::AssertionResult parse_as(std::string_view txtpb) {
        auto original = construct_message(txtpb);
        iobuf out;
        auto binpb = original->SerializeAsString();
        out.append(binpb.data(), binpb.size());
        auto parsed
          = ::serde::pb::parse(std::move(out), *T::descriptor()).get();
        auto actual = convert_to_protobuf(*parsed, T::descriptor());
        T expected;
        if (!expected.ParseFromString(binpb)) {
            return testing::AssertionFailure() << "Not wire compatible";
        }
        return proto_equals(*actual, expected);
    }

    template<typename T>
    testing::AssertionResult
    parse_merged_as(const std::vector<std::string_view>& txtpbs) {
        std::vector<std::unique_ptr<pb::Message>> messages;
        std::string binpb;
        for (const auto& txtpb : txtpbs) {
            auto original = construct_message(txtpb);
            binpb += original->SerializeAsString();
        }
        iobuf out;
        out.append(binpb.data(), binpb.size());
        auto parsed
          = ::serde::pb::parse(std::move(out), *T::descriptor()).get();
        auto actual = convert_to_protobuf(*parsed, T::descriptor());
        T expected;
        if (!expected.ParseFromString(binpb)) {
            return testing::AssertionFailure() << "Not wire compatible";
        }
        return proto_equals(*actual, expected);
    }

    testing::AssertionResult
    proto_equals(const pb::Message& actual, const pb::Message& expected) {
        pb::util::MessageDifferencer differ;
        std::string diff;
        differ.ReportDifferencesToString(&diff);
        // Currently, we don't track unknown fields - so ignore them.
        differ.AddIgnoreCriteria(std::make_unique<IgnoreUnknownFields>());
        if (differ.Compare(actual, expected)) {
            return testing::AssertionSuccess();
        } else {
            return testing::AssertionFailure() << diff;
        }
    }

    std::unique_ptr<pb::Message> convert_to_protobuf(
      const parsed::message& msg, const pb::Descriptor* desc) {
        auto output = std::unique_ptr<pb::Message>(
          factory_.GetPrototype(desc)->New());
        for (const auto& [index, value] : msg.fields) {
            validate_oneof(msg, index, desc);
            convert_to_protobuf(index, value, output.get());
        }
        return output;
    }

private:
    void validate_oneof(
      const parsed::message& msg, int32_t num, const pb::Descriptor* desc) {
        auto field = desc->FindFieldByNumber(num);
        auto oneof = field->containing_oneof();
        if (oneof == nullptr) {
            return;
        }
        std::set<int32_t> set;
        for (int32_t i = 0; i < oneof->field_count(); ++i) {
            auto discrimiant = oneof->field(i);
            if (msg.fields.contains(discrimiant->number())) {
                set.insert(discrimiant->number());
            }
        }
        if (set.size() > 1) {
            throw std::runtime_error(fmt::format(
              "multiple oneof values ({}) set: {}",
              oneof->name(),
              fmt::join(set, ", ")));
        }
    }

    void convert_to_protobuf(
      int32_t field_number,
      const parsed::message::field& value,
      pb::Message* output) {
        auto* reflect = output->GetReflection();
        auto* field = output->GetDescriptor()->FindFieldByNumber(field_number);
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
              auto pbmsg = convert_to_protobuf(*v, field->message_type());
              reflect->SetAllocatedMessage(output, pbmsg.release(), field);
          },
          [=, this](const parsed::repeated& v) {
              convert_to_protobuf(v, field, output);
          },
          [=](const iobuf& v) {
              iobuf_const_parser parser(v);
              auto str = parser.read_string(v.size_bytes());
              reflect->SetString(output, field, std::move(str));
          },
          [=, this](const parsed::map& v) {
              auto entries = convert_to_protobuf(v, field->message_type());
              for (auto& entry : entries) {
                  reflect->AddAllocatedMessage(output, field, entry.release());
              }
          });
    }

    void convert_to_protobuf(
      const parsed::repeated& list,
      const pb::FieldDescriptor* field,
      pb::Message* output) {
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
                  auto msg = convert_to_protobuf(*v, field->message_type());
                  reflect->AddAllocatedMessage(output, field, msg.release());
              }
          });
    }

    std::vector<std::unique_ptr<pb::Message>>
    convert_to_protobuf(const parsed::map& map, const pb::Descriptor* desc) {
        std::vector<std::unique_ptr<pb::Message>> entries;
        for (const auto& [k, v] : map.entries) {
            auto output = std::unique_ptr<pb::Message>(
              factory_.GetPrototype(desc)->New());
            auto key_field = desc->map_key();
            ss::visit(
              k,
              [](const std::monostate&) {},
              [&, this](const iobuf& v) {
                  convert_to_protobuf(
                    key_field->number(),
                    parsed::message::field(v.copy()),
                    output.get());
              },
              [&, this](const auto& v) {
                  convert_to_protobuf(
                    key_field->number(),
                    parsed::message::field(v),
                    output.get());
              });
            auto value_field = desc->map_value();
            ss::visit(
              v,
              [](const std::monostate&) {},
              [&, this](const iobuf& v) {
                  convert_to_protobuf(
                    value_field->number(),
                    parsed::message::field(v.copy()),
                    output.get());
              },
              [&, this](const std::unique_ptr<parsed::message>& v) {
                  if (value_field->message_type() == nullptr) {
                      throw std::runtime_error(fmt::format(
                        "expected message type got: {}",
                        value_field->DebugString()));
                  }
                  auto msg = convert_to_protobuf(
                    *v, value_field->message_type());
                  output->GetReflection()->SetAllocatedMessage(
                    output.get(), msg.release(), value_field);
              },
              [&, this](const auto& v) {
                  convert_to_protobuf(
                    value_field->number(),
                    parsed::message::field(v),
                    output.get());
              });
            entries.push_back(std::move(output));
        }
        return entries;
    }

    pb::DynamicMessageFactory factory_;
};

TEST_F(ProtobufParserFixture, Works) {
    EXPECT_TRUE(parse(R"(
# proto-file: three.proto
# proto-message: SearchRequest
query: "what's the best kafka alternative?"
page_number: 0
results_per_page: 100
)"));
}

TEST_F(ProtobufParserFixture, EmptyProto) {
    EXPECT_TRUE(parse(R"(
# proto-file: three.proto
# proto-message: SearchRequest
)"));
}

TEST_F(ProtobufParserFixture, NestedProto) {
    EXPECT_TRUE(parse(R"(
# proto-file: three.proto
# proto-message: SearchResponse
results: {
  url: "http://redpanda.com"
  title: "fastest queue in the west"
  snippets: "fastest"
  snippets: "queue"
  snippets: "kafka alternative"
}
results: {
  url: "http://docs.redpanda.com"
  title: "Redpanda the best kafka alternative"
  snippets: "kafka"
  snippets: "alternative"
}
results: {
  url: "http://redpanda.com/blog/hello-world"
  title: "Introducing the best Apache Kafka Alternative - Redpanda"
}
)"));
}

TEST_F(ProtobufParserFixture, RecursiveProto) {
    EXPECT_TRUE(parse(R"(
# proto-file: three.proto
# proto-message: Node
left: {
  left: {
    value: 99
  }
  right: {
    value: 101
  }
}
right: {
  value: 3000
}
)"));
}

TEST_F(ProtobufParserFixture, Compatibility) {
    EXPECT_TRUE(parse_as<pbthree::Version2>(R"(
# proto-file: three.proto
# proto-message: Version1
test: 9223372036854775807
)"));
    EXPECT_TRUE(parse_as<pbthree::Version3>(R"(
# proto-file: three.proto
# proto-message: Version1
test: 9223372036854775807
)"));
    EXPECT_TRUE(parse_as<pbthree::Version4>(R"(
# proto-file: three.proto
# proto-message: Version1
test: 9223372036854775807
)"));
    EXPECT_TRUE(parse_as<pbthree::Version4>(R"(
# proto-file: three.proto
# proto-message: Version3
foo: "hello"
data: true
)"));
    EXPECT_TRUE(parse_as<pbthree::Version3>(R"(
# proto-file: three.proto
# proto-message: Version2
test: -1
foo: "hello"
)"));
    EXPECT_TRUE(parse_as<pbthree::Version1>(R"(
# proto-file: three.proto
# proto-message: Packed
test: 1
test: 2
test: 3
test: 4
)"));
    EXPECT_TRUE(parse_as<pbthree::Packed>(R"(
# proto-file: three.proto
# proto-message: Version1
test: 1
)"));
}

TEST_F(ProtobufParserFixture, OneOf) {
    EXPECT_TRUE(parse_merged_as<pbthree::Version4>({
      R"(
# proto-file: three.proto
# proto-message: Version4
foo: "hello"
)",
      R"(
# proto-file: three.proto
# proto-message: Version4
data: true
)",
    }));
    EXPECT_TRUE(parse_merged_as<pbthree::Version4>({
      R"(
# proto-file: three.proto
# proto-message: Version4
data: true
)",
      R"(
# proto-file: three.proto
# proto-message: Version4
foo: "hello"
)",
    }));
}

TEST_F(ProtobufParserFixture, NegativeEnums) {
    EXPECT_TRUE(parse(R"(
# proto-file: test_messages_edition2023.proto
# proto-message: TestAllTypesEdition2023
optional_nested_enum: NEG
)"));
}

TEST_F(ProtobufParserFixture, NegativeInts) {
    // Negative numbers are sign extended and written as u64
    EXPECT_TRUE(parse(R"(
# proto-file: test_messages_edition2023.proto
# proto-message: TestAllTypesEdition2023
optional_int32: -1
)"));
    EXPECT_TRUE(parse(R"(
# proto-file: test_messages_edition2023.proto
# proto-message: TestAllTypesEdition2023
optional_sint32: -1
)"));
    EXPECT_TRUE(parse(R"(
# proto-file: test_messages_edition2023.proto
# proto-message: TestAllTypesEdition2023
optional_sfixed32: -1
)"));
}

TEST_F(ProtobufParserFixture, Map) {
    auto msg = parse_raw(R"(
# proto-file: three.proto
# proto-message: Map
meta {
  key: "foo"
  value: "bar"
}
meta {
  key: "baz"
  value: "qux"
}
meta {
  value: "nokey"
}
meta {
  key: "novalue"
}
)");
    ASSERT_EQ(msg->fields.size(), 1);
    ASSERT_TRUE(msg->fields.contains(1));
    const auto& field = msg->fields[1];
    ASSERT_TRUE(std::holds_alternative<parsed::map>(field));
    const auto& map = std::get<parsed::map>(field);
    EXPECT_EQ(map.entries.size(), 4);
    EXPECT_TRUE(parse(R"(
# proto-file: three.proto
# proto-message: Entries
entry {
  key: "foo"
  value: 42
}
entry {
  key: "baz"
  value: 53
}
entry {
  key: "novalue"
}
entry {
  value: 99
}
)"));
}

TEST_F(ProtobufParserFixture, RandomData) {
    protobuf_test_messages::editions::TestAllTypesEdition2023 kitchen_sink;
    constexpr size_t size = 512;
    for (char c = '@'; c < '~'; ++c) {
        std::string buf;
        buf.resize(size, c);
        iobuf b;
        b.append(buf.data(), buf.size());
        if (kitchen_sink.ParseFromString(buf)) {
            EXPECT_NO_THROW(
              ::serde::pb::parse(b.copy(), *kitchen_sink.descriptor()).get())
              << c;
        } else {
            EXPECT_ANY_THROW(
              ::serde::pb::parse(b.copy(), *kitchen_sink.descriptor()).get())
              << c;
        }
    }
    // TODO: Write more tests on invalid data
}

class ProtobufParserFuzzer : public ProtobufParserFixture {
public:
    ProtobufParserFuzzer() { mutator_.Seed(testing::FLAGS_gtest_random_seed); }

    void mutate(pb::Message* msg) { mutator_.Mutate(msg, 3_MiB); }

private:
    protobuf_mutator::Mutator mutator_;
};

TEST_F(ProtobufParserFuzzer, AllTypes) {
    protobuf_test_messages::editions::TestAllTypesEdition2023 kitchen_sink;
    constexpr int num_iterations = 10;
    for (int i = 0; i < num_iterations; ++i) {
        mutate(&kitchen_sink);
        auto binpb = kitchen_sink.SerializeAsString();
        iobuf buf;
        buf.append(binpb.data(), binpb.size());
        auto parsed = ::serde::pb::parse(
                        std::move(buf), *kitchen_sink.descriptor())
                        .get();
        auto msg = convert_to_protobuf(*parsed, kitchen_sink.descriptor());
        EXPECT_TRUE(proto_equals(*msg, kitchen_sink));
    }
}

TEST_F(ProtobufParserFuzzer, AllTypesMerged) {
    protobuf_test_messages::editions::TestAllTypesEdition2023 kitchen_sink;
    constexpr int num_iterations = 10;
    std::string merged_binpb;
    for (int i = 0; i < num_iterations; ++i) {
        mutate(&kitchen_sink);
        merged_binpb += kitchen_sink.SerializeAsString();
        kitchen_sink.Clear();
    }
    iobuf buf;
    buf.append(merged_binpb.data(), merged_binpb.size());
    auto parsed
      = ::serde::pb::parse(std::move(buf), *kitchen_sink.descriptor()).get();
    auto msg = convert_to_protobuf(*parsed, kitchen_sink.descriptor());
    EXPECT_TRUE(kitchen_sink.ParseFromString(merged_binpb));
    EXPECT_TRUE(proto_equals(*msg, kitchen_sink));
}

} // namespace
} // namespace serde::pb
