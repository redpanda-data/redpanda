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

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "encryption/field_transformer.h"
#include "encryption/field_transformer_ref.h"
#include "encryption/types.h"
#include "serde/avro/encoder.h"
#include "serde/avro/parser.h"
#include "serde/json/parser.h"
#include "serde/protobuf/encoder.h"
#include "serde/protobuf/parser.h"
#include "test_utils/test.h"
#include "utils/base64.h"

#include <seastar/util/variant_utils.hh>

#include <avro/Compiler.hh>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <gtest/gtest.h>

#include <array>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace encryption {
namespace {

// Fixed 256-bit test DEK.
constexpr std::array<uint8_t, 32> test_dek_bytes = {
  0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA,
  0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5,
  0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF,
};

// Second fixed DEK for multi-KEK tests.
constexpr std::array<uint8_t, 32> test_dek2_bytes = {
  0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA,
  0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5,
  0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF,
};

bytes make_dek(const auto& arr) { return bytes(arr.data(), arr.size()); }

dek_state make_dek_state(const auto& arr, ss::sstring kek_name) {
    return dek_state{
      .plaintext_dek = make_dek(arr),
      .encrypted_dek = {},
      .algorithm = dek_algorithm::aes256_gcm,
      .kek_name = std::move(kek_name),
      .kms_type = "mock",
      .kms_key_id = "test-key",
      .version = 1,
      .created_at = model::timestamp::now(),
      .expiry = std::nullopt,
    };
}

dek_set make_single_dek_set(ss::sstring kek_name = "test-kek") {
    dek_set deks;
    deks.emplace(kek_name, make_dek_state(test_dek_bytes, kek_name));
    return deks;
}

dek_set make_multi_dek_set() {
    dek_set deks;
    deks.emplace("kek-pii", make_dek_state(test_dek_bytes, "kek-pii"));
    deks.emplace("kek-fin", make_dek_state(test_dek2_bytes, "kek-fin"));
    return deks;
}

// =========================================================================
// Avro helpers
// =========================================================================

::avro::ValidSchema compile_avro_schema(std::string_view json) {
    return ::avro::compileJsonSchemaFromString(std::string(json));
}

iobuf avro_serialize(
  const ::avro::GenericDatum& datum, const ::avro::ValidSchema& schema) {
    std::unique_ptr<::avro::OutputStream> out = ::avro::memoryOutputStream();
    auto e = ::avro::binaryEncoder();
    e->init(*out);
    ::avro::GenericWriter writer(schema, e);
    writer.write(datum);
    e->flush();
    auto data = ::avro::snapshot(*out);
    iobuf buffer;
    buffer.append(data->data(), e->byteCount());
    return buffer;
}

// =========================================================================
// Protobuf helpers
// =========================================================================

/// Build a simple protobuf Descriptor programmatically for testing.
/// Uses the DescriptorPool/FileDescriptorProto API.
struct proto_schema {
    google::protobuf::DescriptorPool pool;
    const google::protobuf::FileDescriptor* file{nullptr};

    const google::protobuf::Descriptor* build_simple() {
        // message SimpleRecord { string name = 1; string ssn = 2; int32 age =
        // 3; }
        google::protobuf::FileDescriptorProto file_proto;
        file_proto.set_name("test.proto");
        file_proto.set_syntax("proto3");

        auto* msg = file_proto.add_message_type();
        msg->set_name("SimpleRecord");

        auto* f1 = msg->add_field();
        f1->set_name("name");
        f1->set_number(1);
        f1->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
        f1->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);

        auto* f2 = msg->add_field();
        f2->set_name("ssn");
        f2->set_number(2);
        f2->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
        f2->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);

        auto* f3 = msg->add_field();
        f3->set_name("age");
        f3->set_number(3);
        f3->set_type(google::protobuf::FieldDescriptorProto::TYPE_INT32);
        f3->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);

        file = pool.BuildFile(file_proto);
        return file->FindMessageTypeByName("SimpleRecord");
    }

    const google::protobuf::Descriptor* build_nested() {
        // message Address { string street = 1; string city = 2; }
        // message Person { string name = 1; Address address = 2; int32 age =
        // 3; }
        google::protobuf::FileDescriptorProto file_proto;
        file_proto.set_name("test_nested.proto");
        file_proto.set_syntax("proto3");

        auto* addr_msg = file_proto.add_message_type();
        addr_msg->set_name("Address");
        {
            auto* f = addr_msg->add_field();
            f->set_name("street");
            f->set_number(1);
            f->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
            f->set_label(
              google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
        }
        {
            auto* f = addr_msg->add_field();
            f->set_name("city");
            f->set_number(2);
            f->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
            f->set_label(
              google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
        }

        auto* person_msg = file_proto.add_message_type();
        person_msg->set_name("Person");
        {
            auto* f = person_msg->add_field();
            f->set_name("name");
            f->set_number(1);
            f->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
            f->set_label(
              google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
        }
        {
            auto* f = person_msg->add_field();
            f->set_name("address");
            f->set_number(2);
            f->set_type(google::protobuf::FieldDescriptorProto::TYPE_MESSAGE);
            f->set_type_name("Address");
            f->set_label(
              google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
        }
        {
            auto* f = person_msg->add_field();
            f->set_name("age");
            f->set_number(3);
            f->set_type(google::protobuf::FieldDescriptorProto::TYPE_INT32);
            f->set_label(
              google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
        }

        file = pool.BuildFile(file_proto);
        return file->FindMessageTypeByName("Person");
    }
};

iobuf pb_serialize(const google::protobuf::Message& msg) {
    auto s = msg.SerializeAsString();
    iobuf buf;
    buf.append(s.data(), s.size());
    return buf;
}

// =========================================================================
// Encrypt/decrypt unit tests
// =========================================================================

TEST(encrypt_decrypt, roundtrip_256) {
    auto dek = make_dek(test_dek_bytes);
    iobuf plaintext;
    plaintext.append_str("hello world");
    auto ct = encrypt_field_value(dek, plaintext.copy());
    EXPECT_NE(ct, plaintext);
    // [12 IV + 11 ciphertext + 16 tag] = 39 bytes
    EXPECT_EQ(ct.size_bytes(), 12 + 11 + 16);
    auto pt = decrypt_field_value(dek, std::move(ct));
    EXPECT_EQ(pt, plaintext);
}

TEST(encrypt_decrypt, empty_plaintext) {
    auto dek = make_dek(test_dek_bytes);
    iobuf plaintext;
    auto ct = encrypt_field_value(dek, plaintext.copy());
    // [12 IV + 0 ciphertext + 16 tag] = 28 bytes
    EXPECT_EQ(ct.size_bytes(), 28);
    auto pt = decrypt_field_value(dek, std::move(ct));
    EXPECT_EQ(pt.size_bytes(), 0);
}

// =========================================================================
// Avro field transformer tests
// =========================================================================

TEST_CORO(field_transformer_avro, single_tagged_field) {
    auto schema = compile_avro_schema(R"({
        "type": "record",
        "name": "Person",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "ssn", "type": "string"},
            {"name": "age", "type": "int"}
        ]
    })");

    // Build an Avro record.
    ::avro::GenericDatum datum(schema);
    auto& rec = datum.value<::avro::GenericRecord>();
    rec.setFieldAt(0, ::avro::GenericDatum(std::string("Alice")));
    rec.setFieldAt(1, ::avro::GenericDatum(std::string("123-45-6789")));
    rec.setFieldAt(2, ::avro::GenericDatum(int32_t(30)));

    auto input = avro_serialize(datum, schema);
    auto avro_schema_ptr = std::make_shared<::avro::ValidSchema>(schema);

    encryption_schema es{
      .format = schema_format::avro,
      .handle = avro_schema_ptr,
      .tagged_fields = {
        {.path = {"ssn"}, .tag = "PII", .kek_name = "test-kek"},
      },
    };

    ref_field_transformer transformer;
    auto deks = make_single_dek_set();
    auto result = co_await transformer.transform(std::move(input), es, deks);

    // Parse the result and verify ssn is not the original plaintext.
    auto parsed = co_await serde::avro::parse(std::move(result), schema);
    auto& result_rec = std::get<serde::avro::parsed::record>(*parsed);

    // name (field 0) should be unchanged.
    auto& name_msg = *result_rec.fields[0];
    auto& name_prim = std::get<serde::avro::parsed::primitive>(name_msg);
    auto& name_buf = std::get<iobuf>(name_prim);
    EXPECT_EQ(name_buf.linearize_to_string(), "Alice");

    // age (field 2) should be unchanged.
    auto& age_msg = *result_rec.fields[2];
    auto& age_prim = std::get<serde::avro::parsed::primitive>(age_msg);
    EXPECT_EQ(std::get<int32_t>(age_prim), 30);

    // ssn (field 1) should be encrypted (not the original value).
    auto& ssn_msg = *result_rec.fields[1];
    auto& ssn_prim = std::get<serde::avro::parsed::primitive>(ssn_msg);
    auto& ssn_buf = std::get<iobuf>(ssn_prim);
    EXPECT_NE(ssn_buf.linearize_to_string(), "123-45-6789");

    // Decrypt and verify.
    auto dek = make_dek(test_dek_bytes);
    auto decrypted = decrypt_field_value(dek, ssn_buf.copy());
    EXPECT_EQ(decrypted.linearize_to_string(), "123-45-6789");
}

TEST_CORO(field_transformer_avro, multiple_tagged_fields) {
    auto schema = compile_avro_schema(R"({
        "type": "record",
        "name": "Patient",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "ssn", "type": "string"},
            {"name": "credit_card", "type": "string"},
            {"name": "age", "type": "int"}
        ]
    })");

    ::avro::GenericDatum datum(schema);
    auto& rec = datum.value<::avro::GenericRecord>();
    rec.setFieldAt(0, ::avro::GenericDatum(std::string("Bob")));
    rec.setFieldAt(1, ::avro::GenericDatum(std::string("987-65-4321")));
    rec.setFieldAt(2, ::avro::GenericDatum(std::string("4111-1111-1111-1111")));
    rec.setFieldAt(3, ::avro::GenericDatum(int32_t(45)));

    auto input = avro_serialize(datum, schema);
    auto avro_schema_ptr = std::make_shared<::avro::ValidSchema>(schema);

    encryption_schema es{
      .format = schema_format::avro,
      .handle = avro_schema_ptr,
      .tagged_fields = {
        {.path = {"ssn"}, .tag = "PII", .kek_name = "kek-pii"},
        {.path = {"credit_card"}, .tag = "FIN", .kek_name = "kek-fin"},
      },
    };

    ref_field_transformer transformer;
    auto deks = make_multi_dek_set();
    auto result = co_await transformer.transform(std::move(input), es, deks);

    auto parsed = co_await serde::avro::parse(std::move(result), schema);
    auto& result_rec = std::get<serde::avro::parsed::record>(*parsed);

    // name unchanged.
    auto& name_buf = std::get<iobuf>(
      std::get<serde::avro::parsed::primitive>(*result_rec.fields[0]));
    EXPECT_EQ(name_buf.linearize_to_string(), "Bob");

    // age unchanged.
    EXPECT_EQ(
      std::get<int32_t>(
        std::get<serde::avro::parsed::primitive>(*result_rec.fields[3])),
      45);

    // ssn encrypted with kek-pii DEK.
    auto& ssn_buf = std::get<iobuf>(
      std::get<serde::avro::parsed::primitive>(*result_rec.fields[1]));
    auto ssn_decrypted = decrypt_field_value(
      make_dek(test_dek_bytes), ssn_buf.copy());
    EXPECT_EQ(ssn_decrypted.linearize_to_string(), "987-65-4321");

    // credit_card encrypted with kek-fin DEK.
    auto& cc_buf = std::get<iobuf>(
      std::get<serde::avro::parsed::primitive>(*result_rec.fields[2]));
    auto cc_decrypted = decrypt_field_value(
      make_dek(test_dek2_bytes), cc_buf.copy());
    EXPECT_EQ(cc_decrypted.linearize_to_string(), "4111-1111-1111-1111");
}

TEST_CORO(field_transformer_avro, nested_tagged_field) {
    auto schema = compile_avro_schema(R"({
        "type": "record",
        "name": "Person",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "address", "type": {
                "type": "record",
                "name": "Address",
                "fields": [
                    {"name": "street", "type": "string"},
                    {"name": "city", "type": "string"}
                ]
            }},
            {"name": "age", "type": "int"}
        ]
    })");

    ::avro::GenericDatum datum(schema);
    auto& rec = datum.value<::avro::GenericRecord>();
    rec.setFieldAt(0, ::avro::GenericDatum(std::string("Carol")));

    // Build nested address record.
    auto addr_schema = schema.root()->leafAt(1);
    ::avro::GenericDatum addr_datum(addr_schema);
    auto& addr_rec = addr_datum.value<::avro::GenericRecord>();
    addr_rec.setFieldAt(0, ::avro::GenericDatum(std::string("123 Main St")));
    addr_rec.setFieldAt(1, ::avro::GenericDatum(std::string("Anytown")));
    rec.setFieldAt(1, addr_datum);

    rec.setFieldAt(2, ::avro::GenericDatum(int32_t(25)));

    auto input = avro_serialize(datum, schema);
    auto avro_schema_ptr = std::make_shared<::avro::ValidSchema>(schema);

    encryption_schema es{
      .format = schema_format::avro,
      .handle = avro_schema_ptr,
      .tagged_fields = {
        {.path = {"address", "street"},
         .tag = "PII",
         .kek_name = "test-kek"},
      },
    };

    ref_field_transformer transformer;
    auto deks = make_single_dek_set();
    auto result = co_await transformer.transform(std::move(input), es, deks);

    auto parsed = co_await serde::avro::parse(std::move(result), schema);
    auto& result_rec = std::get<serde::avro::parsed::record>(*parsed);

    // name unchanged.
    auto& name_buf = std::get<iobuf>(
      std::get<serde::avro::parsed::primitive>(*result_rec.fields[0]));
    EXPECT_EQ(name_buf.linearize_to_string(), "Carol");

    // address.city unchanged.
    auto& addr = std::get<serde::avro::parsed::record>(*result_rec.fields[1]);
    auto& city_buf = std::get<iobuf>(
      std::get<serde::avro::parsed::primitive>(*addr.fields[1]));
    EXPECT_EQ(city_buf.linearize_to_string(), "Anytown");

    // address.street encrypted.
    auto& street_buf = std::get<iobuf>(
      std::get<serde::avro::parsed::primitive>(*addr.fields[0]));
    EXPECT_NE(street_buf.linearize_to_string(), "123 Main St");
    auto decrypted = decrypt_field_value(
      make_dek(test_dek_bytes), street_buf.copy());
    EXPECT_EQ(decrypted.linearize_to_string(), "123 Main St");
}

TEST_CORO(field_transformer_avro, no_tagged_fields_passthrough) {
    auto schema = compile_avro_schema(R"({
        "type": "record",
        "name": "Simple",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"}
        ]
    })");

    ::avro::GenericDatum datum(schema);
    auto& rec = datum.value<::avro::GenericRecord>();
    rec.setFieldAt(0, ::avro::GenericDatum(std::string("Dave")));
    rec.setFieldAt(1, ::avro::GenericDatum(int32_t(40)));

    auto input = avro_serialize(datum, schema);
    auto input_copy = input.copy();
    auto avro_schema_ptr = std::make_shared<::avro::ValidSchema>(schema);

    encryption_schema es{
      .format = schema_format::avro,
      .handle = avro_schema_ptr,
      .tagged_fields = {},
    };

    ref_field_transformer transformer;
    auto deks = make_single_dek_set();
    auto result = co_await transformer.transform(std::move(input), es, deks);

    // With no tagged fields, output should be exactly the input.
    EXPECT_EQ(result, input_copy);
}

TEST_CORO(field_transformer_avro, null_field_not_encrypted) {
    auto schema = compile_avro_schema(R"({
        "type": "record",
        "name": "OptRecord",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "ssn", "type": ["null", "string"]}
        ]
    })");

    ::avro::GenericDatum datum(schema);
    auto& rec = datum.value<::avro::GenericRecord>();
    rec.setFieldAt(0, ::avro::GenericDatum(std::string("Eve")));
    // ssn is null (branch 0 of the union is null).
    ::avro::GenericDatum ssn_datum(schema.root()->leafAt(1));
    // Default is null branch.
    rec.setFieldAt(1, ssn_datum);

    auto input = avro_serialize(datum, schema);
    auto avro_schema_ptr = std::make_shared<::avro::ValidSchema>(schema);

    encryption_schema es{
      .format = schema_format::avro,
      .handle = avro_schema_ptr,
      .tagged_fields = {
        {.path = {"ssn"}, .tag = "PII", .kek_name = "test-kek"},
      },
    };

    ref_field_transformer transformer;
    auto deks = make_single_dek_set();

    // Should not crash. The null field is simply left as-is.
    auto result = co_await transformer.transform(std::move(input), es, deks);

    auto parsed = co_await serde::avro::parse(std::move(result), schema);
    // Verify we got a valid parse result (no crash).
    EXPECT_TRUE(parsed != nullptr);
}

// =========================================================================
// Protobuf field transformer tests
// =========================================================================

TEST_CORO(field_transformer_proto, single_tagged_field) {
    proto_schema ps;
    auto* desc = ps.build_simple();
    EXPECT_NE(desc, nullptr);
    if (desc == nullptr) {
        co_return;
    }

    google::protobuf::DynamicMessageFactory factory;
    auto msg = std::unique_ptr<google::protobuf::Message>(
      factory.GetPrototype(desc)->New());
    auto* reflect = msg->GetReflection();

    reflect->SetString(msg.get(), desc->FindFieldByName("name"), "Alice");
    reflect->SetString(msg.get(), desc->FindFieldByName("ssn"), "123-45-6789");
    reflect->SetInt32(msg.get(), desc->FindFieldByName("age"), 30);

    auto input = pb_serialize(*msg);

    encryption_schema es{
      .format = schema_format::protobuf,
      .handle = desc,
      .tagged_fields = {
        {.path = {"ssn"}, .tag = "PII", .kek_name = "test-kek"},
      },
    };

    ref_field_transformer transformer;
    auto deks = make_single_dek_set();
    auto result = co_await transformer.transform(std::move(input), es, deks);

    // Parse back using our protobuf parser.
    auto parsed = co_await serde::pb::parse(std::move(result), *desc);

    // name (field 1) unchanged.
    auto name_it = parsed->fields.find(1);
    EXPECT_NE(name_it, parsed->fields.end());
    auto& name_buf = std::get<iobuf>(name_it->second);
    EXPECT_EQ(name_buf.linearize_to_string(), "Alice");

    // age (field 3) unchanged.
    auto age_it = parsed->fields.find(3);
    EXPECT_NE(age_it, parsed->fields.end());
    EXPECT_EQ(std::get<int32_t>(age_it->second), 30);

    // ssn (field 2) encrypted.
    auto ssn_it = parsed->fields.find(2);
    EXPECT_NE(ssn_it, parsed->fields.end());
    auto& ssn_buf = std::get<iobuf>(ssn_it->second);
    EXPECT_NE(ssn_buf.linearize_to_string(), "123-45-6789");

    auto decrypted = decrypt_field_value(
      make_dek(test_dek_bytes), ssn_buf.copy());
    EXPECT_EQ(decrypted.linearize_to_string(), "123-45-6789");
}

TEST_CORO(field_transformer_proto, nested_tagged_field) {
    proto_schema ps;
    auto* desc = ps.build_nested();
    EXPECT_NE(desc, nullptr);
    if (desc == nullptr) {
        co_return;
    }

    google::protobuf::DynamicMessageFactory factory;
    auto msg = std::unique_ptr<google::protobuf::Message>(
      factory.GetPrototype(desc)->New());
    auto* reflect = msg->GetReflection();

    reflect->SetString(msg.get(), desc->FindFieldByName("name"), "Bob");
    reflect->SetInt32(msg.get(), desc->FindFieldByName("age"), 35);

    auto* addr_field = desc->FindFieldByName("address");
    auto* addr_msg = reflect->MutableMessage(msg.get(), addr_field);
    auto* addr_desc = addr_field->message_type();
    auto* addr_reflect = addr_msg->GetReflection();
    addr_reflect->SetString(
      addr_msg, addr_desc->FindFieldByName("street"), "456 Oak Ave");
    addr_reflect->SetString(
      addr_msg, addr_desc->FindFieldByName("city"), "Springfield");

    auto input = pb_serialize(*msg);

    encryption_schema es{
      .format = schema_format::protobuf,
      .handle = desc,
      .tagged_fields = {
        {.path = {"address", "street"},
         .tag = "PII",
         .kek_name = "test-kek"},
      },
    };

    ref_field_transformer transformer;
    auto deks = make_single_dek_set();
    auto result = co_await transformer.transform(std::move(input), es, deks);

    auto parsed = co_await serde::pb::parse(std::move(result), *desc);

    // name unchanged.
    auto& name_buf = std::get<iobuf>(parsed->fields.at(1));
    EXPECT_EQ(name_buf.linearize_to_string(), "Bob");

    // address sub-message.
    auto& addr = *std::get<std::unique_ptr<serde::pb::parsed::message>>(
      parsed->fields.at(2));

    // city unchanged.
    auto& city_buf = std::get<iobuf>(addr.fields.at(2));
    EXPECT_EQ(city_buf.linearize_to_string(), "Springfield");

    // street encrypted.
    auto& street_buf = std::get<iobuf>(addr.fields.at(1));
    EXPECT_NE(street_buf.linearize_to_string(), "456 Oak Ave");
    auto decrypted = decrypt_field_value(
      make_dek(test_dek_bytes), street_buf.copy());
    EXPECT_EQ(decrypted.linearize_to_string(), "456 Oak Ave");
}

// =========================================================================
// JSON field transformer tests
// =========================================================================

TEST_CORO(field_transformer_json, single_tagged_field) {
    auto input = iobuf::from(
      R"({"name":"Alice","ssn":"123-45-6789","age":30})");

    encryption_schema es{
      .format = schema_format::json,
      .handle = std::monostate{},
      .tagged_fields = {
        {.path = {"ssn"}, .tag = "PII", .kek_name = "test-kek"},
      },
    };

    ref_field_transformer transformer;
    auto deks = make_single_dek_set();
    auto result = co_await transformer.transform(std::move(input), es, deks);

    // Parse the result JSON and check fields.
    auto p = serde::json::parser(std::move(result));
    ss::sstring name_val;
    ss::sstring ssn_val;
    int64_t age_val = 0;
    ss::sstring current_key;

    while (co_await p.next()) {
        if (p.token() == serde::json::token::key) {
            current_key = p.value_string().linearize_to_string();
        } else if (p.token() == serde::json::token::value_string) {
            if (current_key == "name") {
                name_val = p.value_string().linearize_to_string();
            } else if (current_key == "ssn") {
                ssn_val = p.value_string().linearize_to_string();
            }
        } else if (p.token() == serde::json::token::value_int) {
            if (current_key == "age") {
                age_val = p.value_int();
            }
        }
    }

    EXPECT_EQ(name_val, "Alice");
    EXPECT_EQ(age_val, 30);
    // ssn should be a base64-encoded encrypted value, not the original.
    EXPECT_NE(ssn_val, "123-45-6789");

    // Decode base64 and decrypt.
    auto ct_bytes = base64_to_bytes(ssn_val);
    iobuf ct_buf;
    ct_buf.append(ct_bytes.data(), ct_bytes.size());
    auto decrypted = decrypt_field_value(
      make_dek(test_dek_bytes), std::move(ct_buf));
    EXPECT_EQ(decrypted.linearize_to_string(), "123-45-6789");
}

TEST_CORO(field_transformer_json, nested_tagged_field) {
    auto input = iobuf::from(
      R"({"name":"Bob","address":{"street":"789 Elm St","city":"Portland"},"age":40})");

    encryption_schema es{
      .format = schema_format::json,
      .handle = std::monostate{},
      .tagged_fields = {
        {.path = {"address", "street"},
         .tag = "PII",
         .kek_name = "test-kek"},
      },
    };

    ref_field_transformer transformer;
    auto deks = make_single_dek_set();
    auto result = co_await transformer.transform(std::move(input), es, deks);

    // Parse and verify by walking the JSON tokens.
    auto p = serde::json::parser(std::move(result));
    ss::sstring name_val;
    ss::sstring street_val;
    ss::sstring city_val;
    ss::sstring current_key;
    bool in_address = false;

    while (co_await p.next()) {
        switch (p.token()) {
        case serde::json::token::key:
            current_key = p.value_string().linearize_to_string();
            if (current_key == "address") {
                in_address = true;
            }
            break;
        case serde::json::token::value_string:
            if (current_key == "name") {
                name_val = p.value_string().linearize_to_string();
            } else if (current_key == "street" && in_address) {
                street_val = p.value_string().linearize_to_string();
            } else if (current_key == "city" && in_address) {
                city_val = p.value_string().linearize_to_string();
            }
            break;
        case serde::json::token::end_object:
            if (in_address && current_key != "address") {
                in_address = false;
            }
            break;
        default:
            break;
        }
    }

    EXPECT_EQ(name_val, "Bob");
    EXPECT_EQ(city_val, "Portland");
    // street should be encrypted (base64).
    EXPECT_NE(street_val, "789 Elm St");

    auto ct_bytes = base64_to_bytes(street_val);
    iobuf ct_buf;
    ct_buf.append(ct_bytes.data(), ct_bytes.size());
    auto decrypted = decrypt_field_value(
      make_dek(test_dek_bytes), std::move(ct_buf));
    EXPECT_EQ(decrypted.linearize_to_string(), "789 Elm St");
}

TEST_CORO(field_transformer_json, array_of_objects_tagged_field) {
    auto input = iobuf::from(
      R"({"people":[{"ssn":"111-11-1111"},{"ssn":"222-22-2222"}]})");

    encryption_schema es{
      .format = schema_format::json,
      .handle = std::monostate{},
      .tagged_fields = {
        {.path = {"people", "ssn"}, .tag = "PII", .kek_name = "test-kek"},
      },
    };

    ref_field_transformer transformer;
    auto deks = make_single_dek_set();
    auto result = co_await transformer.transform(std::move(input), es, deks);

    // Parse the result and collect all ssn values from the array.
    auto p = serde::json::parser(std::move(result));
    ss::sstring current_key;
    std::vector<ss::sstring> ssn_vals;

    while (co_await p.next()) {
        if (p.token() == serde::json::token::key) {
            current_key = p.value_string().linearize_to_string();
        } else if (
          p.token() == serde::json::token::value_string
          && current_key == "ssn") {
            ssn_vals.push_back(p.value_string().linearize_to_string());
        }
    }

    // Both ssn values must be present and encrypted.
    EXPECT_EQ(ssn_vals.size(), 2);
    if (ssn_vals.size() != 2) {
        co_return;
    }

    // Neither should be the original plaintext.
    EXPECT_NE(ssn_vals[0], "111-11-1111");
    EXPECT_NE(ssn_vals[1], "222-22-2222");

    // Decrypt both and verify round-trip.
    auto dek = make_dek(test_dek_bytes);
    {
        auto ct_b = base64_to_bytes(ssn_vals[0]);
        iobuf ct_buf;
        ct_buf.append(ct_b.data(), ct_b.size());
        auto decrypted = decrypt_field_value(dek, std::move(ct_buf));
        EXPECT_EQ(decrypted.linearize_to_string(), "111-11-1111");
    }
    {
        auto ct_b = base64_to_bytes(ssn_vals[1]);
        iobuf ct_buf;
        ct_buf.append(ct_b.data(), ct_b.size());
        auto decrypted = decrypt_field_value(dek, std::move(ct_buf));
        EXPECT_EQ(decrypted.linearize_to_string(), "222-22-2222");
    }
}

TEST_CORO(field_transformer_json, null_field_not_encrypted) {
    auto input = iobuf::from(R"({"name":"Eve","ssn":null})");

    encryption_schema es{
      .format = schema_format::json,
      .handle = std::monostate{},
      .tagged_fields = {
        {.path = {"ssn"}, .tag = "PII", .kek_name = "test-kek"},
      },
    };

    ref_field_transformer transformer;
    auto deks = make_single_dek_set();
    // Should not crash when encountering null for a tagged field.
    auto result = co_await transformer.transform(std::move(input), es, deks);

    // Verify the output contains null for ssn.
    auto result_str = result.linearize_to_string();
    auto p = serde::json::parser(std::move(result));
    ss::sstring current_key;
    bool ssn_is_null = false;

    while (co_await p.next()) {
        if (p.token() == serde::json::token::key) {
            current_key = p.value_string().linearize_to_string();
        } else if (
          p.token() == serde::json::token::value_null && current_key == "ssn") {
            ssn_is_null = true;
        }
    }
    EXPECT_TRUE(ssn_is_null);
}

} // namespace
} // namespace encryption
