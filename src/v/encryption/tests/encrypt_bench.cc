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

#include "encryption/field_transformer.h"
#include "encryption/field_transformer_ref.h"
#include "encryption/types.h"

#include <seastar/testing/perf_tests.hh>

#include <avro/Compiler.hh>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>

#include <array>
#include <memory>
#include <string>
#include <vector>

namespace {

constexpr std::array<uint8_t, 32> test_dek_bytes = {
  0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA,
  0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5,
  0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF,
};

encryption::dek_set make_dek_set() {
    encryption::dek_set deks;
    deks.emplace(
      "test-kek",
      encryption::dek_state{
        .plaintext_dek = bytes(test_dek_bytes.data(), test_dek_bytes.size()),
        .encrypted_dek = {},
        .algorithm = encryption::dek_algorithm::aes256_gcm,
        .kek_name = "test-kek",
        .kms_type = "mock",
        .kms_key_id = "test-key",
        .version = 1,
        .created_at = model::timestamp::now(),
        .expiry = std::nullopt,
      });
    return deks;
}

std::string make_padding(size_t len) {
    std::string s;
    s.reserve(len);
    for (size_t i = 0; i < len; ++i) {
        s.push_back(static_cast<char>('A' + (i % 26)));
    }
    return s;
}

// =========================================================================
// Avro helpers
// =========================================================================

/// Build a schema with many string fields (to represent a realistic record
/// where most fields are NOT encrypted) plus one encrypted field "ssn".
///
/// field_count controls the total number of string fields. The first field
/// is "ssn" (encrypted), the rest are padding fields filled to reach the
/// target record size.
::avro::ValidSchema build_avro_schema(size_t field_count) {
    std::string json = R"({"type":"record","name":"BenchRecord","fields":[)";
    json += R"({"name":"ssn","type":"string"})";
    for (size_t i = 1; i < field_count; ++i) {
        json += fmt::format(R"(,{{"name":"field{}","type":"string"}})", i);
    }
    json += "]}";
    return ::avro::compileJsonSchemaFromString(json);
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

/// Build an Avro record of approximately target_size bytes.
/// ssn is always a short string (the encrypted field). The remaining
/// fields are filled with padding strings distributed evenly.
iobuf build_avro_record(
  const ::avro::ValidSchema& schema, size_t field_count, size_t target_size) {
    ::avro::GenericDatum datum(schema);
    auto& rec = datum.value<::avro::GenericRecord>();

    // ssn field (the one that gets encrypted)
    rec.setFieldAt(0, ::avro::GenericDatum(std::string("123-45-6789")));

    // Distribute remaining bytes across padding fields
    size_t overhead = 20; // rough estimate for ssn + varint lengths
    size_t remaining = target_size > overhead ? target_size - overhead : 0;
    size_t pad_count = field_count - 1;
    size_t per_field = pad_count > 0 ? remaining / pad_count : 0;

    for (size_t i = 1; i < field_count; ++i) {
        rec.setFieldAt(
          static_cast<size_t>(i),
          ::avro::GenericDatum(make_padding(per_field)));
    }

    return avro_serialize(datum, schema);
}

// =========================================================================
// Protobuf helpers
// =========================================================================

struct proto_test_schema {
    google::protobuf::DescriptorPool pool;
    const google::protobuf::FileDescriptor* file{nullptr};
    google::protobuf::DynamicMessageFactory factory;

    /// Build a message with field_count string fields.
    /// Field 1 = ssn (encrypted), fields 2..N = padding.
    const google::protobuf::Descriptor* build(size_t field_count) {
        google::protobuf::FileDescriptorProto file_proto;
        file_proto.set_name("bench.proto");
        file_proto.set_syntax("proto3");

        auto* msg = file_proto.add_message_type();
        msg->set_name("BenchRecord");

        auto* f_ssn = msg->add_field();
        f_ssn->set_name("ssn");
        f_ssn->set_number(1);
        f_ssn->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
        f_ssn->set_label(
          google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);

        for (size_t i = 1; i < field_count; ++i) {
            auto* f = msg->add_field();
            f->set_name(fmt::format("field{}", i));
            f->set_number(static_cast<int>(i + 1));
            f->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
            f->set_label(
              google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
        }

        file = pool.BuildFile(file_proto);
        return file->FindMessageTypeByName("BenchRecord");
    }
};

iobuf build_proto_record(
  proto_test_schema& schema,
  const google::protobuf::Descriptor* desc,
  size_t field_count,
  size_t target_size) {
    auto* prototype = schema.factory.GetPrototype(desc);
    std::unique_ptr<google::protobuf::Message> msg(prototype->New());
    auto* refl = msg->GetReflection();

    // ssn field
    auto* ssn_fd = desc->FindFieldByName("ssn");
    refl->SetString(msg.get(), ssn_fd, "123-45-6789");

    size_t overhead = 20;
    size_t remaining = target_size > overhead ? target_size - overhead : 0;
    size_t pad_count = field_count - 1;
    size_t per_field = pad_count > 0 ? remaining / pad_count : 0;

    for (size_t i = 1; i < field_count; ++i) {
        auto* fd = desc->FindFieldByName(fmt::format("field{}", i));
        refl->SetString(msg.get(), fd, make_padding(per_field));
    }

    auto s = msg->SerializeAsString();
    iobuf buf;
    buf.append(s.data(), s.size());
    return buf;
}

// =========================================================================
// JSON helpers
// =========================================================================

iobuf build_json_record(size_t field_count, size_t target_size) {
    std::string json = R"({"ssn":"123-45-6789")";

    size_t overhead = json.size() + 1; // +1 for closing brace
    size_t remaining = target_size > overhead ? target_size - overhead : 0;
    size_t pad_count = field_count - 1;
    size_t per_field = pad_count > 0 ? remaining / pad_count : 0;

    for (size_t i = 1; i < field_count; ++i) {
        json += fmt::format(R"(,"field{}":"{}")", i, make_padding(per_field));
    }
    json += "}";

    iobuf buf;
    buf.append(json.data(), json.size());
    return buf;
}

} // namespace

// =========================================================================
// Benchmark fixture
// =========================================================================
//
// Each record has many string fields (simulating a real record) with only
// one field ("ssn") tagged for encryption. This is the typical case where
// the zero-copy optimization matters: most fields are passed through
// unchanged.

// Lazy-initialized fixture: avoids paying the cost of building all test data
// up-front (and avoids exceptions in the constructor path which would silently
// cause the framework to skip the test).
struct encrypt_bench {
    static constexpr size_t small_fields = 5;
    static constexpr size_t medium_fields = 10;
    static constexpr size_t large_fields = 15;

    static constexpr size_t small_size = 100;
    static constexpr size_t medium_size = 1024;
    static constexpr size_t large_size = 10240;

    // Transformer
    encryption::ref_field_transformer ref;

    // DEK
    encryption::dek_set deks{make_dek_set()};

    // Avro
    ::avro::ValidSchema avro_small_schema{build_avro_schema(small_fields)};
    ::avro::ValidSchema avro_medium_schema{build_avro_schema(medium_fields)};
    ::avro::ValidSchema avro_large_schema{build_avro_schema(large_fields)};
    std::shared_ptr<::avro::ValidSchema> avro_small_schema_ptr{
      std::make_shared<::avro::ValidSchema>(avro_small_schema)};
    std::shared_ptr<::avro::ValidSchema> avro_medium_schema_ptr{
      std::make_shared<::avro::ValidSchema>(avro_medium_schema)};
    std::shared_ptr<::avro::ValidSchema> avro_large_schema_ptr{
      std::make_shared<::avro::ValidSchema>(avro_large_schema)};
    iobuf avro_100b{
      build_avro_record(avro_small_schema, small_fields, small_size)};
    iobuf avro_1kb{
      build_avro_record(avro_medium_schema, medium_fields, medium_size)};
    iobuf avro_10kb{
      build_avro_record(avro_large_schema, large_fields, large_size)};

    encryption::encryption_schema avro_es_small{
      make_avro_es(avro_small_schema_ptr)};
    encryption::encryption_schema avro_es_medium{
      make_avro_es(avro_medium_schema_ptr)};
    encryption::encryption_schema avro_es_large{
      make_avro_es(avro_large_schema_ptr)};

    // Protobuf
    proto_test_schema pb_schema_small;
    proto_test_schema pb_schema_medium;
    proto_test_schema pb_schema_large;
    const google::protobuf::Descriptor* pb_desc_small{
      pb_schema_small.build(small_fields)};
    const google::protobuf::Descriptor* pb_desc_medium{
      pb_schema_medium.build(medium_fields)};
    const google::protobuf::Descriptor* pb_desc_large{
      pb_schema_large.build(large_fields)};
    iobuf proto_100b{build_proto_record(
      pb_schema_small, pb_desc_small, small_fields, small_size)};
    iobuf proto_1kb{build_proto_record(
      pb_schema_medium, pb_desc_medium, medium_fields, medium_size)};
    iobuf proto_10kb{build_proto_record(
      pb_schema_large, pb_desc_large, large_fields, large_size)};
    encryption::encryption_schema pb_es_small{make_pb_es(pb_desc_small)};
    encryption::encryption_schema pb_es_medium{make_pb_es(pb_desc_medium)};
    encryption::encryption_schema pb_es_large{make_pb_es(pb_desc_large)};

    // JSON
    iobuf json_100b{build_json_record(small_fields, small_size)};
    iobuf json_1kb{build_json_record(medium_fields, medium_size)};
    iobuf json_10kb{build_json_record(large_fields, large_size)};
    encryption::encryption_schema json_es{
      .format = encryption::schema_format::json,
      .handle = std::monostate{},
      .tagged_fields = {
        {.path = {"ssn"}, .tag = "PII", .kek_name = "test-kek"},
      },
    };

    static encryption::encryption_schema
    make_avro_es(std::shared_ptr<::avro::ValidSchema>& ptr) {
        return encryption::encryption_schema{
          .format = encryption::schema_format::avro,
          .handle = ptr,
          .tagged_fields = {
            {.path = {"ssn"}, .tag = "PII", .kek_name = "test-kek"},
          },
        };
    }

    static encryption::encryption_schema
    make_pb_es(const google::protobuf::Descriptor* desc) {
        return encryption::encryption_schema{
          .format = encryption::schema_format::protobuf,
          .handle = desc,
          .tagged_fields = {
            {.path = {"ssn"}, .tag = "PII", .kek_name = "test-kek"},
          },
        };
    }
};

// =========================================================================
// Avro benchmarks: ref (Implementation A) vs opt (Implementation C)
// =========================================================================

PERF_TEST_F(encrypt_bench, avro_ref_100b) {
    return ref.transform(avro_100b.copy(), avro_es_small, deks)
      .then([](iobuf result) { perf_tests::do_not_optimize(result); });
}

PERF_TEST_F(encrypt_bench, avro_ref_1kb) {
    return ref.transform(avro_1kb.copy(), avro_es_medium, deks)
      .then([](iobuf result) { perf_tests::do_not_optimize(result); });
}

PERF_TEST_F(encrypt_bench, avro_ref_10kb) {
    return ref.transform(avro_10kb.copy(), avro_es_large, deks)
      .then([](iobuf result) { perf_tests::do_not_optimize(result); });
}

// =========================================================================
// Protobuf benchmarks
// =========================================================================

PERF_TEST_F(encrypt_bench, proto_ref_100b) {
    return ref.transform(proto_100b.copy(), pb_es_small, deks)
      .then([](iobuf result) { perf_tests::do_not_optimize(result); });
}

PERF_TEST_F(encrypt_bench, proto_ref_1kb) {
    return ref.transform(proto_1kb.copy(), pb_es_medium, deks)
      .then([](iobuf result) { perf_tests::do_not_optimize(result); });
}

PERF_TEST_F(encrypt_bench, proto_ref_10kb) {
    return ref.transform(proto_10kb.copy(), pb_es_large, deks)
      .then([](iobuf result) { perf_tests::do_not_optimize(result); });
}

// =========================================================================
// JSON benchmarks
// =========================================================================

PERF_TEST_F(encrypt_bench, json_ref_100b) {
    return ref.transform(json_100b.copy(), json_es, deks)
      .then([](iobuf result) { perf_tests::do_not_optimize(result); });
}

PERF_TEST_F(encrypt_bench, json_ref_1kb) {
    return ref.transform(json_1kb.copy(), json_es, deks).then([](iobuf result) {
        perf_tests::do_not_optimize(result);
    });
}

PERF_TEST_F(encrypt_bench, json_ref_10kb) {
    return ref.transform(json_10kb.copy(), json_es, deks)
      .then([](iobuf result) { perf_tests::do_not_optimize(result); });
}
