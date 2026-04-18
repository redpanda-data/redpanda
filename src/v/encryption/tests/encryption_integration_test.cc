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

/// Integration test exercising the full produce -> encrypt -> store -> fetch ->
/// verify round-trip using the encrypting_partition_proxy with real AES-GCM
/// encryption, real DEK management (mock KMS), and all three schema formats:
/// Avro, Protobuf, and JSON.

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "cluster/partition_probe.h"
#include "cluster/types.h"
#include "encryption/dek_manager.h"
#include "encryption/dek_refill.h"
#include "encryption/encryption_metadata_ser.h"
#include "encryption/field_transformer.h"
#include "encryption/field_transformer_ref.h"
#include "encryption/mock_kms_provider.h"
#include "encryption/schema_resolver.h"
#include "encryption/types.h"
#include "kafka/data/encrypting_partition_proxy.h"
#include "kafka/data/partition_proxy.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "raft/replicate.h"
#include "serde/avro/parser.h"
#include "serde/json/parser.h"
#include "serde/protobuf/parser.h"
#include "storage/record_batch_builder.h"
#include "test_utils/test.h"
#include "utils/base64.h"

#include <seastar/core/future.hh>

#include <avro/Compiler.hh>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <gtest/gtest.h>

#include <array>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace {

// =========================================================================
// Mock partition proxy (same pattern as encrypting_proxy_test.cc)
// =========================================================================

struct noop_partition_probe_impl final : cluster::partition_probe::impl {
    void add_records_produced(uint64_t) override {}
    void add_records_fetched(uint64_t) override {}
    void add_bytes_produced(uint64_t) override {}
    void add_batches_produced(uint64_t) override {}
    void add_bytes_fetched(uint64_t) override {}
    void add_bytes_fetched_from_follower(uint64_t) override {}
    void add_schema_id_validation_failed() override {}
    void setup_metrics(const model::ntp&) override {}
    void clear_metrics() override {}
};

class mock_partition_proxy final : public kafka::partition_proxy::impl {
public:
    model::ntp test_ntp{
      model::ns{"kafka"}, model::topic{"test-topic"}, model::partition_id{0}};
    cluster::partition_probe probe_{
      std::make_unique<noop_partition_probe_impl>()};

    std::optional<chunked_vector<model::record_batch>> last_batches;
    result<model::offset> replicate_result_{model::offset{42}};

    const model::ntp& ntp() const override { return test_ntp; }

    ss::future<result<model::offset, kafka::error_code>>
    sync_effective_start(model::timeout_clock::duration) override {
        co_return model::offset{0};
    }

    model::offset local_start_offset() const override {
        return model::offset{0};
    }
    model::offset start_offset() const override { return model::offset{0}; }
    model::offset high_watermark() const override { return model::offset{0}; }

    checked<model::offset, kafka::error_code>
    last_stable_offset() const override {
        return model::offset{0};
    }

    kafka::leader_epoch leader_epoch() const override {
        return kafka::leader_epoch{0};
    }

    ss::future<std::optional<model::offset>>
    get_leader_epoch_last_offset(kafka::leader_epoch) const override {
        co_return std::nullopt;
    }

    bool is_leader() const override { return true; }

    ss::future<std::error_code> linearizable_barrier() override {
        co_return std::error_code{};
    }

    ss::future<kafka::error_code>
    prefix_truncate(model::offset, ss::lowres_clock::time_point) override {
        co_return kafka::error_code::none;
    }

    ss::future<storage::translating_reader>
    make_reader(kafka::log_reader_config) override {
        std::terminate();
    }

    ss::future<std::optional<storage::timequery_result>>
    timequery(storage::timequery_config) override {
        co_return std::nullopt;
    }

    ss::future<std::vector<model::tx_range>> aborted_transactions(
      model::offset,
      model::offset,
      ss::lw_shared_ptr<const storage::offset_translator_state>) override {
        co_return std::vector<model::tx_range>{};
    }

    ss::future<kafka::error_code> validate_fetch_offset(
      model::offset, bool, model::timeout_clock::time_point) override {
        co_return kafka::error_code::none;
    }

    ss::future<result<model::offset>> replicate(
      chunked_vector<model::record_batch> batches,
      raft::replicate_options) override {
        last_batches.emplace(std::move(batches));
        co_return replicate_result_;
    }

    raft::replicate_stages replicate(
      model::batch_identity,
      model::record_batch batch,
      raft::replicate_options) override {
        chunked_vector<model::record_batch> v;
        v.push_back(std::move(batch));
        last_batches.emplace(std::move(v));
        ss::promise<result<raft::replicate_result>> p;
        auto f = p.get_future();
        raft::replicate_result rr{
          .last_offset = model::offset{42}, .last_term = model::term_id{1}};
        p.set_value(rr);
        return raft::replicate_stages(ss::make_ready_future<>(), std::move(f));
    }

    std::unique_ptr<kafka::exact_offset_replicator>
      make_exact_offset_replicator() && override {
        return nullptr;
    }

    result<kafka::partition_info> get_partition_info() const override {
        return kafka::partition_info{};
    }

    size_t estimate_size_between(kafka::offset, kafka::offset) const override {
        return 0;
    }

    cluster::partition_probe& probe() override { return probe_; }

    size_t local_size_bytes() const override { return 0; }

    ss::future<std::optional<size_t>> cloud_size_bytes() const override {
        co_return std::nullopt;
    }

    model::offset offset_lag() const override { return model::offset{0}; }

    ss::future<cluster::partition_cloud_storage_status>
    get_cloud_storage_status() const override {
        co_return cluster::partition_cloud_storage_status{};
    }
};

// =========================================================================
// Schema-registry wire format helpers
// =========================================================================

iobuf prepend_schema_prefix(iobuf value, int32_t schema_id = 1) {
    iobuf result;
    uint8_t magic = 0x00;
    result.append(&magic, 1);
    uint8_t id_bytes[4];
    id_bytes[0] = static_cast<uint8_t>((schema_id >> 24) & 0xFF);
    id_bytes[1] = static_cast<uint8_t>((schema_id >> 16) & 0xFF);
    id_bytes[2] = static_cast<uint8_t>((schema_id >> 8) & 0xFF);
    id_bytes[3] = static_cast<uint8_t>(schema_id & 0xFF);
    result.append(id_bytes, 4);
    result.append(std::move(value));
    return result;
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

struct proto_schema {
    google::protobuf::DescriptorPool pool;
    const google::protobuf::FileDescriptor* file{nullptr};

    const google::protobuf::Descriptor* build() {
        google::protobuf::FileDescriptorProto file_proto;
        file_proto.set_name("test.proto");
        file_proto.set_syntax("proto3");

        auto* msg = file_proto.add_message_type();
        msg->set_name("PersonalData");

        auto* f1 = msg->add_field();
        f1->set_name("id");
        f1->set_number(1);
        f1->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
        f1->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);

        auto* f2 = msg->add_field();
        f2->set_name("name");
        f2->set_number(2);
        f2->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
        f2->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);

        auto* f3 = msg->add_field();
        f3->set_name("birthday");
        f3->set_number(3);
        f3->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
        f3->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);

        auto* f4 = msg->add_field();
        f4->set_name("ssn");
        f4->set_number(4);
        f4->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
        f4->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);

        file = pool.BuildFile(file_proto);
        return file->FindMessageTypeByName("PersonalData");
    }
};

iobuf pb_serialize(const google::protobuf::Message& msg) {
    auto s = msg.SerializeAsString();
    iobuf buf;
    buf.append(s.data(), s.size());
    return buf;
}

// =========================================================================
// Registration helpers
// =========================================================================

/// Register Avro encryption rules: birthday and ssn tagged PII.
void register_avro_rules(
  encryption::schema_resolver& resolver, const ::avro::ValidSchema& schema) {
    encryption::topic_encryption_config config;
    config.format = encryption::schema_format::avro;
    config.handle = std::make_shared<::avro::ValidSchema>(schema);
    config.rules.push_back(
      encryption::encryption_rule{.tag = "PII", .kek_name = "test-kek"});
    config.field_tags.push_back(
      encryption::field_tag_mapping{.path = {"birthday"}, .tag = "PII"});
    config.field_tags.push_back(
      encryption::field_tag_mapping{.path = {"ssn"}, .tag = "PII"});
    resolver.register_rules(model::topic{"test-topic"}, std::move(config));
}

/// Register Protobuf encryption rules: birthday and ssn tagged PII.
void register_proto_rules(
  encryption::schema_resolver& resolver,
  const google::protobuf::Descriptor* desc) {
    encryption::topic_encryption_config config;
    config.format = encryption::schema_format::protobuf;
    config.handle = desc;
    config.rules.push_back(
      encryption::encryption_rule{.tag = "PII", .kek_name = "test-kek"});
    config.field_tags.push_back(
      encryption::field_tag_mapping{.path = {"birthday"}, .tag = "PII"});
    config.field_tags.push_back(
      encryption::field_tag_mapping{.path = {"ssn"}, .tag = "PII"});
    resolver.register_rules(model::topic{"test-topic"}, std::move(config));
}

/// Register JSON encryption rules: ssn tagged PII.
void register_json_rules(encryption::schema_resolver& resolver) {
    encryption::topic_encryption_config config;
    config.format = encryption::schema_format::json;
    config.handle = std::monostate{};
    config.rules.push_back(
      encryption::encryption_rule{.tag = "PII", .kek_name = "test-kek"});
    config.field_tags.push_back(
      encryption::field_tag_mapping{.path = {"ssn"}, .tag = "PII"});
    resolver.register_rules(model::topic{"test-topic"}, std::move(config));
}

/// Build a batch of N Avro-encoded records with schema-registry prefix.
model::record_batch make_avro_batch(
  const ::avro::ValidSchema& schema, size_t count, int32_t schema_id = 1) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});
    for (size_t i = 0; i < count; ++i) {
        ::avro::GenericDatum datum(schema);
        auto& rec = datum.value<::avro::GenericRecord>();
        rec.setFieldAt(
          0, ::avro::GenericDatum(std::string("id-" + std::to_string(i))));
        rec.setFieldAt(
          1, ::avro::GenericDatum(std::string("Person-" + std::to_string(i))));
        rec.setFieldAt(
          2, ::avro::GenericDatum(std::string("1990-01-" + std::to_string(i))));
        rec.setFieldAt(
          3,
          ::avro::GenericDatum(
            std::string("123-45-" + std::to_string(6780 + i))));

        auto serialized = avro_serialize(datum, schema);
        auto prefixed = prepend_schema_prefix(std::move(serialized), schema_id);
        builder.add_raw_kv(std::nullopt, std::move(prefixed));
    }
    return std::move(builder).build();
}

/// Build a batch of N Protobuf-encoded records with schema-registry prefix.
model::record_batch
make_proto_batch(const google::protobuf::Descriptor* desc, size_t count) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});
    google::protobuf::DynamicMessageFactory factory;
    for (size_t i = 0; i < count; ++i) {
        auto msg = std::unique_ptr<google::protobuf::Message>(
          factory.GetPrototype(desc)->New());
        auto* reflect = msg->GetReflection();
        reflect->SetString(
          msg.get(), desc->FindFieldByName("id"), "id-" + std::to_string(i));
        reflect->SetString(
          msg.get(),
          desc->FindFieldByName("name"),
          "Person-" + std::to_string(i));
        reflect->SetString(
          msg.get(),
          desc->FindFieldByName("birthday"),
          "1990-01-" + std::to_string(i));
        reflect->SetString(
          msg.get(),
          desc->FindFieldByName("ssn"),
          "123-45-" + std::to_string(6780 + i));

        auto serialized = pb_serialize(*msg);
        auto prefixed = prepend_schema_prefix(std::move(serialized));
        builder.add_raw_kv(std::nullopt, std::move(prefixed));
    }
    return std::move(builder).build();
}

/// Build a batch of N JSON records with schema-registry prefix.
model::record_batch make_json_batch(size_t count) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});
    for (size_t i = 0; i < count; ++i) {
        auto json_str = fmt::format(
          R"({{"id":"id-{}","name":"Person-{}","ssn":"123-45-{}"}})",
          i,
          i,
          6780 + i);
        auto json_buf = iobuf::from(json_str);
        auto prefixed = prepend_schema_prefix(std::move(json_buf));
        builder.add_raw_kv(std::nullopt, std::move(prefixed));
    }
    return std::move(builder).build();
}

/// Strip the 5-byte schema-registry prefix from an iobuf.
iobuf strip_prefix(iobuf value) {
    constexpr size_t prefix_len = 5;
    return value.share(prefix_len, value.size_bytes() - prefix_len);
}

} // namespace

// =========================================================================
// Test 1: Avro full round-trip
// =========================================================================

TEST_CORO(encryption_integration, avro_full_roundtrip) {
    constexpr size_t record_count = 5;

    auto schema = compile_avro_schema(R"({
        "name": "PersonalData",
        "type": "record",
        "namespace": "com.example",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "birthday", "type": "string"},
            {"name": "ssn", "type": "string"}
        ]
    })");

    encryption::mock_kms_provider kms;
    encryption::dek_manager dek_mgr{kms};
    encryption::schema_resolver resolver;
    encryption::ref_field_transformer transformer;

    register_avro_rules(resolver, schema);

    auto mock = std::make_unique<mock_partition_proxy>();
    auto* mock_ptr = mock.get();

    auto proxy = std::make_unique<kafka::encrypting_partition_proxy>(
      std::move(mock), resolver, dek_mgr, transformer);

    auto batch = make_avro_batch(schema, record_count);
    chunked_vector<model::record_batch> batches;
    batches.push_back(std::move(batch));

    auto result = co_await proxy->replicate(
      std::move(batches),
      raft::replicate_options{raft::consistency_level::quorum_ack});

    EXPECT_TRUE(result.has_value());
    EXPECT_TRUE(mock_ptr->last_batches.has_value());
    if (!result.has_value() || !mock_ptr->last_batches.has_value()) {
        co_return;
    }
    EXPECT_EQ(mock_ptr->last_batches->size(), 1);
    if (mock_ptr->last_batches->empty()) {
        co_return;
    }

    auto& received = mock_ptr->last_batches->front();

    // Verify rp.encryption header is present.
    auto extracted = co_await encryption::extract_encryption_metadata(received);
    EXPECT_TRUE(extracted.has_value())
      << "batch must contain rp.encryption header";
    if (!extracted.has_value()) {
        co_return;
    }
    EXPECT_TRUE(extracted->contains("test-kek"))
      << "DEK set must contain test-kek entry";

    // Retrieve the DEK with plaintext key from the dek_manager cache
    // (the header only stores encrypted_dek, not plaintext_dek).
    auto dek_state = co_await dek_mgr.get_or_create_dek(
      "test-topic",
      "test-kek",
      "mock",
      "default-key",
      encryption::dek_algorithm::aes256_gcm,
      std::nullopt);
    auto& dek = dek_state.plaintext_dek;

    // Walk each record: verify encrypted payload does not contain plaintext
    // for tagged fields, but does contain plaintext for non-tagged fields.
    size_t rec_idx = 0;
    received.for_each_record([&](model::record rec) {
        EXPECT_GE(rec.value_size(), 5);
        if (rec.value_size() < 5) {
            ++rec_idx;
            return;
        }
        auto payload = strip_prefix(rec.value().copy());
        auto payload_bytes = iobuf_to_bytes(payload);
        auto payload_sv = std::string_view(
          reinterpret_cast<const char*>(payload_bytes.data()),
          payload_bytes.size());

        auto expected_birthday = "1990-01-" + std::to_string(rec_idx);
        auto expected_ssn = "123-45-" + std::to_string(6780 + rec_idx);

        EXPECT_EQ(payload_sv.find(expected_birthday), std::string_view::npos)
          << "birthday should be encrypted for record " << rec_idx;
        EXPECT_EQ(payload_sv.find(expected_ssn), std::string_view::npos)
          << "ssn should be encrypted for record " << rec_idx;

        auto expected_id = "id-" + std::to_string(rec_idx);
        auto expected_name = "Person-" + std::to_string(rec_idx);
        EXPECT_NE(payload_sv.find(expected_id), std::string_view::npos)
          << "id should remain in plaintext for record " << rec_idx;
        EXPECT_NE(payload_sv.find(expected_name), std::string_view::npos)
          << "name should remain in plaintext for record " << rec_idx;

        ++rec_idx;
    });
    EXPECT_EQ(rec_idx, record_count);

    // Full async round-trip: parse first record, decrypt tagged fields.
    auto records = received.copy_records();
    EXPECT_FALSE(records.empty());
    if (records.empty()) {
        co_return;
    }
    auto first_payload = strip_prefix(records.front().value().copy());
    auto parsed = co_await serde::avro::parse(std::move(first_payload), schema);
    auto& result_rec = std::get<serde::avro::parsed::record>(*parsed);

    // id (field 0) unchanged.
    auto& id_buf = std::get<iobuf>(
      std::get<serde::avro::parsed::primitive>(*result_rec.fields[0]));
    EXPECT_EQ(id_buf.linearize_to_string(), "id-0");

    // name (field 1) unchanged.
    auto& name_buf = std::get<iobuf>(
      std::get<serde::avro::parsed::primitive>(*result_rec.fields[1]));
    EXPECT_EQ(name_buf.linearize_to_string(), "Person-0");

    // birthday (field 2) encrypted -- decrypt and verify.
    auto& bday_buf = std::get<iobuf>(
      std::get<serde::avro::parsed::primitive>(*result_rec.fields[2]));
    EXPECT_NE(bday_buf.linearize_to_string(), "1990-01-0");
    auto bday_decrypted = encryption::decrypt_field_value(dek, bday_buf.copy());
    EXPECT_EQ(bday_decrypted.linearize_to_string(), "1990-01-0");

    // ssn (field 3) encrypted -- decrypt and verify.
    auto& ssn_buf = std::get<iobuf>(
      std::get<serde::avro::parsed::primitive>(*result_rec.fields[3]));
    EXPECT_NE(ssn_buf.linearize_to_string(), "123-45-6780");
    auto ssn_decrypted = encryption::decrypt_field_value(dek, ssn_buf.copy());
    EXPECT_EQ(ssn_decrypted.linearize_to_string(), "123-45-6780");
}

// =========================================================================
// Test 2: Protobuf full round-trip
// =========================================================================

TEST_CORO(encryption_integration, proto_full_roundtrip) {
    constexpr size_t record_count = 5;

    proto_schema ps;
    auto* desc = ps.build();
    EXPECT_NE(desc, nullptr);
    if (desc == nullptr) {
        co_return;
    }

    encryption::mock_kms_provider kms;
    encryption::dek_manager dek_mgr{kms};
    encryption::schema_resolver resolver;
    encryption::ref_field_transformer transformer;

    register_proto_rules(resolver, desc);

    auto mock = std::make_unique<mock_partition_proxy>();
    auto* mock_ptr = mock.get();

    auto proxy = std::make_unique<kafka::encrypting_partition_proxy>(
      std::move(mock), resolver, dek_mgr, transformer);

    auto batch = make_proto_batch(desc, record_count);
    chunked_vector<model::record_batch> batches;
    batches.push_back(std::move(batch));

    auto result = co_await proxy->replicate(
      std::move(batches),
      raft::replicate_options{raft::consistency_level::quorum_ack});

    EXPECT_TRUE(result.has_value());
    EXPECT_TRUE(mock_ptr->last_batches.has_value());
    if (!result.has_value() || !mock_ptr->last_batches.has_value()) {
        co_return;
    }
    EXPECT_EQ(mock_ptr->last_batches->size(), 1);
    if (mock_ptr->last_batches->empty()) {
        co_return;
    }

    auto& received = mock_ptr->last_batches->front();

    // Verify rp.encryption header.
    auto extracted = co_await encryption::extract_encryption_metadata(received);
    EXPECT_TRUE(extracted.has_value());
    if (!extracted.has_value()) {
        co_return;
    }
    EXPECT_TRUE(extracted->contains("test-kek"));

    // Retrieve the DEK with plaintext key from the dek_manager cache.
    auto dek_state = co_await dek_mgr.get_or_create_dek(
      "test-topic",
      "test-kek",
      "mock",
      "default-key",
      encryption::dek_algorithm::aes256_gcm,
      std::nullopt);
    auto& dek = dek_state.plaintext_dek;

    // Verify each record has encrypted tagged fields.
    size_t rec_idx = 0;
    received.for_each_record([&](model::record rec) {
        EXPECT_GE(rec.value_size(), 5);
        if (rec.value_size() < 5) {
            ++rec_idx;
            return;
        }
        auto payload = strip_prefix(rec.value().copy());
        auto payload_bytes = iobuf_to_bytes(payload);
        auto payload_sv = std::string_view(
          reinterpret_cast<const char*>(payload_bytes.data()),
          payload_bytes.size());

        auto expected_birthday = "1990-01-" + std::to_string(rec_idx);
        auto expected_ssn = "123-45-" + std::to_string(6780 + rec_idx);

        EXPECT_EQ(payload_sv.find(expected_birthday), std::string_view::npos)
          << "birthday should be encrypted for record " << rec_idx;
        EXPECT_EQ(payload_sv.find(expected_ssn), std::string_view::npos)
          << "ssn should be encrypted for record " << rec_idx;

        auto expected_id = "id-" + std::to_string(rec_idx);
        auto expected_name = "Person-" + std::to_string(rec_idx);
        EXPECT_NE(payload_sv.find(expected_id), std::string_view::npos)
          << "id should remain in plaintext for record " << rec_idx;
        EXPECT_NE(payload_sv.find(expected_name), std::string_view::npos)
          << "name should remain in plaintext for record " << rec_idx;

        ++rec_idx;
    });
    EXPECT_EQ(rec_idx, record_count);

    // Full async round-trip: parse first record, decrypt tagged fields.
    auto records = received.copy_records();
    EXPECT_FALSE(records.empty());
    if (records.empty()) {
        co_return;
    }
    auto first_payload = strip_prefix(records.front().value().copy());
    auto parsed = co_await serde::pb::parse(std::move(first_payload), *desc);

    // id (field 1) unchanged.
    auto id_it = parsed->fields.find(1);
    EXPECT_NE(id_it, parsed->fields.end());
    if (id_it != parsed->fields.end()) {
        EXPECT_EQ(std::get<iobuf>(id_it->second).linearize_to_string(), "id-0");
    }

    // name (field 2) unchanged.
    auto name_it = parsed->fields.find(2);
    EXPECT_NE(name_it, parsed->fields.end());
    if (name_it != parsed->fields.end()) {
        EXPECT_EQ(
          std::get<iobuf>(name_it->second).linearize_to_string(), "Person-0");
    }

    // birthday (field 3) encrypted.
    auto bday_it = parsed->fields.find(3);
    EXPECT_NE(bday_it, parsed->fields.end());
    if (bday_it != parsed->fields.end()) {
        auto& bday_buf = std::get<iobuf>(bday_it->second);
        EXPECT_NE(bday_buf.linearize_to_string(), "1990-01-0");
        auto bday_decrypted = encryption::decrypt_field_value(
          dek, bday_buf.copy());
        EXPECT_EQ(bday_decrypted.linearize_to_string(), "1990-01-0");
    }

    // ssn (field 4) encrypted.
    auto ssn_it = parsed->fields.find(4);
    EXPECT_NE(ssn_it, parsed->fields.end());
    if (ssn_it != parsed->fields.end()) {
        auto& ssn_buf = std::get<iobuf>(ssn_it->second);
        EXPECT_NE(ssn_buf.linearize_to_string(), "123-45-6780");
        auto ssn_decrypted = encryption::decrypt_field_value(
          dek, ssn_buf.copy());
        EXPECT_EQ(ssn_decrypted.linearize_to_string(), "123-45-6780");
    }
}

// =========================================================================
// Test 3: JSON full round-trip
// =========================================================================

TEST_CORO(encryption_integration, json_full_roundtrip) {
    constexpr size_t record_count = 5;

    encryption::mock_kms_provider kms;
    encryption::dek_manager dek_mgr{kms};
    encryption::schema_resolver resolver;
    encryption::ref_field_transformer transformer;

    register_json_rules(resolver);

    auto mock = std::make_unique<mock_partition_proxy>();
    auto* mock_ptr = mock.get();

    auto proxy = std::make_unique<kafka::encrypting_partition_proxy>(
      std::move(mock), resolver, dek_mgr, transformer);

    auto batch = make_json_batch(record_count);
    chunked_vector<model::record_batch> batches;
    batches.push_back(std::move(batch));

    auto result = co_await proxy->replicate(
      std::move(batches),
      raft::replicate_options{raft::consistency_level::quorum_ack});

    EXPECT_TRUE(result.has_value());
    EXPECT_TRUE(mock_ptr->last_batches.has_value());
    if (!result.has_value() || !mock_ptr->last_batches.has_value()) {
        co_return;
    }
    EXPECT_EQ(mock_ptr->last_batches->size(), 1);
    if (mock_ptr->last_batches->empty()) {
        co_return;
    }

    auto& received = mock_ptr->last_batches->front();

    // Verify rp.encryption header.
    auto extracted = co_await encryption::extract_encryption_metadata(received);
    EXPECT_TRUE(extracted.has_value());
    if (!extracted.has_value()) {
        co_return;
    }
    EXPECT_TRUE(extracted->contains("test-kek"));

    // Retrieve the DEK with plaintext key from the dek_manager cache.
    auto dek_state = co_await dek_mgr.get_or_create_dek(
      "test-topic",
      "test-kek",
      "mock",
      "default-key",
      encryption::dek_algorithm::aes256_gcm,
      std::nullopt);
    auto& dek = dek_state.plaintext_dek;

    // Verify each record: parse JSON, check ssn is encrypted, decrypt it.
    auto records = received.copy_records();
    EXPECT_EQ(records.size(), record_count);
    if (records.size() != record_count) {
        co_return;
    }

    for (size_t i = 0; i < records.size(); ++i) {
        auto payload = strip_prefix(records[i].value().copy());
        auto p = serde::json::parser(std::move(payload));
        ss::sstring current_key;
        ss::sstring id_val;
        ss::sstring name_val;
        ss::sstring ssn_val;

        while (co_await p.next()) {
            if (p.token() == serde::json::token::key) {
                current_key = p.value_string().linearize_to_string();
            } else if (p.token() == serde::json::token::value_string) {
                if (current_key == "id") {
                    id_val = p.value_string().linearize_to_string();
                } else if (current_key == "name") {
                    name_val = p.value_string().linearize_to_string();
                } else if (current_key == "ssn") {
                    ssn_val = p.value_string().linearize_to_string();
                }
            }
        }

        auto expected_id = "id-" + std::to_string(i);
        auto expected_name = "Person-" + std::to_string(i);
        auto expected_ssn = "123-45-" + std::to_string(6780 + i);

        EXPECT_EQ(id_val, expected_id)
          << "id should be unchanged for record " << i;
        EXPECT_EQ(name_val, expected_name)
          << "name should be unchanged for record " << i;
        EXPECT_NE(ssn_val, expected_ssn)
          << "ssn should be encrypted for record " << i;

        // Decrypt the base64-encoded ciphertext.
        auto ct_bytes = base64_to_bytes(ssn_val);
        iobuf ct_buf;
        ct_buf.append(ct_bytes.data(), ct_bytes.size());
        auto decrypted = encryption::decrypt_field_value(
          dek, std::move(ct_buf));
        EXPECT_EQ(decrypted.linearize_to_string(), expected_ssn)
          << "decrypted ssn should match original for record " << i;
    }
}

// =========================================================================
// Test 4: passthrough with no rules
// =========================================================================

TEST_CORO(encryption_integration, passthrough_no_rules) {
    constexpr size_t record_count = 3;

    auto schema = compile_avro_schema(R"({
        "name": "PersonalData",
        "type": "record",
        "namespace": "com.example",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "birthday", "type": "string"},
            {"name": "ssn", "type": "string"}
        ]
    })");

    encryption::mock_kms_provider kms;
    encryption::dek_manager dek_mgr{kms};
    encryption::schema_resolver resolver;
    encryption::ref_field_transformer transformer;

    // Do NOT register any rules.

    auto mock = std::make_unique<mock_partition_proxy>();
    auto* mock_ptr = mock.get();

    auto proxy = std::make_unique<kafka::encrypting_partition_proxy>(
      std::move(mock), resolver, dek_mgr, transformer);

    auto batch = make_avro_batch(schema, record_count);

    // Save a copy of the original record payloads for comparison.
    std::vector<bytes> original_payloads;
    batch.for_each_record([&](model::record rec) {
        original_payloads.push_back(iobuf_to_bytes(rec.value().copy()));
    });

    chunked_vector<model::record_batch> batches;
    batches.push_back(std::move(batch));

    auto result = co_await proxy->replicate(
      std::move(batches),
      raft::replicate_options{raft::consistency_level::quorum_ack});

    EXPECT_TRUE(result.has_value());
    EXPECT_TRUE(mock_ptr->last_batches.has_value());
    if (!result.has_value() || !mock_ptr->last_batches.has_value()) {
        co_return;
    }
    EXPECT_EQ(mock_ptr->last_batches->size(), 1);
    if (mock_ptr->last_batches->empty()) {
        co_return;
    }

    auto& received = mock_ptr->last_batches->front();

    // There should be no rp.encryption header.
    auto extracted = co_await encryption::extract_encryption_metadata(received);
    EXPECT_FALSE(extracted.has_value())
      << "no encryption header when no rules are registered";

    // All record values should be exactly as the originals.
    size_t rec_idx = 0;
    received.for_each_record([&](model::record rec) {
        auto actual = iobuf_to_bytes(rec.value().copy());
        if (rec_idx < original_payloads.size()) {
            EXPECT_EQ(actual, original_payloads[rec_idx])
              << "record " << rec_idx << " should pass through unchanged";
        }
        ++rec_idx;
    });
    EXPECT_EQ(rec_idx, record_count);
}

// =========================================================================
// Test 5: multiple batches produce consistent DEK metadata
// =========================================================================

TEST_CORO(encryption_integration, multiple_batches_same_dek_version) {
    auto schema = compile_avro_schema(R"({
        "name": "PersonalData",
        "type": "record",
        "namespace": "com.example",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "birthday", "type": "string"},
            {"name": "ssn", "type": "string"}
        ]
    })");

    encryption::mock_kms_provider kms;
    encryption::dek_manager dek_mgr{kms};
    encryption::schema_resolver resolver;
    encryption::ref_field_transformer transformer;

    register_avro_rules(resolver, schema);

    auto mock = std::make_unique<mock_partition_proxy>();
    auto* mock_ptr = mock.get();

    auto proxy = std::make_unique<kafka::encrypting_partition_proxy>(
      std::move(mock), resolver, dek_mgr, transformer);

    // Two separate batches in the same replicate call.
    auto batch1 = make_avro_batch(schema, 3);
    auto batch2 = make_avro_batch(schema, 2);
    chunked_vector<model::record_batch> batches;
    batches.push_back(std::move(batch1));
    batches.push_back(std::move(batch2));

    auto result = co_await proxy->replicate(
      std::move(batches),
      raft::replicate_options{raft::consistency_level::quorum_ack});

    EXPECT_TRUE(result.has_value());
    EXPECT_TRUE(mock_ptr->last_batches.has_value());
    if (!result.has_value() || !mock_ptr->last_batches.has_value()) {
        co_return;
    }
    EXPECT_EQ(mock_ptr->last_batches->size(), 2);
    if (mock_ptr->last_batches->size() != 2) {
        co_return;
    }

    // Both batches should have encryption metadata.
    auto meta1 = co_await encryption::extract_encryption_metadata(
      mock_ptr->last_batches->at(0));
    auto meta2 = co_await encryption::extract_encryption_metadata(
      mock_ptr->last_batches->at(1));

    EXPECT_TRUE(meta1.has_value());
    EXPECT_TRUE(meta2.has_value());
    if (!meta1.has_value() || !meta2.has_value()) {
        co_return;
    }

    EXPECT_TRUE(meta1->contains("test-kek"));
    EXPECT_TRUE(meta2->contains("test-kek"));

    // The DEK version should be the same (no rotation between batches).
    auto it1 = meta1->find("test-kek");
    auto it2 = meta2->find("test-kek");
    EXPECT_NE(it1, meta1->end());
    EXPECT_NE(it2, meta2->end());
    if (it1 != meta1->end() && it2 != meta2->end()) {
        EXPECT_EQ(it1->second.version, it2->second.version);
    }

    // Both batches should have encrypted records.
    for (size_t batch_idx = 0; batch_idx < 2; ++batch_idx) {
        auto& b = mock_ptr->last_batches->at(batch_idx);
        b.for_each_record([&](model::record rec) {
            EXPECT_GE(rec.value_size(), 5);
            if (rec.value_size() < 5) {
                return;
            }
            auto payload = strip_prefix(rec.value().copy());
            auto payload_bytes = iobuf_to_bytes(payload);
            auto payload_sv = std::string_view(
              reinterpret_cast<const char*>(payload_bytes.data()),
              payload_bytes.size());
            EXPECT_EQ(payload_sv.find("1990-01-"), std::string_view::npos)
              << "birthday should be encrypted in batch " << batch_idx;
        });
    }
}

// =========================================================================
// Test 6: produce->consume all records have full rp.encryption header
// =========================================================================

TEST_CORO(
  encryption_integration, produce_consume_all_records_have_full_header) {
    constexpr size_t record_count = 5;

    auto schema = compile_avro_schema(R"({
        "name": "PersonalData",
        "type": "record",
        "namespace": "com.example",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "birthday", "type": "string"},
            {"name": "ssn", "type": "string"}
        ]
    })");

    encryption::mock_kms_provider kms;
    encryption::dek_manager dek_mgr{kms};
    encryption::schema_resolver resolver;
    encryption::ref_field_transformer transformer;

    register_avro_rules(resolver, schema);

    auto mock = std::make_unique<mock_partition_proxy>();
    auto* mock_ptr = mock.get();

    auto proxy = std::make_unique<kafka::encrypting_partition_proxy>(
      std::move(mock), resolver, dek_mgr, transformer);

    auto batch = make_avro_batch(schema, record_count);
    chunked_vector<model::record_batch> batches;
    batches.push_back(std::move(batch));

    auto result = co_await proxy->replicate(
      std::move(batches),
      raft::replicate_options{raft::consistency_level::quorum_ack});

    EXPECT_TRUE(result.has_value());
    EXPECT_TRUE(mock_ptr->last_batches.has_value());
    if (!result.has_value() || !mock_ptr->last_batches.has_value()) {
        co_return;
    }
    EXPECT_EQ(mock_ptr->last_batches->size(), 1);
    if (mock_ptr->last_batches->empty()) {
        co_return;
    }

    // --- Verify the intermediate state after the write path ---
    // Record 0 should have a full (non-empty) rp.encryption header.
    // Records 1-4 should have sentinel (empty) rp.encryption headers.
    auto& captured = mock_ptr->last_batches->front();
    {
        size_t rec_idx = 0;
        captured.for_each_record([&](model::record rec) {
            for (const auto& h : rec.headers()) {
                if (
                  h.key().linearize_to_string()
                  == ss::sstring{encryption::encryption_header_key}) {
                    if (rec_idx == 0) {
                        EXPECT_GT(h.value_size(), 0)
                          << "record 0 must have full DEK header";
                    } else {
                        EXPECT_EQ(h.value_size(), 0)
                          << "record " << rec_idx
                          << " must have sentinel (empty) header";
                    }
                }
            }
            ++rec_idx;
        });
        EXPECT_EQ(rec_idx, record_count);
    }

    // --- Simulate the read path: refill sentinels ---
    auto refill = co_await encryption::refill_dek_sentinels(
      captured.copy(), std::nullopt);

    // All 5 records should now have non-empty rp.encryption headers.
    std::vector<iobuf> header_values;
    {
        size_t rec_idx = 0;
        refill.batch.for_each_record([&](model::record rec) {
            bool found = false;
            for (const auto& h : rec.headers()) {
                if (
                  h.key().linearize_to_string()
                  == ss::sstring{encryption::encryption_header_key}) {
                    found = true;
                    EXPECT_GT(h.value_size(), 0)
                      << "record " << rec_idx
                      << " must have non-empty header after refill";
                    header_values.push_back(h.value().copy());
                }
            }
            EXPECT_TRUE(found)
              << "record " << rec_idx << " must have rp.encryption header";
            ++rec_idx;
        });
        EXPECT_EQ(rec_idx, record_count);
    }

    // All header values must be byte-identical (same serialized DEK metadata).
    EXPECT_EQ(header_values.size(), record_count);
    for (size_t i = 1; i < header_values.size(); ++i) {
        EXPECT_EQ(header_values[0], header_values[i])
          << "header value for record " << i
          << " must be identical to record 0";
    }

    // The refill result should carry the DEK metadata forward.
    EXPECT_TRUE(refill.last_dek_metadata.has_value());
    if (refill.last_dek_metadata.has_value()) {
        EXPECT_EQ(*refill.last_dek_metadata, header_values[0]);
    }
}
