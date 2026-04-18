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

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "cluster/partition_probe.h"
#include "cluster/types.h"
#include "encryption/dek_manager.h"
#include "encryption/encryption_metadata_ser.h"
#include "encryption/field_transformer.h"
#include "encryption/mock_kms_provider.h"
#include "encryption/schema_resolver.h"
#include "encryption/types.h"
#include "kafka/data/encrypting_partition_proxy.h"
#include "kafka/data/partition_proxy.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "raft/replicate.h"
#include "storage/record_batch_builder.h"
#include "test_utils/test.h"

#include <seastar/core/future.hh>

#include <gtest/gtest.h>

#include <memory>
#include <optional>

namespace {

/// No-op partition_probe::impl for testing.
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

/// Mock partition_proxy::impl that records replicate calls and returns
/// configurable results.
class mock_partition_proxy final : public kafka::partition_proxy::impl {
public:
    model::ntp test_ntp{
      model::ns{"kafka"}, model::topic{"test-topic"}, model::partition_id{0}};
    cluster::partition_probe probe_{
      std::make_unique<noop_partition_probe_impl>()};

    // Captured from replicate calls.
    std::optional<model::record_batch> last_batch;
    std::optional<chunked_vector<model::record_batch>> last_batches;

    // Configurable return values.
    result<model::offset> replicate_result_{model::offset{42}};
    bool replicate_stages_called{false};

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
        // Not needed for replicate tests.
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
        replicate_stages_called = true;
        last_batch.emplace(std::move(batch));
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

/// Minimal field_transformer that just returns the value unchanged, to
/// verify the proxy wiring without needing real encryption.
class passthrough_transformer final : public encryption::field_transformer {
public:
    bool transform_called{false};

    ss::future<iobuf> transform(
      iobuf value,
      const encryption::encryption_schema&,
      const encryption::dek_set&) override {
        transform_called = true;
        co_return value;
    }
};

/// Field transformer that XORs every byte of the value with 0xFF.
/// This makes it easy to verify that the value was actually transformed.
class xor_transformer final : public encryption::field_transformer {
public:
    ss::future<iobuf> transform(
      iobuf value,
      const encryption::encryption_schema&,
      const encryption::dek_set&) override {
        auto lin = iobuf_to_bytes(value);
        bytes result(bytes::initialized_later{}, lin.size());
        for (size_t i = 0; i < lin.size(); ++i) {
            result[i] = lin[i] ^ 0xFF;
        }
        iobuf out;
        out.append(result.data(), result.size());
        co_return out;
    }
};

/// Prepend a 5-byte schema-registry prefix (magic 0x00 + big-endian schema
/// ID) to an iobuf.
iobuf prepend_schema_prefix(iobuf value, int32_t schema_id = 1) {
    iobuf result;
    // Magic byte
    uint8_t magic = 0x00;
    result.append(&magic, 1);
    // Big-endian schema ID
    uint8_t id_bytes[4];
    id_bytes[0] = static_cast<uint8_t>((schema_id >> 24) & 0xFF);
    id_bytes[1] = static_cast<uint8_t>((schema_id >> 16) & 0xFF);
    id_bytes[2] = static_cast<uint8_t>((schema_id >> 8) & 0xFF);
    id_bytes[3] = static_cast<uint8_t>(schema_id & 0xFF);
    result.append(id_bytes, 4);
    result.append(std::move(value));
    return result;
}

/// Build a batch with a single record whose value has the schema prefix.
model::record_batch
make_prefixed_batch(const ss::sstring& payload, int32_t schema_id = 1) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});
    iobuf value;
    value.append(payload.data(), payload.size());
    auto prefixed = prepend_schema_prefix(std::move(value), schema_id);
    builder.add_raw_kv(std::nullopt, std::move(prefixed));
    return std::move(builder).build();
}

/// Register encryption rules for "test-topic" with a single field.
void register_test_rules(encryption::schema_resolver& resolver) {
    encryption::topic_encryption_config config;
    config.format = encryption::schema_format::avro;
    config.handle = std::monostate{};
    config.rules.push_back(
      encryption::encryption_rule{.tag = "PII", .kek_name = "test-kek"});
    config.field_tags.push_back(
      encryption::field_tag_mapping{.path = {"field1"}, .tag = "PII"});
    resolver.register_rules(model::topic{"test-topic"}, std::move(config));
}

} // namespace

TEST_CORO(encrypting_partition_proxy_test, replicate_encrypts_tagged_fields) {
    encryption::mock_kms_provider kms;
    encryption::dek_manager dek_mgr{kms};
    encryption::schema_resolver resolver;
    xor_transformer transformer;

    register_test_rules(resolver);

    auto mock = std::make_unique<mock_partition_proxy>();
    auto* mock_ptr = mock.get();

    auto proxy = std::make_unique<kafka::encrypting_partition_proxy>(
      std::move(mock), resolver, dek_mgr, transformer);

    auto batch = make_prefixed_batch("hello-world");
    chunked_vector<model::record_batch> batches;
    batches.push_back(std::move(batch));

    auto result = co_await proxy->replicate(
      std::move(batches),
      raft::replicate_options{raft::consistency_level::quorum_ack});

    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), model::offset{42});

    EXPECT_TRUE(mock_ptr->last_batches.has_value());
    EXPECT_EQ(mock_ptr->last_batches->size(), 1);

    if (
      !mock_ptr->last_batches.has_value() || mock_ptr->last_batches->empty()) {
        co_return;
    }

    auto& received = mock_ptr->last_batches->front();
    received.for_each_record([](model::record rec) {
        EXPECT_GE(rec.value_size(), 5);
        if (rec.value_size() < 5) {
            return;
        }
        auto val = rec.value().copy();
        iobuf_const_parser parser(val);
        auto magic = parser.consume_type<uint8_t>();
        EXPECT_EQ(magic, 0x00);

        // The payload after the 5-byte prefix should be XOR'd (not plaintext).
        auto payload_buf = val.share(5, val.size_bytes() - 5);
        auto payload_str = iobuf_to_bytes(payload_buf);
        // Original was "hello-world", XOR'd should differ.
        ss::sstring original = "hello-world";
        bytes original_bytes(
          reinterpret_cast<const uint8_t*>(original.data()), original.size());
        EXPECT_NE(payload_str, original_bytes)
          << "payload should be transformed (encrypted)";
    });
}

TEST_CORO(encrypting_partition_proxy_test, replicate_passthrough_no_rules) {
    encryption::mock_kms_provider kms;
    encryption::dek_manager dek_mgr{kms};
    encryption::schema_resolver resolver;
    passthrough_transformer transformer;

    // Do NOT register any rules -- the proxy should pass through.

    auto mock = std::make_unique<mock_partition_proxy>();
    auto* mock_ptr = mock.get();

    auto proxy = std::make_unique<kafka::encrypting_partition_proxy>(
      std::move(mock), resolver, dek_mgr, transformer);

    auto batch = make_prefixed_batch("plaintext-value");
    chunked_vector<model::record_batch> batches;
    batches.push_back(std::move(batch));

    auto result = co_await proxy->replicate(
      std::move(batches),
      raft::replicate_options{raft::consistency_level::quorum_ack});

    EXPECT_TRUE(result.has_value());

    EXPECT_TRUE(mock_ptr->last_batches.has_value());
    EXPECT_EQ(mock_ptr->last_batches->size(), 1);

    if (
      !mock_ptr->last_batches.has_value() || mock_ptr->last_batches->empty()) {
        co_return;
    }

    auto& received = mock_ptr->last_batches->front();
    received.for_each_record([](model::record rec) {
        // Value should be unchanged (passthrough).
        auto val = rec.value().copy();
        EXPECT_GE(val.size_bytes(), 5);
        if (val.size_bytes() < 5) {
            return;
        }
        auto payload_buf = val.share(5, val.size_bytes() - 5);
        auto payload_str = iobuf_to_bytes(payload_buf);
        ss::sstring expected = "plaintext-value";
        bytes expected_bytes(
          reinterpret_cast<const uint8_t*>(expected.data()), expected.size());
        EXPECT_EQ(payload_str, expected_bytes)
          << "value should pass through unchanged when no rules exist";
    });

    // transformer should not be called.
    EXPECT_FALSE(transformer.transform_called);
}

TEST_CORO(
  encrypting_partition_proxy_test, replicate_injects_encryption_header) {
    encryption::mock_kms_provider kms;
    encryption::dek_manager dek_mgr{kms};
    encryption::schema_resolver resolver;
    passthrough_transformer transformer;

    register_test_rules(resolver);

    auto mock = std::make_unique<mock_partition_proxy>();
    auto* mock_ptr = mock.get();

    auto proxy = std::make_unique<kafka::encrypting_partition_proxy>(
      std::move(mock), resolver, dek_mgr, transformer);

    auto batch = make_prefixed_batch("test-data");
    chunked_vector<model::record_batch> batches;
    batches.push_back(std::move(batch));

    auto result = co_await proxy->replicate(
      std::move(batches),
      raft::replicate_options{raft::consistency_level::quorum_ack});

    EXPECT_TRUE(result.has_value());
    EXPECT_TRUE(mock_ptr->last_batches.has_value());
    EXPECT_EQ(mock_ptr->last_batches->size(), 1);

    if (
      !mock_ptr->last_batches.has_value() || mock_ptr->last_batches->empty()) {
        co_return;
    }

    // The received batch should have the rp.encryption header on the first
    // record.
    auto& received = mock_ptr->last_batches->front();
    auto extracted = co_await encryption::extract_encryption_metadata(received);
    EXPECT_TRUE(extracted.has_value())
      << "batch should contain rp.encryption header";
    if (extracted.has_value()) {
        EXPECT_TRUE(extracted->contains("test-kek"))
          << "DEK set should contain the test-kek entry";
    }
}

TEST_CORO(encrypting_partition_proxy_test, replicate_stages_futures_chain) {
    encryption::mock_kms_provider kms;
    encryption::dek_manager dek_mgr{kms};
    encryption::schema_resolver resolver;
    passthrough_transformer transformer;

    register_test_rules(resolver);

    auto mock = std::make_unique<mock_partition_proxy>();
    auto* mock_ptr = mock.get();

    auto proxy = std::make_unique<kafka::encrypting_partition_proxy>(
      std::move(mock), resolver, dek_mgr, transformer);

    auto batch = make_prefixed_batch("stages-test");
    model::batch_identity bid{};

    auto stages = proxy->replicate(
      bid,
      std::move(batch),
      raft::replicate_options{raft::consistency_level::quorum_ack});

    co_await std::move(stages.request_enqueued);
    auto replicate_result = co_await std::move(stages.replicate_finished);

    EXPECT_TRUE(replicate_result.has_value());
    if (replicate_result.has_value()) {
        EXPECT_EQ(replicate_result.value().last_offset, model::offset{42});
    }
    EXPECT_TRUE(mock_ptr->replicate_stages_called);
    EXPECT_TRUE(mock_ptr->last_batch.has_value());
}

TEST_CORO(encrypting_partition_proxy_test, replicate_error_propagation) {
    encryption::mock_kms_provider kms;
    encryption::dek_manager dek_mgr{kms};
    encryption::schema_resolver resolver;
    passthrough_transformer transformer;

    // No rules: passthrough, so the inner proxy's error propagates directly.

    auto mock = std::make_unique<mock_partition_proxy>();
    mock->replicate_result_ = raft::errc::not_leader;

    auto proxy = std::make_unique<kafka::encrypting_partition_proxy>(
      std::move(mock), resolver, dek_mgr, transformer);

    auto batch = make_prefixed_batch("error-test");
    chunked_vector<model::record_batch> batches;
    batches.push_back(std::move(batch));

    auto result = co_await proxy->replicate(
      std::move(batches),
      raft::replicate_options{raft::consistency_level::quorum_ack});

    EXPECT_TRUE(result.has_error());
}
