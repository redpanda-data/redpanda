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

#include "kafka/data/encrypting_partition_proxy.h"

#include "base/vlog.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "encryption/dek_manager.h"
#include "encryption/encryption_metadata_ser.h"
#include "encryption/field_transformer.h"
#include "encryption/schema_resolver.h"
#include "kafka/data/dek_refilling_reader.h"
#include "kafka/data/logger.h"
#include "model/record.h"
#include "storage/record_batch_builder.h"

#include <seastar/coroutine/all.hh>

namespace kafka {

encrypting_partition_proxy::encrypting_partition_proxy(
  std::unique_ptr<partition_proxy::impl> inner,
  encryption::schema_resolver& resolver,
  encryption::dek_manager& dek_mgr,
  encryption::field_transformer& transformer)
  : forwarding_partition_proxy_impl(std::move(inner))
  , _resolver(resolver)
  , _dek_mgr(dek_mgr)
  , _transformer(transformer) {}

namespace {

/// Schema-registry wire format prefix length: magic byte (1) + schema ID (4).
constexpr size_t schema_prefix_len = 5;
constexpr uint8_t schema_registry_magic_byte = 0;

/// Extract the 5-byte schema prefix from a record value iobuf. Returns the
/// prefix and strips it from the value (value becomes the payload without
/// prefix). Returns nullopt if the value is too short or the magic byte is
/// wrong.
std::optional<iobuf> strip_schema_prefix(iobuf& value) {
    if (value.size_bytes() < schema_prefix_len) {
        return std::nullopt;
    }
    iobuf_const_parser parser(value);
    auto magic = parser.consume_type<uint8_t>();
    if (magic != schema_registry_magic_byte) {
        return std::nullopt;
    }
    // Rewind: share the first 5 bytes as prefix, the rest as payload.
    iobuf prefix = value.share(0, schema_prefix_len);
    iobuf payload = value.share(
      schema_prefix_len, value.size_bytes() - schema_prefix_len);
    value = std::move(payload);
    return prefix;
}

} // namespace

ss::future<model::record_batch>
encrypting_partition_proxy::encrypt_batch(model::record_batch batch) {
    const auto& topic = ntp().tp.topic;

    auto schema_opt = co_await _resolver.resolve(topic);
    if (!schema_opt.has_value()) {
        vlog(
          kdlog.trace,
          "No encryption schema for topic {}, passing through",
          topic);
        co_return batch;
    }
    vlog(
      kdlog.trace,
      "Encrypting batch for topic {} with {} tagged fields",
      topic,
      schema_opt->tagged_fields.size());
    auto& schema = *schema_opt;

    // Collect DEKs for all distinct kek_names in the schema.
    encryption::dek_set deks;
    for (const auto& field : schema.tagged_fields) {
        if (deks.contains(field.kek_name)) {
            continue;
        }
        auto dek = co_await _dek_mgr.get_or_create_dek(
          ss::sstring{topic()},
          ss::sstring{field.kek_name},
          "mock",
          "default-key",
          encryption::dek_algorithm::aes256_gcm,
          std::nullopt);
        deks.emplace(ss::sstring{field.kek_name}, std::move(dek));
    }

    // Materialize records so we can process each one asynchronously.
    auto records = batch.copy_records();

    // Rebuild the batch with encrypted record values.
    storage::record_batch_builder builder(
      batch.header().type, batch.base_offset());
    builder.set_compression(batch.header().attrs.compression());

    for (auto& rec : records) {
        std::optional<iobuf> key;
        if (rec.key_size() >= 0) {
            key = rec.key().copy();
        }

        std::optional<iobuf> value;
        if (rec.value_size() >= 0) {
            auto val = rec.value().copy();
            auto prefix = strip_schema_prefix(val);
            if (prefix.has_value()) {
                // Transform the payload (value now has prefix stripped).
                auto encrypted = co_await _transformer.transform(
                  std::move(val), schema, deks);
                // Re-prepend the schema prefix.
                iobuf result;
                result.append(std::move(*prefix));
                result.append(std::move(encrypted));
                value = std::move(result);
            } else {
                // No schema prefix: pass through unchanged.
                value = rec.value().copy();
            }
        }

        chunked_vector<model::record_header> hdrs;
        for (const auto& h : rec.headers()) {
            hdrs.push_back(h.copy());
        }

        builder.add_raw_kw(std::move(key), std::move(value), std::move(hdrs));
    }

    auto encrypted_batch = std::move(builder).build();

    // Inject the rp.encryption header with DEK metadata.
    co_return co_await encryption::inject_encryption_headers(
      std::move(encrypted_batch), deks);
}

ss::future<result<model::offset>> encrypting_partition_proxy::replicate(
  chunked_vector<model::record_batch> batches, raft::replicate_options opts) {
    chunked_vector<model::record_batch> encrypted;
    encrypted.reserve(batches.size());
    for (auto& batch : batches) {
        encrypted.push_back(co_await encrypt_batch(std::move(batch)));
    }
    co_return co_await _inner->replicate(std::move(encrypted), opts);
}

raft::replicate_stages encrypting_partition_proxy::replicate(
  model::batch_identity bid,
  model::record_batch batch,
  raft::replicate_options opts) {
    ss::promise<result<raft::replicate_result>> p;
    auto finished = p.get_future();
    auto enqueued = encrypt_batch(std::move(batch))
                      .then([this, bid, opts, p = std::move(p)](
                              model::record_batch encrypted) mutable {
                          auto inner = _inner->replicate(
                            bid, std::move(encrypted), opts);
                          inner.replicate_finished.forward_to(std::move(p));
                          return std::move(inner.request_enqueued);
                      });
    return raft::replicate_stages(std::move(enqueued), std::move(finished));
}

ss::future<storage::translating_reader>
encrypting_partition_proxy::make_reader(kafka::log_reader_config cfg) {
    auto reader = co_await _inner->make_reader(cfg);
    reader.reader = make_dek_refilling_reader(std::move(reader.reader));
    co_return reader;
}

std::unique_ptr<exact_offset_replicator>
encrypting_partition_proxy::make_exact_offset_replicator() && {
    return std::move(*_inner).make_exact_offset_replicator();
}

} // namespace kafka
