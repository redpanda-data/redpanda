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

#include "encryption/encryption_metadata_ser.h"

#include "bytes/iobuf.h"
#include "container/chunked_hash_map.h"
#include "encryption/types.h"
#include "model/record.h"
#include "proto/redpanda/core/encryption/v1/encryption.proto.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/sstring.hh>
#include <seastar/coroutine/all.hh>

namespace encryption {

namespace {

ss::sstring algorithm_to_string(dek_algorithm algo) {
    switch (algo) {
    case dek_algorithm::aes128_gcm:
        return "AES128_GCM";
    case dek_algorithm::aes256_gcm:
        return "AES256_GCM";
    case dek_algorithm::aes256_siv:
        return "AES256_SIV";
    }
    __builtin_unreachable();
}

dek_algorithm algorithm_from_string(std::string_view s) {
    if (s == "AES128_GCM") {
        return dek_algorithm::aes128_gcm;
    }
    if (s == "AES256_GCM") {
        return dek_algorithm::aes256_gcm;
    }
    if (s == "AES256_SIV") {
        return dek_algorithm::aes256_siv;
    }
    throw std::invalid_argument(fmt::format("unknown dek_algorithm: {}", s));
}

} // namespace

ss::future<iobuf> serialize_encryption_metadata(const dek_set& deks) {
    proto::encryption::encryption_metadata msg;
    for (const auto& [name, state] : deks) {
        auto& entry = msg.get_deks().emplace_back();
        entry.set_kek_name(ss::sstring{state.kek_name});
        entry.set_kms_type(ss::sstring{state.kms_type});
        entry.set_kms_key_id(ss::sstring{state.kms_key_id});
        iobuf encrypted_copy;
        encrypted_copy.append(
          state.encrypted_dek.data(), state.encrypted_dek.size());
        entry.set_encrypted_dek(std::move(encrypted_copy));
        entry.set_algorithm(algorithm_to_string(state.algorithm));
        entry.set_dek_version(state.version);
    }
    co_return co_await msg.to_proto();
}

ss::future<dek_set> deserialize_encryption_metadata(iobuf buf) {
    auto msg = co_await proto::encryption::encryption_metadata::from_proto(
      std::move(buf));
    dek_set result;
    for (const auto& entry : msg.get_deks()) {
        dek_state state;
        state.kek_name = entry.get_kek_name();
        state.kms_type = entry.get_kms_type();
        state.kms_key_id = entry.get_kms_key_id();
        state.encrypted_dek = bytes(
          bytes::initialized_later{}, entry.get_encrypted_dek().size_bytes());
        {
            iobuf::iterator_consumer it(
              entry.get_encrypted_dek().cbegin(),
              entry.get_encrypted_dek().cend());
            it.consume_to(
              entry.get_encrypted_dek().size_bytes(),
              state.encrypted_dek.data());
        }
        state.algorithm = algorithm_from_string(entry.get_algorithm());
        state.version = entry.get_dek_version();
        result.emplace(ss::sstring{state.kek_name}, std::move(state));
    }
    co_return result;
}

ss::future<model::record_batch>
inject_encryption_headers(model::record_batch batch, const dek_set& deks) {
    auto serialized = co_await serialize_encryption_metadata(deks);

    storage::record_batch_builder builder(
      batch.header().type, batch.base_offset());
    builder.set_compression(batch.header().attrs.compression());

    bool first = true;
    batch.for_each_record([&](model::record rec) {
        // Copy key and value
        std::optional<iobuf> key;
        if (rec.key_size() >= 0) {
            key = rec.key().copy();
        }
        std::optional<iobuf> value;
        if (rec.value_size() >= 0) {
            value = rec.value().copy();
        }

        // Copy existing headers
        chunked_vector<model::record_header> hdrs;
        for (const auto& h : rec.headers()) {
            hdrs.push_back(h.copy());
        }

        iobuf header_key;
        header_key.append(
          encryption_header_key.data(), encryption_header_key.size());
        if (first) {
            // Add full encryption header to first record
            hdrs.emplace_back(std::move(header_key), serialized.copy());
            first = false;
        } else {
            // Add sentinel (empty value) to subsequent records
            hdrs.emplace_back(std::move(header_key), iobuf{});
        }

        builder.add_raw_kw(std::move(key), std::move(value), std::move(hdrs));
    });

    co_return std::move(builder).build();
}

ss::future<std::optional<dek_set>>
extract_encryption_metadata(const model::record_batch& batch) {
    std::optional<iobuf> header_value;
    batch.for_each_record([&](model::record rec) {
        if (header_value) {
            return ss::stop_iteration::yes;
        }
        for (const auto& h : rec.headers()) {
            if (
              h.key_size() == static_cast<int32_t>(encryption_header_key.size())
              && h.key().linearize_to_string()
                   == ss::sstring{encryption_header_key}) {
                header_value = h.value().copy();
                return ss::stop_iteration::yes;
            }
        }
        return ss::stop_iteration::no;
    });

    if (!header_value) {
        co_return std::nullopt;
    }

    co_return co_await deserialize_encryption_metadata(
      std::move(*header_value));
}

} // namespace encryption
