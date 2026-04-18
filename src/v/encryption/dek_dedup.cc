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

#include "encryption/dek_dedup.h"

#include "encryption/encryption_metadata_ser.h"
#include "encryption/types.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>

namespace encryption {

ss::future<model::record_batch> strip_duplicate_dek_headers(
  model::record_batch batch, seen_dek_set& seen_deks) {
    // Compressed batches cannot be iterated; pass through unmodified
    if (batch.compressed()) {
        co_return batch;
    }

    // First pass: check if any modification is needed.
    // Extract encryption metadata from the batch to see if there are DEKs.
    auto maybe_deks = co_await extract_encryption_metadata(batch);
    if (!maybe_deks) {
        // No encryption header -- pass through unmodified
        co_return batch;
    }

    // Determine which DEKs are new vs duplicates
    dek_set new_deks;
    bool has_duplicates = false;
    for (const auto& [name, state] : *maybe_deks) {
        dek_id id{.kek_name = state.kek_name, .dek_version = state.version};
        if (seen_deks.contains(id)) {
            has_duplicates = true;
        } else {
            new_deks.emplace(name, state);
        }
    }

    if (!has_duplicates) {
        // All DEKs are new -- add them to the seen set and return unmodified
        for (const auto& [name, state] : *maybe_deks) {
            seen_deks.emplace(
              dek_id{.kek_name = state.kek_name, .dek_version = state.version});
        }
        co_return batch;
    }

    // Add new DEKs to the seen set
    for (const auto& [name, state] : new_deks) {
        seen_deks.emplace(
          dek_id{.kek_name = state.kek_name, .dek_version = state.version});
    }

    // Rebuild the batch with filtered (or removed) encryption headers.
    // Serialize the filtered metadata if any new DEKs remain.
    std::optional<iobuf> filtered_header;
    if (!new_deks.empty()) {
        filtered_header = co_await serialize_encryption_metadata(new_deks);
    }

    storage::record_batch_builder builder(
      batch.header().type, batch.base_offset());
    builder.set_compression(batch.header().attrs.compression());

    bool first_record = true;
    batch.for_each_record([&](model::record rec) {
        std::optional<iobuf> key;
        if (rec.key_size() >= 0) {
            key = rec.key().copy();
        }
        std::optional<iobuf> value;
        if (rec.value_size() >= 0) {
            value = rec.value().copy();
        }

        chunked_vector<model::record_header> hdrs;
        for (const auto& h : rec.headers()) {
            auto key_str = h.key().linearize_to_string();
            if (key_str == ss::sstring{encryption_header_key} && first_record) {
                // Replace or remove the encryption header on the first record
                iobuf hdr_key;
                hdr_key.append(
                  encryption_header_key.data(), encryption_header_key.size());
                if (filtered_header) {
                    hdrs.emplace_back(
                      std::move(hdr_key), filtered_header->copy());
                } else {
                    // All DEKs were duplicates -- emit a sentinel (empty
                    // value) so the read path knows to use a previously
                    // seen DEK.
                    hdrs.emplace_back(std::move(hdr_key), iobuf{});
                }
            } else {
                hdrs.push_back(h.copy());
            }
        }
        first_record = false;

        builder.add_raw_kw(std::move(key), std::move(value), std::move(hdrs));
    });

    co_return std::move(builder).build();
}

} // namespace encryption
