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

#include "encryption/dek_refill.h"

#include "encryption/encryption_metadata_ser.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>

namespace encryption {

ss::future<refill_result> refill_dek_sentinels(
  model::record_batch batch, std::optional<iobuf> previous_dek_metadata) {
    if (batch.compressed()) {
        co_return refill_result{
          .batch = std::move(batch),
          .last_dek_metadata = std::move(previous_dek_metadata),
        };
    }

    // First pass: determine the current DEK metadata and whether any
    // sentinels exist that require rebuilding.
    std::optional<iobuf> current_dek;
    bool has_sentinels = false;
    bool has_any_header = false;

    batch.for_each_record([&](model::record rec) {
        for (const auto& h : rec.headers()) {
            if (
              h.key_size() == static_cast<int32_t>(encryption_header_key.size())
              && h.key().linearize_to_string()
                   == ss::sstring{encryption_header_key}) {
                has_any_header = true;
                if (h.value_size() > 0) {
                    // Non-empty value: this is a full DEK header.
                    if (!current_dek) {
                        current_dek = h.value().copy();
                    }
                } else {
                    // Empty value: sentinel.
                    has_sentinels = true;
                }
            }
        }
    });

    if (!has_any_header) {
        co_return refill_result{
          .batch = std::move(batch),
          .last_dek_metadata = std::nullopt,
        };
    }

    // If we found no full DEK in this batch, fall back to the previous
    // batch's metadata.
    if (!current_dek && previous_dek_metadata) {
        current_dek = std::move(previous_dek_metadata);
    }

    if (!has_sentinels) {
        // No sentinels to refill -- return batch unchanged.
        std::optional<iobuf> out_dek;
        if (current_dek) {
            out_dek = current_dek->copy();
        }
        co_return refill_result{
          .batch = std::move(batch),
          .last_dek_metadata = std::move(out_dek),
        };
    }

    // Second pass: rebuild the batch, replacing sentinels with the
    // current DEK metadata.
    storage::record_batch_builder builder(
      batch.header().type, batch.base_offset());
    builder.set_compression(batch.header().attrs.compression());

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
            if (key_str == ss::sstring{encryption_header_key}) {
                iobuf hdr_key;
                hdr_key.append(
                  encryption_header_key.data(), encryption_header_key.size());
                if (h.value_size() > 0) {
                    // Full header -- keep as-is.
                    hdrs.emplace_back(std::move(hdr_key), h.value().copy());
                } else if (current_dek) {
                    // Sentinel -- replace with current DEK metadata.
                    hdrs.emplace_back(std::move(hdr_key), current_dek->copy());
                } else {
                    // Sentinel but no DEK metadata available -- keep
                    // sentinel.
                    hdrs.emplace_back(std::move(hdr_key), iobuf{});
                }
            } else {
                hdrs.push_back(h.copy());
            }
        }

        builder.add_raw_kw(std::move(key), std::move(value), std::move(hdrs));
    });

    std::optional<iobuf> out_dek;
    if (current_dek) {
        out_dek = current_dek->copy();
    }

    co_return refill_result{
      .batch = std::move(builder).build(),
      .last_dek_metadata = std::move(out_dek),
    };
}

} // namespace encryption
