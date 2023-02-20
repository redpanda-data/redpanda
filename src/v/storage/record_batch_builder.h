/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "bytes/iobuf.h"
#include "model/record.h"
#include "seastarx.h"
#include "utils/vint.h"

namespace storage {
class record_batch_builder {
public:
    record_batch_builder(model::record_batch_type, model::offset);
    record_batch_builder(record_batch_builder&&) = default;
    record_batch_builder(const record_batch_builder&) = delete;
    record_batch_builder& operator=(record_batch_builder&&) = default;
    record_batch_builder& operator=(const record_batch_builder&) = delete;

    virtual record_batch_builder&
    add_raw_kv(std::optional<iobuf>&& key, std::optional<iobuf>&& value) {
        _records.emplace_back(std::move(key), std::move(value));
        return *this;
    }
    virtual record_batch_builder& add_raw_kw(
      std::optional<iobuf>&& key,
      std::optional<iobuf>&& value,
      std::vector<model::record_header> headers) {
        _records.emplace_back(
          std::move(key), std::move(value), std::move(headers));
        return *this;
    }
    virtual model::record_batch build() &&;
    virtual ~record_batch_builder();

    void set_producer_identity(int64_t id, int16_t epoch) {
        _producer_id = id;
        _producer_epoch = epoch;
    }

    void set_control_type() { _is_control_type = true; }

    void set_transactional_type() { _transactional_type = true; }

    void set_compression(model::compression c) { _compression = c; }

    void set_timestamp(model::timestamp ts) { _timestamp = ts; }

    /*
     * Returns true if no records have been added, and false otherwise.
     */
    bool empty() const { return _records.empty(); }

private:
    static constexpr int64_t zero_vint_size = vint::vint_size(0);
    struct serialized_record {
        serialized_record(
          std::optional<iobuf> k,
          std::optional<iobuf> v,
          std::vector<model::record_header> hdrs
          = std::vector<model::record_header>())
          : headers(std::move(hdrs)) {
            if (k) {
                key = std::move(*k);
                encoded_key_size = key.size_bytes();
            } else {
                encoded_key_size = -1;
            }
            if (likely(v)) {
                value = std::move(*v);
                encoded_value_size = value.size_bytes();
            } else {
                encoded_value_size = -1;
            }
        }

        iobuf key;
        int32_t encoded_key_size;
        iobuf value;
        int32_t encoded_value_size;
        std::vector<model::record_header> headers;
    };

    uint32_t record_size(int32_t offset_delta, const serialized_record& r);

    model::record_batch_type _batch_type;
    model::offset _base_offset;
    int64_t _producer_id{-1};
    int16_t _producer_epoch{-1};
    bool _is_control_type{false};
    bool _transactional_type{false};
    std::vector<serialized_record> _records;
    model::compression _compression{model::compression::none};
    std::optional<model::timestamp> _timestamp;
};
} // namespace storage
