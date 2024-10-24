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

#include "model/record.h"
#include "model/record_batch_reader.h"

namespace storage::internal {

/// \brief Decompress over a model::record_batch_reader
class decompress_batch_consumer {
public:
    ss::future<ss::stop_iteration> operator()(model::record_batch&);
    model::record_batch_reader end_of_stream();

private:
    model::record_batch_reader::data_t _batches;
};

/// \brief Apply compressor over a model::record_batch_reader
class compress_batch_consumer {
public:
    compress_batch_consumer(model::compression, std::size_t threshold) noexcept;
    ss::future<ss::stop_iteration> operator()(model::record_batch&);
    model::record_batch_reader end_of_stream();

private:
    model::compression _compression_type{model::compression::none};
    std::size_t _threshold{0};
    model::record_batch_reader::data_t _batches;
};

/// \brief batch decompression
ss::future<model::record_batch> decompress_batch(model::record_batch&&);
/// \brief batch decompression
ss::future<model::record_batch> decompress_batch(const model::record_batch&);
/// \brief synchronous batch decompression
model::record_batch decompress_batch_sync(model::record_batch&&);
/// \brief synchronous batch decompression
/// \throw std::runtime_error If provided batch is not compressed
model::record_batch maybe_decompress_batch_sync(const model::record_batch&);

/// \brief batch compression
ss::future<model::record_batch>
  compress_batch(model::compression, model::record_batch);

/// \brief resets the size, header crc and payload crc
void reset_size_checksum_metadata(model::record_batch_header&, const iobuf&);

inline bool is_zero(const char* data, size_t size) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    return data[0] == 0 && memcmp(data, data + 1, size - 1) == 0;
}

inline bool is_zero(const iobuf& buffer) {
    if (buffer.empty()) {
        return false;
    }
    bool ret = true;
    iobuf::iterator_consumer in(buffer.cbegin(), buffer.cend());
    in.consume(buffer.size_bytes(), [&ret](const char* src, size_t len) {
        if (!is_zero(src, len)) {
            ret = false;
            return ss::stop_iteration::yes;
        }
        return ss::stop_iteration::no;
    });
    return ret;
}

} // namespace storage::internal
