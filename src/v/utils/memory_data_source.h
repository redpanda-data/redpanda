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

#include "base/seastarx.h"

#include <seastar/core/iostream.hh>

#include <numeric>
#include <vector>

class memory_data_source final : public ss::data_source_impl {
public:
    using value_type = ss::temporary_buffer<char>;
    using vector_type = std::vector<value_type>;

    explicit memory_data_source(value_type buffer) {
        add_buffer(std::move(buffer));
    }

    explicit memory_data_source(vector_type buffers) {
        _buffers.reserve(buffers.size());
        for (auto& buffer : buffers) {
            add_buffer(std::move(buffer));
        }
    }

    ss::future<value_type> skip(uint64_t n) final {
        _byte_offset = std::min(_byte_offset + n, _capacity);
        return get();
    }
    ss::future<value_type> get() final {
        if (_byte_offset >= _capacity) {
            return ss::make_ready_future<value_type>();
        }
        int64_t offset = _byte_offset;
        for (auto& _buffer : _buffers) {
            const int64_t end = _buffer.size();
            if (offset - end < 0) {
                _byte_offset += end - offset;
                return ss::make_ready_future<value_type>(
                  _buffer.share(offset, end));
            }
            offset -= end;
        }

        return ss::make_ready_future<value_type>();
    }

private:
    void add_buffer(value_type buffer) {
        _capacity += buffer.size();
        _buffers.push_back(std::move(buffer));
    }

    size_t _capacity = 0;
    vector_type _buffers;
    size_t _byte_offset = 0;
};
