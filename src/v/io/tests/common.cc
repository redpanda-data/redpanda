/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "io/tests/common.h"

#include "units.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/later.hh>

#include <random>
#include <span>

seastar::future<seastar::temporary_buffer<char>>
make_random_data(size_t size, std::optional<uint64_t> alignment) {
    static constexpr size_t chunk_size = 32_KiB;

    static thread_local std::random_device rd;
    static thread_local std::
      independent_bits_engine<std::mt19937, CHAR_BIT, unsigned char>
        eng(rd());

    auto data = [&] {
        if (alignment.has_value()) {
            return seastar::temporary_buffer<char>::aligned(
              alignment.value(), size);
        }
        return seastar::temporary_buffer<char>(size);
    }();

    uint64_t offset = 0;
    std::span<char> span(data.get_write(), data.size());

    while (true) {
        auto len = std::min(span.size() - offset, chunk_size);
        if (len == 0) {
            break;
        }

        auto chunk = span.subspan(offset, len);
        std::generate(chunk.begin(), chunk.end(), [&] { return eng(); });
        offset += len;

        co_await seastar::maybe_yield();
    }

    co_return data;
}
