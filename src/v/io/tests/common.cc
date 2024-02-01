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

#include "base/units.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/later.hh>

#include <random>
#include <span>

using engine_type
  = std::independent_bits_engine<std::mt19937, CHAR_BIT, unsigned char>;

namespace {
seastar::future<seastar::temporary_buffer<char>> make_random_data(
  size_t size, std::optional<uint64_t> alignment, engine_type* eng) {
    static constexpr size_t chunk_size = 32_KiB;

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
        std::generate(chunk.begin(), chunk.end(), [&] { return (*eng)(); });
        offset += len;

        co_await seastar::maybe_yield();
    }

    co_return data;
}
} // namespace

seastar::future<seastar::temporary_buffer<char>> make_random_data(
  size_t size,
  // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
  std::optional<uint64_t> alignment,
  std::optional<uint64_t> seed) {
    static thread_local std::random_device rd;
    static thread_local engine_type eng(rd());

    if (seed.has_value()) {
        engine_type seeded_eng(seed.value());
        co_return co_await make_random_data(size, alignment, &seeded_eng);
    }

    co_return co_await make_random_data(size, alignment, &eng);
}

seastar::lw_shared_ptr<io::page>
make_page(uint64_t offset, std::optional<uint64_t> seed) {
    return seastar::make_lw_shared<io::page>(
      offset, make_random_data(4096, 4096, seed).get());
}

void sleep_ms(unsigned min, unsigned max) {
    const auto ms = random_generators::get_int(min, max);
    seastar::sleep(std::chrono::milliseconds(ms)).get();
}
