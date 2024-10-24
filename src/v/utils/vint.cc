/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "utils/vint.h"

#include "utils/vint_iostream.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>

#include <utility>

namespace unsigned_vint {

ss::future<std::pair<uint32_t, size_t>>
stream_deserialize(ss::input_stream<char>& s) {
    /// Consume up to 5 iterations (0-4) of reading 7 bits each time
    constexpr auto limit = ((max_length - 1) * 7);
    detail::var_decoder decoder(limit);
    while (!s.eof()) {
        auto buf = co_await s.read_exactly(1);
        if (buf.empty() || decoder.accept(buf[0])) {
            break;
        }
    }
    co_return std::make_pair(
      static_cast<uint32_t>(decoder.result), decoder.bytes_read);
}

} // namespace unsigned_vint
