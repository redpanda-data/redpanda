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

#include <crc32c/crc32c.h>

#include <type_traits>

namespace crc {

class crc32c {
public:
    using value_type = uint32_t;

    template<typename T>
    void extend(T num) noexcept
    requires(std::is_integral_v<T>)
    {
        // NOLINTNEXTLINE
        extend(reinterpret_cast<const uint8_t*>(&num), sizeof(T));
    }
    void extend(const uint8_t* data, size_t size) {
        _crc = ::crc32c::Extend(_crc, data, size);
    }
    void extend(const char* data, size_t size) {
        extend(
          // NOLINTNEXTLINE
          reinterpret_cast<const uint8_t*>(data),
          size);
    }

    value_type value() const { return _crc; }

private:
    value_type _crc = 0;
};

} // namespace crc

inline void crc_extend_iobuf(crc::crc32c& crc, const iobuf& buf) {
    auto in = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
    (void)in.consume(buf.size_bytes(), [&crc](const char* src, size_t sz) {
        // NOLINTNEXTLINE
        crc.extend(reinterpret_cast<const uint8_t*>(src), sz);
        return ss::stop_iteration::no;
    });
}
