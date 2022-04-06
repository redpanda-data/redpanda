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
#include <crc32c/crc32c.h>

#include <type_traits>

namespace crc {

class crc32c {
public:
    template<typename T, typename = std::enable_if_t<std::is_integral_v<T>, T>>
    void extend(T num) noexcept {
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

    uint32_t value() const { return _crc; }

private:
    uint32_t _crc = 0;
};

} // namespace crc
