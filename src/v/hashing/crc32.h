/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include <cstdint>
#include <zlib.h> // provides ::crc32

namespace crc {

class crc32 {
public:
    crc32()
      : _crc(::crc32(0L, Z_NULL, 0)) {}

    void extend(const uint8_t* data, size_t size) {
        _crc = ::crc32(_crc, data, size);
    }

    void extend(const char* data, size_t size) {
        extend(
          reinterpret_cast<const uint8_t*>(data), // NOLINT
          size);
    }

    uint32_t value() const { return _crc; }

private:
    uint32_t _crc;
};

} // namespace crc
