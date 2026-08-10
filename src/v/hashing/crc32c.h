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
#include "absl/crc/crc32c.h"
#include "bytes/iobuf.h"

#include <crc32c/crc32c.h>

#include <string_view>
#include <type_traits>

namespace crc {

/// Backends for basic_crc32c, each computing CRC32C (Castagnoli) as defined
/// by RFC 3720 with a different library. Backends produce identical values
/// -- hashing/tests/crc32c_tests.cc pins each against the RFC vectors -- so
/// the choice between them is purely one of performance.

struct crc32c_google_backend {
    static uint32_t extend(uint32_t crc, const char* data, size_t size) {
        return ::crc32c::Extend(
          crc,
          // NOLINTNEXTLINE
          reinterpret_cast<const uint8_t*>(data),
          size);
    }
};

struct crc32c_abseil_backend {
    static uint32_t extend(uint32_t crc, const char* data, size_t size) {
        if (size == 0) {
            // callers pass empty fragments; don't build a view over nullptr
            return crc;
        }
        return static_cast<uint32_t>(absl::ExtendCrc32c(
          absl::crc32c_t{crc}, std::string_view{data, size}));
    }
};

/// Incremental CRC32C (Castagnoli), as defined by RFC 3720, computed by
/// Backend.
template<typename Backend>
class basic_crc32c {
public:
    using value_type = uint32_t;

    template<typename T>
    void extend(T num) noexcept
    requires(std::is_integral_v<T>)
    {
        // NOLINTNEXTLINE
        extend(reinterpret_cast<const char*>(&num), sizeof(T));
    }
    void extend(const uint8_t* data, size_t size) {
        extend(
          // NOLINTNEXTLINE
          reinterpret_cast<const char*>(data),
          size);
    }
    void extend(const char* data, size_t size) {
        _crc = Backend::extend(_crc, data, size);
    }

    value_type value() const { return _crc; }

private:
    value_type _crc = 0;
};

/// The instantiation production code uses. Backed by abseil, which inlines
/// buffers of 64 bytes or less instead of dispatching through a call. That is
/// what most callers do -- see model::internal_header_only_crc, which feeds a
/// batch header through as twelve integers -- and it is worth roughly 15x
/// over an out-of-line call on that path.
using crc32c = basic_crc32c<crc32c_abseil_backend>;

} // namespace crc

template<typename Backend>
inline void
crc_extend_iobuf(crc::basic_crc32c<Backend>& crc, const iobuf& buf) {
    auto in = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
    (void)in.consume(buf.size_bytes(), [&crc](const char* src, size_t sz) {
        // NOLINTNEXTLINE
        crc.extend(reinterpret_cast<const uint8_t*>(src), sz);
        return ss::stop_iteration::no;
    });
}
