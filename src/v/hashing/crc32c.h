#pragma once

#include "bytes/iobuf.h"
#include "utils/vint.h"

#include <crc32c/crc32c.h>

class crc32 {
public:
    template<typename T>
    CONCEPT(requires std::is_integral_v<T>)
    void extend(T num) noexcept {
        // NOLINTNEXTLINE
        extend(reinterpret_cast<const uint8_t*>(&num), sizeof(T));
    }

    /// FIXME: ugh. this imports so much junk into crc. move to util
    /// use templates and iterators
    void extend(const iobuf& buf) {
        auto in = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
        (void)in.consume(buf.size_bytes(), [this](const char* src, size_t sz) {
            // NOLINTNEXTLINE
            extend(reinterpret_cast<const uint8_t*>(src), sz);
            return ss::stop_iteration::no;
        });
    }

    void extend_vint(vint::value_type v) {
        std::array<bytes::value_type, vint::max_length> encoding_buffer{};
        const auto size = vint::serialize(v, encoding_buffer.begin());
        // NOLINTNEXTLINE
        extend(reinterpret_cast<const uint8_t*>(encoding_buffer.data()), size);
    }

    void extend(const uint8_t* data, size_t size) {
        _crc = crc32c::Extend(_crc, data, size);
    }

    uint32_t value() const { return _crc; }

private:
    uint32_t _crc = 0;
};
