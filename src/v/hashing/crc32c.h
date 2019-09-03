#pragma once

#include "utils/fragbuf.h"
#include "utils/vint.h"

#include <crc32c/crc32c.h>

class crc32 {
public:
    template<typename T>
    CONCEPT(requires std::is_integral_v<T>)
    void extend(T num) noexcept {
        extend(reinterpret_cast<const uint8_t*>(&num), sizeof(T));
    }

    void extend(const fragbuf& buf) {
        auto istream = buf.get_istream();
        istream.consume([this](bytes_view bv) {
            extend(reinterpret_cast<const uint8_t*>(bv.data()), bv.size());
        });
    }

    void extend_vint(vint::value_type v) {
        std::array<bytes::value_type, vint::max_length> encoding_buffer;
        const auto size = vint::serialize(v, encoding_buffer.begin());
        extend(reinterpret_cast<const uint8_t*>(encoding_buffer.data()), size);
    }

    void extend(const uint8_t* data, size_t size) {
        _crc = crc32c::Extend(_crc, data, size);
    }

    uint32_t value() const {
        return _crc;
    }

private:
    uint32_t _crc = 0;
};