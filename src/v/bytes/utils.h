#pragma once
#include "bytes/iobuf.h"
#include "hashing/crc32c.h"

inline void crc_extend_iobuf(crc32& crc, const iobuf& buf) {
    auto in = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
    (void)in.consume(buf.size_bytes(), [&crc](const char* src, size_t sz) {
        // NOLINTNEXTLINE
        crc.extend(reinterpret_cast<const uint8_t*>(src), sz);
        return ss::stop_iteration::no;
    });
}
