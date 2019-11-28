#include "bytes/bytes.h"

sstring to_hex(bytes_view b) {
    static char digits[] = "0123456789abcdef";
    sstring out(sstring::initialized_later(), b.size() * 2);
    unsigned end = b.size();
    for (unsigned i = 0; i != end; ++i) {
        uint8_t x = b[i];
        out[2 * i] = digits[x >> 4];
        out[2 * i + 1] = digits[x & 0xf];
    }
    return out;
}

sstring to_hex(const bytes& b) { return to_hex(bytes_view(b)); }

std::ostream& operator<<(std::ostream& os, const bytes& b) {
    return os << to_hex(b);
}

std::ostream& operator<<(std::ostream& os, const bytes_opt& b) {
    if (b) {
        return os << *b;
    }
    return os << "empty";
}

namespace std {

std::ostream& operator<<(std::ostream& os, const bytes_view& b) {
    return os << to_hex(b);
}

} // namespace std
