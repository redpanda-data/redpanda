#include "bytes/bytes.h"

ss::sstring to_hex(bytes_view b) {
    static constexpr std::string_view digits{"0123456789abcdef"};
    ss::sstring out = ss::uninitialized_string(b.size() * 2);
    unsigned end = b.size();
    for (unsigned i = 0; i != end; ++i) {
        uint8_t x = b[i];
        out[2 * i] = digits[x >> uint8_t(4)];
        out[2 * i + 1] = digits[x & uint8_t(0xf)];
    }
    return out;
}

ss::sstring to_hex(const bytes& b) { return to_hex(bytes_view(b)); }

std::ostream& operator<<(std::ostream& os, const bytes& b) {
    return os << to_hex(b);
}

std::ostream& operator<<(std::ostream& os, const bytes_opt& b) {
    if (b) {
        return os << *b;
    }
    return os << "empty";
}

std::ostream& operator<<(std::ostream& os, const bytes_view& b) {
    return os << to_hex(b);
}
