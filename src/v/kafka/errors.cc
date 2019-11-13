#include "kafka/errors.h"

#include <iostream>

namespace kafka {

std::ostream& operator<<(std::ostream& o, error_code code) {
    o << "{ error_code: ";
    // special case as unknown_server_error = -1
    if (code == error_code::unknown_server_error) {
        o << "unknown_server_error";
    } else {
        o << error_code_names[static_cast<int16_t>(code)];
    }
    o << " [" << (int16_t)code << "] }";
    return o;
}

} // namespace kafka
