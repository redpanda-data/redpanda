#include "cluster/types.h"

namespace cluster {

std::ostream& operator<<(std::ostream& o, topic_error_code code) {
    o << "{ topic_error_code: ";
    if (
      code > topic_error_code::topic_error_code_max
      || code < topic_error_code::topic_error_code_min) {
        o << "out_of_range";
    } else {
        o << topic_error_code_names[static_cast<int16_t>(code)];
    }
    o << " }";
    return o;
}
} // namespace cluster