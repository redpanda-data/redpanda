#include "rpc/types.h"

#include "rpc/for_each_field.h"

#include <fmt/format.h>

namespace rpc {

std::ostream& operator<<(std::ostream& o, const header& h) {
    o << "rpc::header(";
    for_each_field(h, [&o](auto& i) { o << "{" << i << "}"; });
    return o << ")";
}
} // namespace rpc
