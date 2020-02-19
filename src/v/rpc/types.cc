#include "rpc/types.h"

#include "reflection/for_each_field.h"

#include <fmt/format.h>

namespace rpc {

std::ostream& operator<<(std::ostream& o, const header& h) {
    o << "rpc::header(";
    reflection::for_each_field(h, [&o](auto& i) { o << "{" << i << "}"; });
    return o << ")";
}

std::ostream& operator<<(std::ostream& o, const server_configuration& c) {
    o << "{";
    for (auto& a : c.addrs) {
        o << a;
    }
    o << ", max_service_memory_per_core: " << c.max_service_memory_per_core
      << ", has_tls_credentials: " << (c.credentials ? "yes" : "no")
      << ", metrics_enabled:" << !c.disable_metrics;
    return o << "}";
}

} // namespace rpc
