#include "rpc/server_probe.h"

namespace rpc {
std::ostream& operator<<(std::ostream& o, const server_probe& p) {
    o << "{"
      << "connects: " << p.get_connects() << ", "
      << "current connections: " << p.get_connections() << ", "
      << "connection close errors: " << p.get_connection_close_errors() << ", "
      << "requests completed: " << p.get_requests_completed() << ", "
      << "received bytes: " << p.get_in_bytes() << ", "
      << "sent bytes: " << p.get_out_bytes() << ", "
      << "bad requests: " << p.get_bad_requests() << ", "
      << "corrupted headers: " << p.get_corrupted_headers() << ", "
      << "method not found errors: " << p.get_method_not_found_errors() << ", "
      << "requests blocked by memory: " << p.get_requests_blocked_memory()
      << "}";
    return o;
}
} // namespace rpc
