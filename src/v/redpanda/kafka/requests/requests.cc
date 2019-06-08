#include "redpanda/kafka/requests/headers.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>
#include <seastar/util/log.hh>

namespace kafka::requests {

std::ostream& operator<<(std::ostream& os, const api_key& key) {
    return seastar::fmt_print(os, "{{api_key: {}}}", key.value());
}

std::ostream& operator<<(std::ostream& os, const api_version& version) {
    return seastar::fmt_print(os, "{{api_version: {}}}", version.value());
}

std::ostream& operator<<(std::ostream& os, const request_header& header) {
    seastar::fmt_print(
      os,
      "{{request_header: {}, {}, {{correlation_id: {}}}, ",
      header.key,
      header.version);
    if (header.client_id) {
        return seastar::fmt_print(os, "{{client_id: {}}}}}", header.client_id);
    }
    return os << "{no client_id}}";
}

} // namespace kafka::requests