#pragma once

#include "seastarx.h"
#include "utils/named_type.h"
#include "redpanda/kafka/types.h"

#include <seastar/core/temporary_buffer.hh>

#include <cstdint>
#include <optional>
#include <string_view>

namespace kafka {

struct request_header {
    api_key key;
    api_version version;
    correlation_type correlation_id;
    temporary_buffer<char> client_id_buffer;
    std::optional<std::string_view> client_id;
};

std::ostream& operator<<(std::ostream&, const request_header&);

struct response_header {
    correlation_type correlation_id;
};

} // namespace kafka
