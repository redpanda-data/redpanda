#pragma once

#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "kafka/types.h"

#include <seastar/core/iostream.hh>
#include <seastar/core/scattered_message.hh>
#include <seastar/core/temporary_buffer.hh>

#include <optional>

namespace kafka {

// TODO: move to iobuf_parser
ss::future<std::optional<request_header>> parse_header(ss::input_stream<char>&);

size_t parse_size_buffer(ss::temporary_buffer<char>&);
ss::future<std::optional<size_t>> parse_size(ss::input_stream<char>&);

ss::scattered_message<char>
response_as_scattered(response_ptr response, correlation_id correlation);

} // namespace kafka
