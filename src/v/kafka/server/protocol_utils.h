/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/protocol/types.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "kafka/types.h"

#include <seastar/core/iostream.hh>
#include <seastar/core/scattered_message.hh>
#include <seastar/core/temporary_buffer.hh>

#include <optional>

namespace kafka {

ss::future<std::pair<std::optional<tagged_fields>, size_t>>
parse_tags(ss::input_stream<char>&);

// TODO: move to iobuf_parser
ss::future<std::optional<request_header>> parse_header(ss::input_stream<char>&);

size_t parse_size_buffer(ss::temporary_buffer<char>);
ss::future<std::optional<size_t>> parse_size(ss::input_stream<char>&);

ss::scattered_message<char> response_as_scattered(response_ptr response);

} // namespace kafka
