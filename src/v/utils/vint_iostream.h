/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include <seastar/core/future.hh>

#include <utility>

namespace seastar {
template<typename T>
class input_stream;
}

namespace unsigned_vint {
seastar::future<std::pair<uint32_t, size_t>>
stream_deserialize(seastar::input_stream<char>&);
}
