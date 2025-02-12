/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"

#include <seastar/core/iostream.hh>
#include <seastar/util/later.hh>

namespace utils {
// Data sink for noop output_stream instance
// needed to implement scanning
struct null_data_sink final : ss::data_sink_impl {
    ss::future<> put(ss::net::packet data) final;
    ss::future<> put(std::vector<ss::temporary_buffer<char>> all) final;
    ss::future<> put(ss::temporary_buffer<char>) final;
    ss::future<> flush() final;
    ss::future<> close() final;
};

ss::output_stream<char> make_null_output_stream();

} // namespace utils
