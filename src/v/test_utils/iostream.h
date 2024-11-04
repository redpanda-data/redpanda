// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>

namespace tests {

/// Create an input stream that throws an exception on first interaction.
template<class Err>
ss::input_stream<char> make_throwing_stream(Err err) {
    struct throwing_stream final : ss::data_source_impl {
        explicit throwing_stream(Err e)
          : _err(std::move(e)) {}

        ss::future<ss::temporary_buffer<char>> skip(uint64_t) final {
            return get();
        }

        ss::future<ss::temporary_buffer<char>> get() final {
            return ss::make_exception_future<ss::temporary_buffer<char>>(
              std::move(_err));
        }

        Err _err;
    };
    auto ds = ss::data_source(std::make_unique<throwing_stream>(err));
    return ss::input_stream<char>(std::move(ds));
}

} // namespace tests
