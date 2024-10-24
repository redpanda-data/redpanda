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

#include "base/seastarx.h"

#include <seastar/core/iostream.hh>

// Interface for returning input streams.
struct stream_provider {
    virtual ~stream_provider() = default;

    // Returns the stream, transferring ownership to the caller.
    virtual ss::input_stream<char> take_stream() = 0;

    // Closes the provider's stream, if appropriate (e.g. implementations may
    // no-op if the stream has been taken).
    virtual ss::future<> close() = 0;
};
