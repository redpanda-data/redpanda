// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/outcome.h"
#include "base/seastarx.h"
#include "storage/mvlog/entry.h"
#include "storage/mvlog/errc.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

namespace storage::experimental::mvlog {

// Wrapper around an input stream to produce entries. The given stream is
// expected to be comprised of contiguous entries.
class entry_stream {
public:
    explicit entry_stream(ss::input_stream<char> stream)
      : stream_(std::move(stream)) {}

    // Reads from the input stream, deserializing an entry and returning it.
    // Returns a well-formed entry.
    //
    // Returns std::nullopt if there isn't a complete entry, signalling the end
    // of the stream.
    //
    // Returns an error if there is a problem with the underlying stream (e.g.
    // a bad checksum).
    ss::future<result<std::optional<entry>, errc>> next();

private:
    ss::input_stream<char> stream_;
};

} // namespace storage::experimental::mvlog
