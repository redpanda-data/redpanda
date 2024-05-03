// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "model/record.h"
#include "storage/mvlog/entry.h"

namespace experimental::io {
class pager;
} // namespace experimental::io

namespace storage::experimental::mvlog {

// Encapsulates writes to a given segment file.
//
// This class is not thread-safe. It is expected that callers will externally
// serialize calls into the appender.
class segment_appender {
public:
    explicit segment_appender(::experimental::io::pager* pager)
      : pager_(pager) {}

    // Serializes and appends the given batch to the underlying segment.
    // Callers are expected to flush the file after this returns.
    ss::future<> append(model::record_batch);

private:
    // The pager with which to perform I/O.
    ::experimental::io::pager* pager_;
};

} // namespace storage::experimental::mvlog
