/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "kafka/protocol/response_writer.h"
#include "seastarx.h"

#include <seastar/core/sharded.hh>

#include <memory>

namespace kafka {

class response {
public:
    response() noexcept
      : _writer(_buf) {}

    response_writer& writer() { return _writer; }

    const iobuf& buf() const { return _buf; }
    iobuf& buf() { return _buf; }
    iobuf release() && { return std::move(_buf); }

    correlation_id correlation() const { return _correlation; }
    void set_correlation(correlation_id c) { _correlation = c; }

    /*
     * Marking a response as a noop means that it will be processed like any
     * other response (e.g. quota accounting) but the response won't written to
     * the connection. This is used by kafka producer with acks=0 in which the
     * client does not expect the broker to respond.
     */
    bool is_noop() const { return _noop; }
    void mark_noop() { _noop = true; }

private:
    bool _noop{false};
    correlation_id _correlation;
    iobuf _buf;
    response_writer _writer;
};

using response_ptr = ss::foreign_ptr<std::unique_ptr<response>>;

} // namespace kafka
