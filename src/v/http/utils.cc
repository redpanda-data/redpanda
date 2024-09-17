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

#include "http/utils.h"

namespace http {

ss::future<boost::beast::http::status>
status(http::client::response_stream_ref response) {
    co_await response->prefetch_headers();
    co_return response->get_headers().result();
}

ss::future<iobuf> drain(http::client::response_stream_ref response) {
    iobuf buffer;
    while (!response->is_done()) {
        buffer.append(co_await response->recv_some());
    }
    co_return buffer;
}

} // namespace http
