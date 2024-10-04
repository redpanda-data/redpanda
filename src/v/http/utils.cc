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

#include <boost/algorithm/string/join.hpp>

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

iobuf form_encode_data(absl::flat_hash_map<ss::sstring, ss::sstring> data) {
    std::vector<ss::sstring> pairs;
    for (const auto& [k, v] : data) {
        pairs.emplace_back(fmt::format("{}={}", k, v));
    }
    return iobuf::from(boost::algorithm::join(pairs, "&"));
}

} // namespace http
