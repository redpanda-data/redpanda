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

#include "cluster/self_test_rpc_types.h"

#include <seastar/core/coroutine.hh>

namespace cluster {

ss::sstring self_test_status_as_string(self_test_status sts) {
    switch (sts) {
    case self_test_status::idle:
        return "idle";
    case self_test_status::running:
        return "running";
    case self_test_status::unreachable:
        return "unreachable";
    default:
        __builtin_unreachable();
    }
}

std::ostream& operator<<(std::ostream& o, self_test_status sts) {
    fmt::print(o, "{}", self_test_status_as_string(sts));
    return o;
}

ss::future<cluster::netcheck_request>
make_netcheck_request(model::node_id src, size_t sz) {
    static const size_t fragment_size = 8192;
    auto frag_sizes = std::vector<size_t>(sz / fragment_size, fragment_size);
    if ((sz % fragment_size) != 0) {
        frag_sizes.push_back(sz % fragment_size);
    }
    auto buffers = co_await ssx::parallel_transform(
      frag_sizes, [](size_t size) {
          ss::temporary_buffer<char> buf(size);
          random_generators::fill_buffer_randomchars(buf.get_write(), size);
          return buf;
      });
    iobuf iob;
    for (auto itr = buffers.rbegin(); itr != buffers.rend(); ++itr) {
        /// iobuf::prepend(temp_buf) performs no auto-trimming
        iob.prepend(std::move(*itr));
    }
    co_return cluster::netcheck_request{.source = src, .buf = std::move(iob)};
}

} // namespace cluster
