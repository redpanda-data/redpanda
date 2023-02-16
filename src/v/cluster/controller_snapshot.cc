/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/controller_snapshot.h"

namespace cluster {

ss::future<> controller_snapshot::serde_async_write(iobuf& out) {
    co_await serde::write_async(out, std::move(bootstrap));
}

ss::future<>
controller_snapshot::serde_async_read(iobuf_parser& in, serde::header const h) {
    bootstrap = co_await serde::read_async_nested<decltype(bootstrap)>(
      in, h._bytes_left_limit);

    if (in.bytes_left() > h._bytes_left_limit) {
        in.skip(in.bytes_left() - h._bytes_left_limit);
    }
}

} // namespace cluster
