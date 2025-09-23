/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/abstract_io.h"

#include "bytes/iostream.h"

namespace cloud_topics::l1 {

ss::future<ss::input_stream<char>> io::read_file(staging_file* file) {
    return file->input_stream();
}

ss::future<std::expected<iobuf, io::errc>>
io::read_object_as_iobuf(object_extent extent, ss::abort_source* as) {
    auto result = co_await this->read_object(extent, as);
    if (!result) {
        co_return std::unexpected(result.error());
    }
    auto& stream = result.value();
    co_return co_await read_iobuf_exactly(stream, extent.size)
      .finally([&stream] { return stream.close(); });
}

} // namespace cloud_topics::l1
