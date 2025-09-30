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

auto fmt::formatter<cloud_topics::l1::io::errc>::format(
  const cloud_topics::l1::io::errc& err, fmt::format_context& ctx) const
  -> decltype(ctx.out()) {
    std::string_view name = "unknown";
    switch (err) {
    case cloud_topics::l1::io::errc::file_io_error:
        name = "file_io_error";
        break;
    case cloud_topics::l1::io::errc::cloud_missing_object:
        name = "cloud_missing_object";
        break;
    case cloud_topics::l1::io::errc::cloud_op_error:
        name = "cloud_op_error";
        break;
    case cloud_topics::l1::io::errc::cloud_op_timeout:
        name = "cloud_op_timeout";
        break;
    }
    return fmt::format_to(ctx.out(), "cloud_topics::l1::io::errc::{}", name);
}
