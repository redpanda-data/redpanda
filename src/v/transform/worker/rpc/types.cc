/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "transform/worker/rpc/data_plane.h"

namespace transform::worker::rpc {

transform_data_request transform_data_request::serde_direct_read(
  iobuf_parser& in, const serde::header& h) {
    using serde::read_nested;

    auto id = read_nested<model::transform_id>(in, h._bytes_left_limit);
    auto version = read_nested<uuid_t>(in, h._bytes_left_limit);
    auto p = read_nested<model::partition_id>(in, h._bytes_left_limit);
    auto count = read_nested<size_t>(in, h._bytes_left_limit);
    chunked_vector<ss::foreign_ptr<std::unique_ptr<model::record_batch>>>
      batches;
    for (size_t i = 0; i < count; ++i) {
        batches.emplace_back(std::make_unique<model::record_batch>(
          read_nested<model::record_batch>(in, h._bytes_left_limit)));
    }
    return transform_data_request{
      .id = id,
      .transform_version = version,
      .partition = p,
      .batches = std::move(batches)};
}

ss::future<transform_data_request>
transform_data_request::serde_async_direct_read(
  iobuf_parser& in, serde::header h) {
    using serde::read_async_nested;
    auto id = co_await read_async_nested<model::transform_id>(
      in, h._bytes_left_limit);
    auto version = co_await read_async_nested<uuid_t>(in, h._bytes_left_limit);
    auto p = co_await read_async_nested<model::partition_id>(
      in, h._bytes_left_limit);
    auto count = co_await read_async_nested<size_t>(in, h._bytes_left_limit);
    chunked_vector<ss::foreign_ptr<std::unique_ptr<model::record_batch>>>
      batches;
    for (size_t i = 0; i < count; ++i) {
        batches.emplace_back(std::make_unique<model::record_batch>(
          co_await read_async_nested<model::record_batch>(
            in, h._bytes_left_limit)));
    }
    co_return transform_data_request{
      .id = id,
      .transform_version = version,
      .partition = p,
      .batches = std::move(batches)};
}

void transform_data_request::serde_write(iobuf& out) {
    using serde::write;
    write(out, id);
    write(out, transform_version);
    write(out, partition);
    write(out, batches.size());
    for (auto& batch : batches) {
        write(out, std::move(batch));
    }
}

ss::future<> transform_data_request::serde_async_write(iobuf& out) {
    using serde::write_async;
    co_await write_async(out, id);
    co_await write_async(out, transform_version);
    co_await write_async(out, partition);
    co_await write_async(out, batches.size());
    for (auto& batch : batches) {
        co_await write_async(out, std::move(batch));
    }
}

transformed_topic_output transformed_topic_output::serde_direct_read(
  iobuf_parser& in, const serde::header& h) {
    using serde::read_nested;
    auto topic = read_nested<model::topic>(in, h._bytes_left_limit);
    auto count = read_nested<size_t>(in, h._bytes_left_limit);
    chunked_vector<ss::foreign_ptr<std::unique_ptr<model::transformed_data>>>
      output;
    for (size_t i = 0; i < count; ++i) {
        output.emplace_back(std::make_unique<model::transformed_data>(
          read_nested<model::transformed_data>(in, h._bytes_left_limit)));
    }
    return {topic, std::move(output)};
}

ss::future<transformed_topic_output>
transformed_topic_output::serde_async_direct_read(
  iobuf_parser& in, serde::header h) {
    using serde::read_async_nested;
    auto topic = co_await read_async_nested<model::topic>(
      in, h._bytes_left_limit);
    auto count = co_await read_async_nested<size_t>(in, h._bytes_left_limit);
    chunked_vector<ss::foreign_ptr<std::unique_ptr<model::transformed_data>>>
      output;
    for (size_t i = 0; i < count; ++i) {
        output.emplace_back(std::make_unique<model::transformed_data>(
          co_await read_async_nested<model::transformed_data>(
            in, h._bytes_left_limit)));
    }
    co_return transformed_topic_output(topic, std::move(output));
}

void transformed_topic_output::serde_write(iobuf& out) {
    using serde::write;
    write(out, topic);
    write(out, output.size());
    for (auto& data : output) {
        write(out, std::move(data));
    }
}

ss::future<> transformed_topic_output::serde_async_write(iobuf& out) {
    using serde::write_async;
    co_await write_async(out, topic);
    co_await write_async(out, output.size());
    for (auto& data : output) {
        co_await write_async(out, std::move(data));
    }
}
} // namespace transform::worker::rpc
