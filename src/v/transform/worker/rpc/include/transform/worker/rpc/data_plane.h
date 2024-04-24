/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/transform.h"
#include "serde/envelope.h"
#include "transform/worker/rpc/errc.h"

#include <seastar/core/sharded.hh>

namespace transform::worker::rpc {

struct transform_data_request
  : public serde::envelope<
      transform_data_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::transform_id id;
    decltype(model::transform_metadata::uuid) transform_version;
    model::partition_id partition;
    chunked_vector<ss::foreign_ptr<std::unique_ptr<model::record_batch>>>
      batches;

    friend bool
    operator==(const transform_data_request&, const transform_data_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const transform_data_request&);

    static transform_data_request
    serde_direct_read(iobuf_parser& in, const serde::header& h);
    static ss::future<transform_data_request>
    serde_async_direct_read(iobuf_parser& in, serde::header h);
    void serde_write(iobuf& out);
    ss::future<> serde_async_write(iobuf& out);
};

struct transformed_topic_output
  : public serde::envelope<
      transformed_topic_output,
      serde::version<0>,
      serde::compat_version<0>> {
    transformed_topic_output() noexcept = default;
    transformed_topic_output(
      model::topic topic,
      chunked_vector<ss::foreign_ptr<std::unique_ptr<model::transformed_data>>>
        output)
      : topic(std::move(topic))
      , output(std::move(output)) {}
    model::topic topic;
    chunked_vector<ss::foreign_ptr<std::unique_ptr<model::transformed_data>>>
      output;

    static transformed_topic_output
    serde_direct_read(iobuf_parser& in, const serde::header& h);
    static ss::future<transformed_topic_output>
    serde_async_direct_read(iobuf_parser& in, serde::header h);
    void serde_write(iobuf& out);
    ss::future<> serde_async_write(iobuf& out);
};

struct transform_data_reply
  : public serde::envelope<
      transform_data_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    errc error_code = errc::success;
    std::vector<transformed_topic_output> output;
    auto serde_fields() { return std::tie(error_code, output); }
};

} // namespace transform::worker::rpc
