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
#include "model/record.h"
#include "model/transform.h"
#include "serde/envelope.h"

namespace transform::worker::rpc {

struct transform_data_request
  : serde::envelope<
      transform_data_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::transform_id id;
    uuid_t transform_version;
    chunked_vector<model::record_batch> batches;

    friend bool
    operator==(const transform_data_request&, const transform_data_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const transform_data_request&);

    auto serde_fields() { return std::tie(id, transform_version, batches); }
};

struct transform_data_reply
  : serde::envelope<
      transform_data_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    std::error_code error_code;
    chunked_vector<model::record_batch> batches;
    auto serde_fields() { return std::tie(error_code, batches); }
};

} // namespace transform::worker::rpc
