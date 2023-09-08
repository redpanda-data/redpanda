// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cluster/fwd.h"
#include "model/fundamental.h"
#include "transform/rpc/deps.h"
#include "transform/rpc/rpc_service.h"
#include "transform/rpc/serde.h"

#include <seastar/core/sharded.hh>

#include <memory>

namespace transform::rpc {

/**
 * A per core sharded service that handles custom data path requests for data
 * transforms.
 */
class local_service {
public:
    local_service(
      std::unique_ptr<topic_metadata_cache> metadata_cache,
      std::unique_ptr<partition_manager> partition_manager);

    ss::future<ss::chunked_fifo<transformed_topic_data_result>> produce(
      ss::chunked_fifo<transformed_topic_data> topic_data,
      model::timeout_clock::duration timeout);

private:
    ss::future<transformed_topic_data_result>
      produce(transformed_topic_data, model::timeout_clock::duration);

    std::unique_ptr<topic_metadata_cache> _metadata_cache;
    std::unique_ptr<partition_manager> _partition_manager;
};

/**
 * A networked wrapper for the local service.
 */
class network_service final : public impl::transform_rpc_service {
public:
    network_service(
      ss::scheduling_group sc,
      ss::smp_service_group ssg,
      ss::sharded<local_service>* service)
      : impl::transform_rpc_service{sc, ssg}
      , _service(service) {}

    ss::future<produce_reply>
    produce(produce_request&&, ::rpc::streaming_context&) override;

private:
    ss::sharded<local_service>* _service;
};

} // namespace transform::rpc
