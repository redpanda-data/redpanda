/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "model/timeout_clock.h"
#include "model/transform.h"
#include "rpc/reconnect_transport.h"
#include "transform/worker/rpc/control_plane.h"
#include "transform/worker/rpc/data_plane.h"

namespace transform::worker::rpc {

class transform_worker_client_protocol;

/**
 * A client to a worker node, from a broker.
 */
class client {
public:
    client(
      ::rpc::reconnect_transport transport, model::timeout_clock::duration);

    ss::future<> stop();

    /**
     * List the current status of all VMs on the node.
     */
    ss::future<result<chunked_vector<vm_status>, errc>> list_status();

    /**
     * Start a VM.
     */
    ss::future<errc>
      start_vm(model::transform_id, model::transform_metadata, iobuf);

    /**
     * Stop a VM.
     */
    ss::future<errc> stop_vm(model::transform_id, uuid_t);

    /**
     * Transform the given records.
     */
    ss::future<result<std::vector<transformed_topic_output>, errc>> transform(
      model::transform_id,
      uuid_t,
      model::partition_id,
      chunked_vector<ss::foreign_ptr<std::unique_ptr<model::record_batch>>>);

private:
    template<auto Method>
    auto call(auto request)
      -> ss::future<result<
        typename std::invoke_result_t<
          decltype(Method),
          transform_worker_client_protocol&,
          std::decay_t<decltype(request)>,
          ::rpc::client_opts>::value_type::value_type::data_type,
        errc>>;

    ::rpc::reconnect_transport _transport;
    model::timeout_clock::duration _timeout;
};

} // namespace transform::worker::rpc
