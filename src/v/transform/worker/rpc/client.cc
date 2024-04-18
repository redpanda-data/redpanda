/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "transform/worker/rpc/client.h"

#include "model/timeout_clock.h"
#include "rpc/types.h"
#include "transform/worker/rpc/control_plane.h"
#include "transform/worker/rpc/rpc_service.h"

#include <seastar/core/coroutine.hh>

#include <type_traits>

namespace transform::worker::rpc {

client::client(
  ::rpc::reconnect_transport transport, model::timeout_clock::duration timeout)
  : _transport(std::move(transport))
  , _timeout(timeout) {}

template<auto Method>
auto client::call(auto request) -> ss::future<result<
  typename std::invoke_result_t<
    decltype(Method),
    transform_worker_client_protocol&,
    std::decay_t<decltype(request)>,
    ::rpc::client_opts>::value_type::value_type::data_type,
  errc>> {
    auto transport = co_await _transport.get_connected(_timeout);
    if (!transport) {
        // TODO(rockwood): log error
        co_return errc::network_error;
    }
    // TODO: Retry network errors?
    auto protocol = transform_worker_client_protocol(
      std::move(transport.value()));
    auto result = co_await std::invoke(
      Method,
      protocol,
      std::move(request),
      ::rpc::client_opts(::rpc::timeout_spec::from_now(_timeout)));
    if (result.error()) {
        // TODO(rockwood): log error
        co_return errc::network_error;
    }
    co_return std::move(result.value().data);
}

ss::future<result<chunked_vector<vm_status>, errc>> client::list_status() {
    auto result
      = co_await call<&transform_worker_client_protocol::compute_current_state>(
        current_state_request{});
    if (result.has_error()) {
        co_return result.error();
    }
    co_return std::move(result.value().state);
}

ss::future<errc> client::stop_vm(model::transform_id id, uuid_t version) {
    auto result = co_await call<&transform_worker_client_protocol::stop_vm>(
      stop_vm_request{
        .id = id,
        .transform_version = version,
      });
    if (result.has_error()) {
        co_return result.error();
    }
    co_return result.value().error_code;
}

ss::future<errc> client::start_vm(
  model::transform_id id, model::transform_metadata meta, iobuf binary) {
    auto result = co_await call<&transform_worker_client_protocol::start_vm>(
      start_vm_request{
        .id = id,
        .metadata = meta,
        .wasm_binary = std::move(binary),
      });
    if (result.has_error()) {
        co_return result.error();
    }
    co_return result.value().error_code;
}

ss::future<result<std::vector<transformed_topic_output>, errc>>
client::transform(
  model::transform_id id,
  uuid_t version,
  model::partition_id partition,
  chunked_vector<model::record_batch> batches) {
    auto result
      = co_await call<&transform_worker_client_protocol::transform_data>(
        transform_data_request{
          .id = id,
          .transform_version = version,
          .partition = partition,
          .batches = std::move(batches),
        });
    if (result.has_error()) {
        co_return result.error();
    }
    if (result.value().error_code != errc::success) {
        co_return result.value().error_code;
    }
    co_return std::move(result.value().output);
}

} // namespace transform::worker::rpc
