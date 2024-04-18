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
#include "model/transform.h"
#include "serde/envelope.h"
#include "transform/worker/rpc/errc.h"

namespace transform::worker::rpc {

struct current_state_request
  : serde::envelope<
      current_state_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    auto serde_fields() { return std::tie(); }
    friend bool
    operator==(const current_state_request&, const current_state_request&)
      = default;
};

enum class vm_state {
    starting,
    running,
    unknown,
};

/**
 * The status of an individual VM in the reconciliation loop.
 */
struct vm_status
  : public serde::
      envelope<vm_status, serde::version<0>, serde::compat_version<0>> {
    vm_status() noexcept = default;
    vm_status(
      model::transform_id id,
      decltype(model::transform_metadata::uuid) transform_version,
      vm_state state) noexcept
      : id(id)
      , transform_version(transform_version)
      , state(state) {}

    model::transform_id id;
    decltype(model::transform_metadata::uuid) transform_version;
    vm_state state;

    auto serde_fields() { return std::tie(id, transform_version, state); }
    friend bool operator==(const vm_status&, const vm_status&) = default;
};

/**
 * The state of all VMs on this worker node.
 */
struct current_state_reply
  : public serde::envelope<
      current_state_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    chunked_vector<vm_status> state;

    auto serde_fields() { return std::tie(state); }
    friend bool
    operator==(const current_state_reply&, const current_state_reply&)
      = default;
};

/**
 * Request to start a VM with the given metadata.
 */
struct start_vm_request
  : public serde::
      envelope<start_vm_request, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::transform_id id;
    model::transform_metadata metadata;
    iobuf wasm_binary;

    auto serde_fields() { return std::tie(id, metadata, wasm_binary); }
    friend bool operator==(const start_vm_request&, const start_vm_request&)
      = default;
};

/**
 * The result of starting a VM.
 */
struct start_vm_reply
  : public serde::
      envelope<start_vm_reply, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    errc error_code;

    auto serde_fields() { return std::tie(error_code); }

    friend bool operator==(const start_vm_reply&, const start_vm_reply&)
      = default;
};

/**
 * Request to stop a VM on this worker.
 */
struct stop_vm_request
  : public serde::
      envelope<stop_vm_request, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::transform_id id;
    decltype(model::transform_metadata::uuid) transform_version;

    auto serde_fields() { return std::tie(id, transform_version); }

    friend bool operator==(const stop_vm_request&, const stop_vm_request&)
      = default;
};

/**
 * The result of stopping a VM on this worker node.
 */
struct stop_vm_reply
  : public serde::
      envelope<stop_vm_reply, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    errc error_code;

    auto serde_fields() { return std::tie(error_code); }

    friend bool operator==(const stop_vm_reply&, const stop_vm_reply&)
      = default;
};

} // namespace transform::worker::rpc
