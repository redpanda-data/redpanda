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

#include "serde/envelope.h"

namespace transform::worker::rpc {

struct current_state_request
  : serde::envelope<
      current_state_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
};

struct current_state_reply
  : serde::envelope<
      current_state_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
};

struct manage_vm_request
  : serde::
      envelope<manage_vm_request, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
};

struct manage_vm_reply
  : serde::
      envelope<manage_vm_reply, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
};

} // namespace transform::worker::rpc
