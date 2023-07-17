/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "wasm/api.h"

#include "wasm/schema_registry.h"
#include "wasm/wasmedge.h"

namespace wasm {
std::unique_ptr<runtime> runtime::create_default(
  ssx::thread_worker* t, pandaproxy::schema_registry::api* schema_reg) {
    return wasmedge::create_runtime(
      t, wasm::schema_registry::make_default(schema_reg));
}
} // namespace wasm
