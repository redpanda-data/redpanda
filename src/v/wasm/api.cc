/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "wasm/api.h"

#include "wasm/schema_registry.h"
#include "wasm/wasmedge.h"

namespace wasm {
std::unique_ptr<runtime>
runtime::create_default(pandaproxy::schema_registry::api* schema_reg) {
    return wasmedge::create_runtime(
      wasm::schema_registry::make_default(schema_reg));
}
} // namespace wasm
