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
#pragma once

#include "base/seastarx.h"
#include "wasm/fwd.h"

#include <seastar/core/future.hh>

namespace schema {
class registry;
} // namespace schema

namespace wasm::wasmtime {
std::unique_ptr<runtime> create_runtime(std::unique_ptr<schema::registry>);
}
