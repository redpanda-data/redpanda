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

#include "pandaproxy/schema_registry/fwd.h"
#include "wasm/engine.h"

#include <memory>

namespace wasm {

std::unique_ptr<runtime>
create_default_runtime(pandaproxy::schema_registry::api* schema_reg);

} // namespace wasm
