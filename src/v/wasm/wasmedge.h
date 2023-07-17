/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "wasm/api.h"

#include <memory>

namespace wasm::wasmedge {

std::unique_ptr<runtime>
create_runtime(ssx::thread_worker*, std::unique_ptr<schema_registry>);

} // namespace wasm::wasmedge
