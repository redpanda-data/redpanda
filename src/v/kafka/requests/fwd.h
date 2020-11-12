/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"

#include <seastar/core/sharded.hh>

#include <memory>

namespace seastar {
class smp_service_group;
}

namespace kafka {
class request_context;
class response_writer;

class response;
using response_ptr = ss::foreign_ptr<std::unique_ptr<response>>;
} // namespace kafka
