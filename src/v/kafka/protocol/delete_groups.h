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
#include "kafka/protocol/errors.h"
#include "kafka/types.h"

#include <seastar/core/future.hh>

namespace kafka {

struct delete_groups_api final {
    static constexpr const char* name = "delete groups";
    static constexpr api_key key = api_key(42);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(0);
};

} // namespace kafka
