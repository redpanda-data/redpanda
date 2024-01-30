/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "storage/batch_cache.h"
#include "storage/log_manager.h"

namespace storage::testing_details {
class log_manager_accessor {
public:
    static batch_cache& batch_cache(storage::log_manager& m) {
        return m._batch_cache;
    }
};
} // namespace storage::testing_details
