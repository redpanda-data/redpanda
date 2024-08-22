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

#include "bytes/bytes.h"

namespace storage {

/**
 * Type representing a record key prefixed with batch_type
 */
struct compaction_key : bytes {
    explicit compaction_key(bytes b)
      : bytes(std::move(b)) {}
};

} // namespace storage
