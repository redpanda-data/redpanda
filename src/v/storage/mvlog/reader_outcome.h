// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

namespace storage::experimental::mvlog {

// Enum used by internal components of a reader to signal its status.
enum class reader_outcome {
    // The operation has completed.
    success,

    // The given entry should be skipped.
    skip,

    // The reader should be stopped because it is complete.
    stop,

    // The read buffer is full and should be emptied before proceeding.
    buffer_full,
};

} // namespace storage::experimental::mvlog
