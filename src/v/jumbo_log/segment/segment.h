// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <cstddef>
#include <stdint.h>
#include <string_view>

namespace jumbo_log::segment {

struct chunk_loc {
    size_t offset_bytes;
    size_t size_bytes;
};

using chunk_size_t = uint32_t;              // 4 bytes
using chunk_data_batches_size_t = uint32_t; // 4 bytes

static constexpr std::string_view RP2_MAGIC_SIGNATURE = "RP2";
static constexpr size_t FOOTER_EPILOGUE_SIZE_BYTES = 11;

} // namespace jumbo_log::segment
