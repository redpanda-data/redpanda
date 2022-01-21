// Copyright 2022 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "json/logger.h"
#include "vlog.h"

#include <fmt/format.h>
#include <rapidjson/allocators.h>
namespace json {
class throwing_allocator {
public:
    void* Malloc(size_t size) {
        void* res = _rp_allocator.Malloc(size);
        if (!res && (0 != size)) {
            vlog(json_log.error, "Could not allocate {} bytes", size);
            throw std::bad_alloc{};
        }
        return res;
    }

    void* Realloc(void* originalPtr, size_t originalSize, size_t newSize) {
        void* res = _rp_allocator.Realloc(originalPtr, originalSize, newSize);

        if (!res && (0 != newSize)) {
            vlog(
              json_log.error,
              "Could not reallocate memory: original size: {} bytes, new size: "
              "{} bytes",
              originalSize,
              newSize);
            throw std::bad_alloc{};
        }
        return res;
    }

    static void Free(void* ptr) { return rapidjson::CrtAllocator::Free(ptr); }

private:
    [[no_unique_address]] rapidjson::CrtAllocator _rp_allocator;
};
} // namespace json
