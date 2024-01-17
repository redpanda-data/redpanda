/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "bytes/iobuf.h"

#include <boost/container_hash/hash.hpp>

namespace std {
template<>
struct hash<::iobuf> {
    size_t operator()(const ::iobuf& b) const {
        size_t h = 0;
        for (auto& f : b) {
            boost::hash_combine(
              h, std::hash<std::string_view>{}({f.get(), f.size()}));
        }
        return h;
    }
};
} // namespace std
