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
#include "hashing/xx.h"

#include <boost/container_hash/hash.hpp>

namespace std {
template<>
struct hash<::iobuf> {
    size_t operator()(const ::iobuf& b) const {
        incremental_xxhash64 h;
        for (const auto& f : b) {
            h.update(f.get(), f.size());
        }
        // mix once
        size_t seed = 0;
        boost::hash_combine(seed, h.digest());
        return seed;
    }
};
} // namespace std
