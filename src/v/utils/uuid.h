// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "bytes/details/out_of_range.h"

#include <absl/hash/hash.h>
#include <boost/functional/hash.hpp>
#include <boost/uuid/uuid.hpp>

#include <vector>

// Wrapper around Boost's UUID type suitable for serialization with serde.
// Provides utilities to construct and convert to other types.
//
// Expected usage is to supply a UUID v4 (i.e. based on random numbers). As
// such, serialization of this type simply serializes 16 bytes.
class uuid_t {
public:
    static constexpr auto length = 16;
    using underlying_t = boost::uuids::uuid;

    static uuid_t create();

    explicit uuid_t(const std::vector<uint8_t>& v)
      : _uuid({}) {
        if (v.size() != length) {
            details::throw_out_of_range(
              "Expected size of {} for UUID, got {}", length, v.size());
        }
        std::copy(v.begin(), v.end(), _uuid.begin());
    }

    uuid_t() noexcept = default;

    std::vector<uint8_t> to_vector() const {
        return {_uuid.begin(), _uuid.end()};
    }

    friend std::ostream& operator<<(std::ostream& os, const uuid_t& u);
    friend bool operator==(const uuid_t& u, const uuid_t& v) = default;

    template<typename H>
    friend H AbslHashValue(H h, const uuid_t& u) {
        for (const uint8_t byte : u._uuid) {
            H tmp = H::combine(std::move(h), byte);
            h = std::move(tmp);
        }
        return h;
    }

    const underlying_t& uuid() const { return _uuid; }

    underlying_t& mutable_uuid() { return _uuid; }

private:
    explicit uuid_t(const underlying_t& uuid)
      : _uuid(uuid) {}

    underlying_t _uuid;
};

namespace std {
template<>
struct hash<uuid_t> {
    size_t operator()(const uuid_t& u) const {
        return boost::hash<uuid_t::underlying_t>()(u.uuid());
    }
};
} // namespace std
