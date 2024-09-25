// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

#include <absl/hash/hash.h>
#include <boost/uuid/uuid.hpp>

#include <string>
#include <string_view>
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
    static_assert(underlying_t::static_size() == length);

    static uuid_t create();

    explicit uuid_t(const std::vector<uint8_t>& v);

    uuid_t() noexcept = default;

    std::vector<uint8_t> to_vector() const {
        return {_uuid.begin(), _uuid.end()};
    }

    friend std::ostream& operator<<(std::ostream& os, const uuid_t& u);
    friend std::istream& operator>>(std::istream& is, uuid_t& u);
    friend bool operator==(const uuid_t& u, const uuid_t& v) = default;

    operator ss::sstring() const;

    template<typename H>
    friend H AbslHashValue(H h, const uuid_t& u) {
        return H::combine_contiguous(
          std::move(h), u._uuid.begin(), underlying_t::static_size());
    }

    const underlying_t& uuid() const { return _uuid; }

    underlying_t& mutable_uuid() { return _uuid; }

    static uuid_t from_string(std::string_view);

private:
    explicit uuid_t(const underlying_t& uuid)
      : _uuid(uuid) {}

    underlying_t _uuid;
};

bool operator<(const uuid_t& l, const uuid_t& r);
