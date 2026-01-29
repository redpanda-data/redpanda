// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "utils/uuid.h"

#include "bytes/details/out_of_range.h"

#include <seastar/core/sstring.hh>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/ostream.h>

#include <iostream>
#include <string>

uuid_t::uuid_t(const std::vector<uint8_t>& v) {
    if (v.size() != length) {
        details::throw_out_of_range(
          "Expected size of {} for UUID, got {}", length, v.size());
    }
    std::copy(v.begin(), v.end(), _uuid.begin());
}

uuid_t uuid_t::create() {
    static thread_local boost::uuids::random_generator uuid_gen;
    return uuid_t(uuid_gen());
}

uuid_t uuid_t::from_string(std::string_view str) {
    static thread_local ::boost::uuids::string_generator gen;

    return uuid_t(gen(str.begin(), str.end()));
}

std::ostream& operator<<(std::ostream& os, const uuid_t& u) {
    return os << fmt::format("{}", u._uuid);
}

std::istream& operator>>(std::istream& is, uuid_t& u) {
    std::string s;
    is >> s;
    try {
        u = uuid_t::from_string(s);
    } catch (const std::runtime_error&) {
        is.setstate(std::ios::failbit);
    }
    return is;
}

std::strong_ordering operator<=>(const uuid_t& u, const uuid_t& v) {
    return std::memcmp(u._uuid.begin(), v._uuid.begin(), uuid_t::length) <=> 0;
}

uuid_t::operator ss::sstring() const { return fmt::format("{}", _uuid); }

bool operator<(const uuid_t& l, const uuid_t& r) { return l.uuid() < r.uuid(); }

bool uuid_t::is_nil() const noexcept { return _uuid.is_nil(); }
uuid_t::operator bool() const noexcept { return !is_nil(); }
