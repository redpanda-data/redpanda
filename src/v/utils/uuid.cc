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

uuid_t::operator ss::sstring() const { return fmt::format("{}", _uuid); }

bool operator<(const uuid_t& l, const uuid_t& r) { return l.uuid() < r.uuid(); }
