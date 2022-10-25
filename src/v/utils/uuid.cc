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

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/ostream.h>

#include <ostream>

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

std::ostream& operator<<(std::ostream& os, const uuid_t& u) {
    return os << fmt::format("{}", u._uuid);
}

uuid_t::operator ss::sstring() const { return fmt::format("{}", _uuid); }
