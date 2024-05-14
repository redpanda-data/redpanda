// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_set.h>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include <iostream>

namespace std {

std::ostream&
operator<<(std::ostream& os, const absl::flat_hash_set<ss::sstring>& d) {
    fmt::print(os, "{}", fmt::join(d, "\n"));
    return os;
}

} // namespace std
