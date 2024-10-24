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

#include "base/seastarx.h"

#include <seastar/net/inet_address.hh>

namespace net {

class inet_address_wrapper final : public ss::net::inet_address {
public:
    inet_address_wrapper(ss::net::inet_address addr)
      : ss::net::inet_address(addr) {}

    template<typename H>
    friend H AbslHashValue(H h, const inet_address_wrapper& k) {
        return H::combine(std::move(h), std::hash<ss::net::inet_address>{}(k));
    }
};

} // namespace net
