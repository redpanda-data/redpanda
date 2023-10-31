/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "outcome.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <iosfwd>

namespace security::oidc {

struct parsed_url {
    ss::sstring scheme;
    ss::sstring host;
    uint16_t port;
    ss::sstring target;
    friend bool operator==(parsed_url const&, parsed_url const&) = default;
    friend std::ostream& operator<<(std::ostream&, parsed_url const&);
};
result<parsed_url> parse_url(std::string_view);

} // namespace security::oidc
