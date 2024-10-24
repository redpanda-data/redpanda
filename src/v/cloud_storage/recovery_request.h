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

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>
#include <seastar/http/request.hh>

namespace cloud_storage {

class bad_request : public std::invalid_argument {
public:
    explicit bad_request(const ss::sstring& msg)
      : std::invalid_argument(msg) {}
};

struct recovery_request {
public:
    static ss::future<recovery_request>
    parse_from_http(const ss::http::request&);

    static recovery_request parse_from_string(const ss::sstring&);

    std::optional<ss::sstring> topic_names_pattern() const;

    std::optional<size_t> retention_bytes() const;

    std::optional<std::chrono::milliseconds> retention_ms() const;

private:
    recovery_request() = default;

    void parse_request_body(const ss::sstring&);

    std::optional<ss::sstring> _topic_names_pattern;
    std::optional<size_t> _retention_bytes;
    std::optional<std::chrono::milliseconds> _retention_ms;
};

std::ostream& operator<<(std::ostream&, const recovery_request&);

} // namespace cloud_storage
