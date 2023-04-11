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

#include "seastarx.h"

#include <seastar/core/sstring.hh>
#include <seastar/http/request.hh>

namespace cloud_storage {

class bad_request : public std::invalid_argument {
public:
    explicit bad_request(ss::sstring msg)
      : std::invalid_argument(msg) {}
};

struct recovery_request {
public:
    explicit recovery_request(const ss::http::request&);

    std::optional<ss::sstring> topic_names_pattern() const;

    std::optional<size_t> retention_bytes() const;

    std::optional<std::chrono::milliseconds> retention_ms() const;

private:
    void parse_request_body(const ss::http::request&);

private:
    std::optional<ss::sstring> _topic_names_pattern;
    std::optional<size_t> _retention_bytes;
    std::optional<std::chrono::milliseconds> _retention_ms;
};

std::ostream& operator<<(std::ostream&, const recovery_request&);

} // namespace cloud_storage
