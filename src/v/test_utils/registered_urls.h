/*
 * Copyright 2022 Redpanda Data, Inc.
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
#include <seastar/http/httpd.hh>

#include <absl/container/flat_hash_map.h>

#include <iosfwd>

namespace http_test_utils {
struct response {
    ss::sstring body;
    ss::httpd::reply::status_type status;
};

std::ostream& operator<<(std::ostream& os, const response& resp);

struct registered_urls {
    using content_reply_map = absl::flat_hash_map<ss::sstring, response>;

    using method_reply_map
      = absl::flat_hash_map<ss::httpd::operation_type, content_reply_map>;

    using request_map = absl::flat_hash_map<ss::sstring, method_reply_map>;

    request_map request_response_map;

    struct add_mapping {
        ss::sstring url;
        ss::httpd::operation_type method;
        ss::sstring request_content;

        struct add_mapping_when {
            request_map& r;
            ss::sstring url;
            ss::httpd::operation_type method;
            ss::sstring request_content;

            void then_reply_with(ss::sstring content);
            void then_reply_with(ss::httpd::reply::status_type status);

            void then_reply_with(
              ss::sstring content, ss::httpd::reply::status_type status);

            add_mapping_when& with_method(ss::operation_type m);

            add_mapping_when& with_content(ss::sstring content);
        };
    };

    add_mapping::add_mapping_when request(
      ss::sstring url,
      ss::httpd::operation_type method,
      ss::sstring request_content);

    add_mapping::add_mapping_when
    request(ss::sstring url, ss::httpd::operation_type method);

    add_mapping::add_mapping_when request(ss::sstring url);

    response lookup(ss::httpd::const_req& req) const;
};
} // namespace http_test_utils
