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

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>
#include <seastar/http/httpd.hh>

#include <absl/container/flat_hash_map.h>
#include <boost/beast/http/field.hpp>

#include <iosfwd>

namespace http_test_utils {
struct response {
    using status_type = ss::http::reply::status_type;
    ss::sstring body;
    std::vector<std::pair<ss::sstring, ss::sstring>> headers;
    status_type status;
};

struct request_info {
    // common request state
    ss::sstring method;
    ss::sstring url;
    ss::sstring content;
    size_t content_length;

    absl::flat_hash_map<ss::sstring, ss::sstring> headers;

    /*
     * an ugly hack for cloud_storage remote_test.cc. these are cherry-picked
     * out of the request for convenience rather than copying over the entire
     * header and query parameter data structures and search routines for
     * those containers. if more request info was needed it might be worth
     * copying over all the state and the accessors.
     */
    ss::sstring q_list_type;
    ss::sstring q_prefix;
    bool has_q_delete;

    explicit request_info(const ss::http::request& req)
      : method(req._method)
      , url(req._url)
      , content(req.content)
      , content_length(req.content_length) {
        q_list_type = req.get_query_param("list-type");
        q_prefix = req.get_query_param("prefix");
        has_q_delete = req.query_parameters.contains("delete");
    }

    std::optional<ss::sstring> header(const ss::sstring& key) const {
        if (!headers.contains(key)) {
            return std::nullopt;
        }
        return headers.at(key);
    }
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
            void then_reply_with(ss::http::reply::status_type status);

            void then_reply_with(
              ss::sstring content, ss::http::reply::status_type status);

            void then_reply_with(
              std::vector<std::pair<ss::sstring, ss::sstring>> headers,
              ss::http::reply::status_type status);

            add_mapping_when& with_method(ss::httpd::operation_type m);

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

    response lookup(const request_info&) const;
};
} // namespace http_test_utils
