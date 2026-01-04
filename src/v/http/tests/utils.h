/*
 * Copyright 2025 Redpanda Data, Inc.
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
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>

namespace http {

namespace test_utils {

// `ss::httpd::function_handler` calls `rep->done(_type)` on the reply before
// returning it. Because certain code paths in imposter services may want to
// return a different `Content-Type` in their response, this impl's `handle()`
// function allows for modifying the `type` set in the function handler's body
// before the final call to `done(type)` is performed.
//
// This class also handles an oversight in `mime_types.cc`, in which `xml` is
// not handled as a possible mime type.
class flexible_function_handler : public ss::httpd::handler_base {
    using flexible_handle_function = std::function<ss::sstring(
      const ss::http::request& req, ss::http::reply& rep, ss::sstring& type)>;

public:
    flexible_function_handler(
      const flexible_handle_function& f_handle,
      ss::sstring content_type = "txt",
      std::set<ss::sstring> content_type_overrides = {});

    ss::future<std::unique_ptr<ss::http::reply>> handle(
      [[maybe_unused]] const ss::sstring& path,
      std::unique_ptr<ss::http::request> req,
      std::unique_ptr<ss::http::reply> rep) override;

private:
    ss::httpd::future_handler_function _f_handle;
    ss::sstring _content_type;
    std::set<ss::sstring> _content_type_overrides;
};

} // namespace test_utils

} // namespace http
