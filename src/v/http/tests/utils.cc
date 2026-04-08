/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "http/tests/utils.h"

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>

namespace http {

namespace test_utils {

flexible_function_handler::flexible_function_handler(
  const flexible_handle_function& f_handle,
  ss::sstring content_type,
  std::set<ss::sstring> content_type_overrides)
  : _f_handle([this, f_handle](
                std::unique_ptr<ss::http::request> req,
                std::unique_ptr<ss::http::reply> rep) {
      rep->_content += f_handle(
        *req.get(), *rep.get(), std::ref(_content_type));
      return ss::make_ready_future<std::unique_ptr<ss::http::reply>>(
        std::move(rep));
  })
  , _content_type(std::move(content_type))
  , _content_type_overrides(std::move(content_type_overrides)) {}

ss::future<std::unique_ptr<ss::http::reply>> flexible_function_handler::handle(
  [[maybe_unused]] const ss::sstring& path,
  std::unique_ptr<ss::http::request> req,
  std::unique_ptr<ss::http::reply> rep) {
    return _f_handle(std::move(req), std::move(rep))
      .then([this](std::unique_ptr<ss::http::reply> rep) {
          if (
            _content_type_overrides.contains(rep->get_header("Content-Type"))) {
              rep->done();
          } else if (_content_type == "xml") {
              // Because `application/xml` is not implemented as a mapping
              // in `http/mime_types.cc`, in order to construct a reply with
              // the `Content-Type` header set to `application/xml`, we
              // need to hard code a path here.
              rep->set_content_type("application/xml");
              rep->done();
          } else {
              rep->done(_content_type);
          }
          return ss::make_ready_future<std::unique_ptr<ss::http::reply>>(
            std::move(rep));
      });
}

} // namespace test_utils

} // namespace http
