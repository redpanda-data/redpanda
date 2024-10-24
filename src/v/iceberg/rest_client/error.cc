/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/rest_client/error.h"

namespace {
struct domain_error_printing_visitor {
    fmt::format_context* ctx;

    auto operator()(const http::url_build_error& err) const {
        return fmt::format_to(ctx->out(), "url_build_error: {}", err);
    }

    auto operator()(const iceberg::rest_client::json_parse_error& err) const {
        return fmt::format_to(
          ctx->out(),
          "json_parse_error: context: {}, error: {}",
          err.context,
          err.error);
    }

    auto operator()(const iceberg::rest_client::http_call_error& err) const {
        return fmt::format_to(ctx->out(), "{}", err);
    }

    auto operator()(const iceberg::rest_client::retries_exhausted& err) const {
        return fmt::format_to(
          ctx->out(), "retries_exhausted:[{}]", fmt::join(err.errors, ", "));
    }
};
} // namespace

auto fmt::formatter<iceberg::rest_client::http_call_error>::format(
  const iceberg::rest_client::http_call_error& err,
  fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return std::visit(
      [&ctx](const auto& http_call_error) {
          return fmt::format_to(
            ctx.out(), "http_call_error: {}", http_call_error);
      },
      err);
}

auto fmt::formatter<iceberg::rest_client::domain_error>::format(
  const iceberg::rest_client::domain_error& err,
  fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return std::visit(domain_error_printing_visitor{.ctx = &ctx}, err);
}
