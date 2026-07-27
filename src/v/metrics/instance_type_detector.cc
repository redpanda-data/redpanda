/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "metrics/instance_type_detector.h"

#include "base/seastarx.h"
#include "base/vlog.h"
#include "bytes/iobuf.h"
#include "cloud_instance_metadata/aws_imds.h"
#include "http/client.h"
#include "instance_type_detector_impl.h"
#include "metrics/instance_info_impl.h"
#include "net/transport.h"
#include "utils/unresolved_address.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/log.hh>

#include <boost/beast/http/status.hpp>

#include <chrono>
#include <cstdlib>

namespace instance_info {

namespace imds = cloud_instance_metadata::aws_imds;

namespace {

ss::logger ii_log("instance_info"); // NOLINT

// IMDSv2 token TTL. The token outlives this one-shot use trivially.
constexpr auto imds_token_ttl = std::chrono::seconds{60};

// Resolve a detected instance-type name (provider unknown) against the table,
// recovering the provider so it can be used as a metric label.
std::optional<detected_instance> resolve_by_name(ss::sstring instance_type) {
    for (const auto& e : instance_table()) {
        if (e.name == instance_type) {
            return detected_instance{
              .provider = e.provider,
              .instance_type = std::move(instance_type),
              .capacity = e.capacity};
        }
    }
    return std::nullopt;
}

} // namespace

ss::future<std::optional<ss::sstring>> query_ec2_instance_type(
  http::abstract_client& client, ss::lowres_clock::duration timeout) {
    // IMDSv2: PUT to obtain a session token, then GET the metadata with the
    // token in a header. We don't fall back to IMDSv1; modern EC2 supports v2.
    auto token_req = imds::token_request(imds::host, imds_token_ttl);

    auto token_resp = co_await client.request_and_collect_response(
      std::move(token_req), std::nullopt, timeout);
    if (token_resp.status != boost::beast::http::status::ok) {
        vlog(
          ii_log.debug,
          "IMDSv2 token request returned {}; not on EC2 or IMDS disabled",
          static_cast<int>(token_resp.status));
        co_return std::nullopt;
    }
    auto token = token_resp.body.linearize_to_string();

    auto meta_req = imds::get(
      imds::host, imds::instance_type_path, std::string_view{token});

    auto meta_resp = co_await client.request_and_collect_response(
      std::move(meta_req), std::nullopt, timeout);
    if (meta_resp.status != boost::beast::http::status::ok) {
        vlog(
          ii_log.debug,
          "IMDSv2 instance-type request returned {}",
          static_cast<int>(meta_resp.status));
        co_return std::nullopt;
    }
    co_return meta_resp.body.linearize_to_string();
}

ss::future<std::optional<detected_instance>>
detect_instance(ss::abort_source& as) {
    // 1. Environment override (testing / bare metal). Resolved by name across
    //    providers; the provider is recovered from the matching table entry.
    if (
      const char* env = std::getenv(instance_type_env_var);
      env != nullptr && *env != '\0') {
        auto resolved = resolve_by_name(ss::sstring{env});
        if (resolved) {
            vlog(
              ii_log.info,
              "using instance type {} from {} override",
              resolved->instance_type,
              instance_type_env_var);
        } else {
            vlog(
              ii_log.warn,
              "{} override {} is not a known instance type; ignoring",
              instance_type_env_var,
              env);
        }
        co_return resolved;
    }

    // 2. EC2 IMDSv2. Detection runs in the background, and the abort_source
    //    cancels any in-flight request promptly at shutdown, so we can afford a
    //    generous timeout to ride out a slow or briefly-unavailable IMDS.
    constexpr auto imds_timeout = std::chrono::seconds{10};
    const net::base_transport::configuration config{
      .server_addr
      = net::unresolved_address{ss::sstring{imds::host}, imds::port},
      .disable_metrics = net::metrics_disabled::yes,
      .disable_public_metrics = net::public_metrics_disabled::yes};

    http::client client{config, as};
    auto fut = co_await ss::coroutine::as_future(
      query_ec2_instance_type(client, imds_timeout));
    co_await client.stop();

    if (fut.failed()) {
        auto ex = fut.get_exception();
        vlog(ii_log.debug, "instance-type detection failed: {}", ex);
        co_return std::nullopt;
    }
    auto instance_type = fut.get();
    if (!instance_type) {
        co_return std::nullopt;
    }

    // A metadata response means we're on EC2, so the provider is aws even when
    // the specific type is absent from our table; in that case we still report
    // the detected instance but with no capacity figures.
    auto capacity = lookup(cloud_provider::aws, *instance_type);
    if (capacity) {
        vlog(ii_log.info, "detected aws instance type {}", *instance_type);
    } else {
        vlog(
          ii_log.warn,
          "detected aws instance type {} is not in the capacity table; its "
          "capacity metrics will be unavailable",
          *instance_type);
    }
    co_return detected_instance{
      .provider = cloud_provider::aws,
      .instance_type = std::move(*instance_type),
      .capacity = capacity};
}

} // namespace instance_info
