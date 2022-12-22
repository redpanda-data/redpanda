/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/client_probe.h"

#include "prometheus/prometheus_sanitize.h"
#include "ssx/metrics.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>

namespace {
constexpr auto endpoint_label_key = "endpoint";
constexpr auto region_label_key = "region";
constexpr auto storage_account_label_key = "storage_account";
} // namespace

namespace cloud_storage_clients {

client_probe::client_probe(
  net::metrics_disabled disable,
  net::public_metrics_disabled public_disable,
  cloud_roles::aws_region_name region,
  endpoint_url endpoint)
  : http::client_probe()
  , _total_rpc_errors(0)
  , _total_slowdowns(0)
  , _total_nosuchkeys(0) {
    namespace sm = ss::metrics;

    std::vector<raw_label> s3_labels = {
      {endpoint_label_key, std::move(endpoint)()},
      {region_label_key, std::move(region)()}};

    setup_internal_metrics(disable, s3_labels);
    setup_public_metrics(public_disable, s3_labels);
}

client_probe::client_probe(
  net::metrics_disabled disable,
  net::public_metrics_disabled public_disable,
  cloud_roles::storage_account storage_account_name,
  endpoint_url endpoint)
  : http::client_probe()
  , _total_rpc_errors(0)
  , _total_slowdowns(0)
  , _total_nosuchkeys(0) {
    std::vector<raw_label> abs_labels = {
      {endpoint_label_key, std::move(endpoint)()},
      {storage_account_label_key, std::move(storage_account_name)()}};

    setup_internal_metrics(disable, abs_labels);
    setup_public_metrics(public_disable, abs_labels);
}

void client_probe::register_failure(s3_error_code err) {
    if (err == s3_error_code::slow_down) {
        _total_slowdowns += 1;
    } else if (err == s3_error_code::no_such_key) {
        _total_nosuchkeys += 1;
    }
    _total_rpc_errors += 1;
}

void client_probe::register_failure(abs_error_code err) {
    if (err == abs_error_code::blob_not_found) {
        _total_nosuchkeys += 1;
    }
    _total_rpc_errors += 1;
}

void client_probe::setup_internal_metrics(
  net::metrics_disabled disable, std::span<raw_label> raw_labels) {
    namespace sm = ss::metrics;
    if (disable) {
        return;
    }

    std::vector<sm::label_instance> labels;
    labels.reserve(raw_labels.size());
    std::transform(
      raw_labels.begin(),
      raw_labels.end(),
      std::back_inserter(labels),
      [](const raw_label& rl) { return sm::label(rl.key)(rl.value); });

    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_client"),
      {
        sm::make_counter(
          "total_uploads",
          [this] { return get_total_put_requests(); },
          sm::description("Number of completed PUT requests"),
          labels),
        sm::make_counter(
          "total_downloads",
          [this] { return get_total_get_requests(); },
          sm::description("Number of completed GET requests"),
          labels),
        sm::make_counter(
          "all_requests",
          [this] { return get_total_requests(); },
          sm::description(
            "Number of completed HTTP requests (includes PUT and GET)"),
          labels),
        sm::make_gauge(
          "active_uploads",
          [this] { return get_active_put_requests(); },
          sm::description("Number of active PUT requests at the moment"),
          labels),
        sm::make_gauge(
          "active_downloads",
          [this] { return get_active_get_requests(); },
          sm::description("Number of active GET requests at the moment"),
          labels),
        sm::make_gauge(
          "active_requests",
          [this] { return get_active_requests(); },
          sm::description("Number of active HTTP requests at the moment "
                          "(includes PUT and GET)"),
          labels),
        sm::make_counter(
          "total_inbound_bytes",
          [this] { return get_inbound_bytes(); },
          sm::description("Total number of bytes received from cloud storage"),
          labels),
        sm::make_counter(
          "total_outbound_bytes",
          [this] { return get_outbound_bytes(); },
          sm::description("Total number of bytes sent to cloud storage"),
          labels),
        sm::make_counter(
          "num_rpc_errors",
          [this] { return _total_rpc_errors; },
          sm::description("Total number of REST API errors received from "
                          "cloud storage provider"),
          labels),
        sm::make_counter(
          "num_transport_errors",
          [this] { return get_transport_errors(); },
          sm::description("Total number of transport errors (TCP and TLS)"),
          labels),
        sm::make_counter(
          "num_slowdowns",
          [this] { return _total_slowdowns; },
          sm::description("Total number of SlowDown errors received from cloud "
                          "storage provider"),
          labels),
        sm::make_counter(
          "num_nosuchkey",
          [this] { return _total_nosuchkeys; },
          sm::description(
            "Total number of NoSuchKey errors received from cloud "
            "storage provider"),
          labels),
      });
}

void client_probe::setup_public_metrics(
  net::public_metrics_disabled disable, std::span<raw_label> raw_labels) {
    namespace sm = ss::metrics;
    if (disable) {
        return;
    }

    std::vector<sm::label_instance> labels;
    labels.reserve(raw_labels.size());
    std::transform(
      raw_labels.begin(),
      raw_labels.end(),
      std::back_inserter(labels),
      [](const raw_label& rl) {
          return sm::label(ssx::metrics::make_namespaced_label(rl.key))(
            rl.value);
      });

    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_client"),
      {
        sm::make_counter(
          "not_found",
          [this] { return _total_nosuchkeys; },
          sm::description(
            "Total number of requests for which the object was not found"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "throttled",
          [this] { return _total_slowdowns; },
          sm::description(
            "Total number of requests throttled by the cloud storage provider"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "uploads",
          [this] { return get_total_put_requests(); },
          sm::description("Total number of requests that uploaded an object to "
                          "cloud storage"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "downloads",
          [this] { return get_total_get_requests(); },
          sm::description("Total number of requests that downloaded an object "
                          "from cloud storage"),
          labels)
          .aggregate({sm::shard_label}),
      });
}

} // namespace cloud_storage_clients
