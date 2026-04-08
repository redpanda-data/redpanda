/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/upstream_registry.h"

#include "cloud_storage_clients/detail/registry_def.h" // IWYU pragma: keep
#include "cloud_storage_clients/logger.h"
#include "cloud_storage_clients/upstream_key.h"

#include <seastar/core/sharded.hh>

namespace cloud_storage_clients {

/// A reasonable upper limit on the number of upstreams.
constexpr size_t max_upstreams = 10;

constexpr std::string_view s3_region_key = "region";
constexpr std::string_view s3_endpoint_key = "endpoint";

upstream_key make_upstream_key(
  const client_configuration& cfg, const bucket_name_parts& bucket) {
    upstream_key key;

    if (bucket.params.empty()) {
        return key;
    }

    if (
      auto s3_cfg = std::get_if<s3_configuration>(&cfg);
      !s3_cfg || s3_cfg->is_gcs) {
        throw std::invalid_argument(
          "Cannot specify bucket params for non-S3 cloud storage "
          "configurations");
    } else {
        if (!s3_cfg->url_style.has_value()) {
            throw std::invalid_argument(
              "Cannot specify bucket params when cloud storage url style is "
              "not set explicitly");
        }

        for (const auto& [param_key, param_value] : bucket.params) {
            if (param_key == s3_region_key) {
                key.region = cloud_roles::aws_region_name{param_value};
            } else if (param_key == s3_endpoint_key) {
                key.endpoint = cloud_storage_clients::endpoint_url{param_value};

                auto host = *s3_cfg->url_style == s3_url_style::virtual_host
                              ? ssx::sformat("{}.{}", bucket.name, param_value)
                              : ss::sstring{param_value};
                key.server_addr = net::unresolved_address(
                  std::move(host),
                  s3_cfg->server_addr.port(),
                  s3_cfg->server_addr.family());
            } else {
                throw std::invalid_argument(
                  fmt::format("Unknown bucket param '{}'", param_key));
            }
        }
    }

    return key;
}

template class detail::
  basic_registry<upstream, upstream_key, upstream_registry>;

upstream_registry::upstream_registry(client_configuration config)
  : detail::basic_registry<upstream, upstream_key, upstream_registry>(
      pool_log, max_upstreams)
  , _config(std::move(config))
  , _probe(std::visit([](auto&& p) { return p.make_probe(); }, _config)) {}

ss::future<> upstream_registry::start() {
    _tls_credentials = co_await build_tls_credentials(_config);
}

ss::future<> upstream_registry::start_svc(
  sharded_constructor& ctor, const upstream_key& key) {
    client_configuration cfg = ss::visit(
      _config,
      [&key](s3_configuration s3_conf) -> client_configuration {
          if (!key.region().empty()) {
              s3_conf.region = key.region;
          }

          if (!key.endpoint().empty()) {
              s3_conf.tls_sni_hostname = key.endpoint();
              s3_conf.uri = access_point_uri{key.endpoint()};
              s3_conf.server_addr = *key.server_addr;
          }

          return s3_conf;
      },
      [](abs_configuration abs_conf) -> client_configuration {
          return abs_conf;
      });

    auto& svc = co_await ctor.start(
      key,
      cfg,
      ss::sharded_parameter(
        [this] { return container().local()._tls_credentials; }),
      ss::sharded_parameter([this] { return container().local().probe(); }));
    co_await svc.invoke_on_all(&upstream::start);
}

} // namespace cloud_storage_clients
