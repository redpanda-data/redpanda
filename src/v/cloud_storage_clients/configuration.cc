/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/configuration.h"

#include "cloud_storage_clients/logger.h"
#include "config/configuration.h"
#include "net/tls.h"

namespace {

ss::future<ss::shared_ptr<ss::tls::certificate_credentials>>
build_tls_credentials(
  std::optional<cloud_storage_clients::ca_trust_file> trust_file,
  ss::logger& log) {
    ss::tls::credentials_builder cred_builder;
    // NOTE: this is a pre-defined gnutls priority string that
    // picks the ciphersuites with 128-bit ciphers which
    // leads to up to 10x improvement in upload speed, compared
    // to 256-bit ciphers
    cred_builder.set_priority_string("PERFORMANCE");
    if (trust_file.has_value()) {
        auto file = trust_file.value();
        vlog(log.info, "Use non-default trust file {}", file());
        co_await cred_builder.set_x509_trust_file(
          file().string(), ss::tls::x509_crt_format::PEM);
    } else {
        // Use GnuTLS defaults, might not work on all systems
        auto ca_file = co_await net::find_ca_file();
        if (ca_file) {
            vlog(
              log.info,
              "Use automatically discovered trust file {}",
              ca_file.value());
            co_await cred_builder.set_x509_trust_file(
              ca_file.value(), ss::tls::x509_crt_format::PEM);
        } else {
            vlog(
              log.info,
              "Trust file can't be detected automatically, using GnuTLS "
              "default");
            co_await cred_builder.set_system_trust();
        }
    }
    co_return co_await cred_builder.build_reloadable_certificate_credentials();
};

} // namespace

namespace cloud_storage_clients {

// Close all connections that were used more than 5 seconds ago.
// AWS S3 endpoint has timeout of 10 seconds. But since we're supporting
// not only AWS S3 it makes sense to set timeout value a bit lower.
static constexpr ss::lowres_clock::duration default_max_idle_time
  = std::chrono::seconds(5);

static constexpr uint16_t default_port = 443;

ss::future<s3_configuration> s3_configuration::make_configuration(
  const std::optional<cloud_roles::public_key_str>& pkey,
  const std::optional<cloud_roles::private_key_str>& skey,
  const cloud_roles::aws_region_name& region,
  const default_overrides& overrides,
  net::metrics_disabled disable_metrics,
  net::public_metrics_disabled disable_public_metrics) {
    s3_configuration client_cfg;
    const auto endpoint_uri = [&]() -> ss::sstring {
        if (overrides.endpoint) {
            return overrides.endpoint.value();
        }
        return ssx::sformat("s3.{}.amazonaws.com", region());
    }();
    client_cfg.tls_sni_hostname = endpoint_uri;

    // Setup credentials for TLS
    client_cfg.access_key = pkey;
    client_cfg.secret_key = skey;
    client_cfg.region = region;
    client_cfg.uri = access_point_uri(endpoint_uri);
    if (overrides.disable_tls == false) {
        client_cfg.credentials = co_await build_tls_credentials(
          overrides.trust_file, s3_log);
    }

    client_cfg.server_addr = net::unresolved_address(
      client_cfg.uri(),
      overrides.port ? *overrides.port : default_port,
      ss::net::inet_address::family::INET);
    client_cfg.disable_metrics = disable_metrics;
    client_cfg.disable_public_metrics = disable_public_metrics;
    client_cfg._probe = ss::make_shared<client_probe>(
      disable_metrics,
      disable_public_metrics,
      region,
      endpoint_url{endpoint_uri});
    client_cfg.max_idle_time = overrides.max_idle_time
                                 ? *overrides.max_idle_time
                                 : default_max_idle_time;
    co_return client_cfg;
}

std::ostream& operator<<(std::ostream& o, const s3_configuration& c) {
    o << "{access_key:"
      << c.access_key.value_or(cloud_roles::public_key_str{""})
      << ",region:" << c.region() << ",secret_key:****"
      << ",access_point_uri:" << c.uri() << ",server_addr:" << c.server_addr
      << ",max_idle_time:"
      << std::chrono::duration_cast<std::chrono::milliseconds>(c.max_idle_time)
           .count()
      << "}";
    return o;
}

ss::future<abs_configuration> abs_configuration::make_configuration(
  const std::optional<cloud_roles::private_key_str>& shared_key,
  const cloud_roles::storage_account& storage_account_name,
  const default_overrides& overrides,
  net::metrics_disabled disable_metrics,
  net::public_metrics_disabled disable_public_metrics) {
    abs_configuration client_cfg;

    const auto endpoint_uri = [&]() -> ss::sstring {
        if (overrides.endpoint) {
            return overrides.endpoint.value();
        }
        return ssx::sformat("{}.blob.core.windows.net", storage_account_name());
    }();

    client_cfg.tls_sni_hostname = endpoint_uri;
    client_cfg.storage_account_name = storage_account_name;
    client_cfg.shared_key = shared_key;
    client_cfg.uri = access_point_uri{endpoint_uri};
    if (overrides.disable_tls == false) {
        client_cfg.credentials = co_await build_tls_credentials(
          overrides.trust_file, abs_log);
    }

    client_cfg.server_addr = net::unresolved_address(
      client_cfg.uri(),
      overrides.port ? *overrides.port : default_port,
      ss::net::inet_address::family::INET);
    client_cfg.disable_metrics = disable_metrics;
    client_cfg.disable_public_metrics = disable_public_metrics;
    client_cfg._probe = ss::make_shared<client_probe>(
      disable_metrics,
      disable_public_metrics,
      storage_account_name,
      endpoint_url{endpoint_uri});
    client_cfg.max_idle_time = overrides.max_idle_time
                                 ? *overrides.max_idle_time
                                 : default_max_idle_time;
    co_return client_cfg;
}

std::ostream& operator<<(std::ostream& o, const abs_configuration& c) {
    o << "{storage_account_name: " << c.storage_account_name()
      << "shared_key:****"
      << ",access_point_uri:" << c.uri() << ",server_addr:" << c.server_addr
      << ",max_idle_time:"
      << std::chrono::duration_cast<std::chrono::milliseconds>(c.max_idle_time)
           .count()
      << "}";
    return o;
}

model::cloud_storage_backend infer_backend_from_configuration(
  const client_configuration& client_config,
  model::cloud_credentials_source cloud_storage_credentials_source) {
    if (auto v = config::shard_local_cfg().cloud_storage_backend.value();
        v != model::cloud_storage_backend::unknown) {
        vlog(
          client_config_log.info,
          "cloud_storage_backend is explicitly set to {}",
          v);
        return v;
    }

    if (std::holds_alternative<abs_configuration>(client_config)) {
        return model::cloud_storage_backend::azure;
    }

    switch (cloud_storage_credentials_source) {
    case model::cloud_credentials_source::aws_instance_metadata:
        [[fallthrough]];
    case model::cloud_credentials_source::sts:
        vlog(
          client_config_log.info,
          "cloud_storage_backend derived from cloud_credentials_source {} "
          "as aws",
          cloud_storage_credentials_source);
        return model::cloud_storage_backend::aws;
    case model::cloud_credentials_source::gcp_instance_metadata:
        vlog(
          client_config_log.info,
          "cloud_storage_backend derived from cloud_credentials_source {} "
          "as google_s3_compat",
          cloud_storage_credentials_source);
        return model::cloud_storage_backend::google_s3_compat;
    case model::cloud_credentials_source::config_file:
        break;
    }

    auto& s3_config = std::get<s3_configuration>(client_config);
    const auto& uri = s3_config.uri;

    auto result
      = string_switch<model::cloud_storage_backend>(uri())
          .match_expr("google", model::cloud_storage_backend::google_s3_compat)
          .match_expr(R"(127\.0\.0\.1)", model::cloud_storage_backend::aws)
          .match_expr("minio", model::cloud_storage_backend::minio)
          .match_expr("amazon", model::cloud_storage_backend::aws)
          .default_match(model::cloud_storage_backend::unknown);

    vlog(
      client_config_log.info,
      "Inferred backend {} using uri: {}",
      result,
      uri());

    return result;
}

} // namespace cloud_storage_clients
