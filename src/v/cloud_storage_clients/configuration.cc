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
#include "config/tls_config.h"
#include "net/tls.h"
#include "net/tls_certificate_probe.h"
#include "utils/functional.h"

#include <seastar/net/tls.hh>

namespace {

ss::future<ss::shared_ptr<ss::tls::certificate_credentials>>
build_tls_credentials(
  ss::sstring name,
  std::optional<cloud_storage_clients::ca_trust_file> trust_file,
  ss::logger& log) {
    ss::tls::credentials_builder cred_builder;
    cred_builder.set_cipher_string(
      {config::tlsv1_2_cipher_string.data(),
       config::tlsv1_2_cipher_string.size()});
    cred_builder.set_ciphersuites(
      {config::tlsv1_3_ciphersuites.data(),
       config::tlsv1_3_ciphersuites.size()});
    cred_builder.set_minimum_tls_version(
      from_config(config::shard_local_cfg().tls_min_version()));
    if (trust_file.has_value()) {
        auto file = trust_file.value();
        vlog(log.info, "Use non-default trust file {}", file());
        co_await cred_builder.set_x509_trust_file(
          file().string(), ss::tls::x509_crt_format::PEM);
    } else {
        // Use system defaults, might not work on all systems
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
              "Trust file can't be detected automatically, using system "
              "default");
            co_await cred_builder.set_system_trust();
        }
    }
    if (auto crl_file
        = config::shard_local_cfg().cloud_storage_crl_file.value();
        crl_file.has_value()) {
        co_await cred_builder.set_x509_crl_file(
          *crl_file, ss::tls::x509_crt_format ::PEM);
    }
    co_return co_await net::build_reloadable_credentials_with_probe<
      ss::tls::certificate_credentials>(
      std::move(cred_builder), "cloud_storage_client", std::move(name));
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
  const bucket_name& bucket,
  std::optional<cloud_storage_clients::s3_url_style> url_style,
  bool node_is_in_fips_mode,
  const default_overrides& overrides,
  net::metrics_disabled disable_metrics,
  net::public_metrics_disabled disable_public_metrics) {
    s3_configuration client_cfg;

    if (url_style.has_value()) {
        vassert(
          !node_is_in_fips_mode
            || url_style.value() == s3_url_style::virtual_host,
          "node is in fips mode, but url_style is not set to virtual_host");
        client_cfg.url_style = url_style.value();
    } else {
        // If the url style in not specified, it will be determined with
        // self configuration.
        client_cfg.requires_self_configuration = true;
        // fips mode needs to build the endpoint in virtual host mode, so force
        // the value and attempt self_configuration to check that the TS service
        // can be reached in virtual_host mode
        if (node_is_in_fips_mode) {
            vlog(
              client_config_log.info,
              "in fips mode, url_style set to {}",
              s3_url_style::virtual_host);
            url_style = s3_url_style::virtual_host;
            client_cfg.url_style = s3_url_style::virtual_host;
        }
    }

    // if overrides.endpoint is not specified, build the default base endpoint.
    // for fips mode the it uses the `s3-fips` subdomain.
    const auto base_endpoint_uri = overrides.endpoint.value_or(
      endpoint_url{ssx::sformat(
        "{}.{}.amazonaws.com",
        node_is_in_fips_mode ? "s3-fips" : "s3",
        region())});

    // if url_style is virtual_host, the complete url for s3 is
    // [bucket].[s3hostname]. s3client will form the complete_endpoint
    // independently, to allow for self_configuration.
    const auto complete_endpoint_uri
      = url_style == s3_url_style::virtual_host
          ? ssx::sformat("{}.{}", bucket(), base_endpoint_uri())
          : base_endpoint_uri();

    client_cfg.tls_sni_hostname = complete_endpoint_uri;

    // Setup credentials for TLS
    client_cfg.access_key = pkey;
    client_cfg.secret_key = skey;
    client_cfg.region = region;
    // defer host creation to client, after it has performed self_configure to
    // discover if the backend is in `virtual_host` or `path mode`
    client_cfg.uri = access_point_uri(base_endpoint_uri);

    if (overrides.disable_tls == false) {
        client_cfg.credentials = co_await build_tls_credentials(
          "s3", overrides.trust_file, s3_log);
    }

    // When using virtual host addressing, the client must connect to
    // the s3 endpoint with the bucket name, e.g.
    // <bucket>.s3.<region>.amazonaws.com.  This is especially required
    // for S3 FIPS endpoints: <bucket>.s3-fips.<region>.amazonaws.com
    client_cfg.server_addr = net::unresolved_address(
      complete_endpoint_uri,
      overrides.port ? *overrides.port : default_port,
      ss::net::inet_address::family::INET);
    client_cfg.disable_metrics = disable_metrics;
    client_cfg.disable_public_metrics = disable_public_metrics;
    client_cfg._probe = ss::make_shared<client_probe>(
      disable_metrics,
      disable_public_metrics,
      region,
      endpoint_url{complete_endpoint_uri});
    client_cfg.max_idle_time = overrides.max_idle_time
                                 ? *overrides.max_idle_time
                                 : default_max_idle_time;
    co_return client_cfg;
}

std::ostream& operator<<(std::ostream& o, const s3_configuration& c) {
    o << "{access_key:"
      << c.access_key.value_or(cloud_roles::public_key_str{""})
      << ",region:" << c.region() << ",secret_key:****"
      << ",url_style:" << c.url_style << ",access_point_uri:" << c.uri()
      << ",server_addr:" << c.server_addr << ",max_idle_time:"
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

    client_cfg.requires_self_configuration = true;

    const auto endpoint_uri = [&]() -> ss::sstring {
        if (overrides.endpoint) {
            return overrides.endpoint.value();
        }
        return ssx::sformat("{}.blob.core.windows.net", storage_account_name());
    }();

    // The ABS TLS server misbehaves and does not send an EOF
    // when prompted to close the connection. Thus, skip the wait
    // in order to avoid Seastar's hardcoded 10s wait.
    client_cfg.wait_for_tls_server_eof = false;

    client_cfg.tls_sni_hostname = endpoint_uri;
    client_cfg.storage_account_name = storage_account_name;
    client_cfg.shared_key = shared_key;
    client_cfg.uri = access_point_uri{endpoint_uri};
    if (overrides.disable_tls == false) {
        client_cfg.credentials = co_await build_tls_credentials(
          "abs", overrides.trust_file, abs_log);
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

abs_configuration abs_configuration::make_adls_configuration() const {
    abs_configuration adls_config{*this};

    const auto endpoint_uri = [&]() -> ss::sstring {
        auto adls_endpoint_override
          = config::shard_local_cfg().cloud_storage_azure_adls_endpoint.value();
        if (adls_endpoint_override.has_value()) {
            return adls_endpoint_override.value();
        }
        return ssx::sformat("{}.dfs.core.windows.net", storage_account_name());
    }();

    adls_config.tls_sni_hostname = endpoint_uri;
    adls_config.uri = access_point_uri{endpoint_uri};

    auto adls_port_override
      = config::shard_local_cfg().cloud_storage_azure_adls_port();
    adls_config.server_addr = net::unresolved_address{
      endpoint_uri,
      adls_port_override.has_value() ? *adls_port_override : default_port};

    return adls_config;
}

void apply_self_configuration_result(
  client_configuration& cfg, const client_self_configuration_output& res) {
    std::visit(
      [&res](auto& cfg) -> void {
          using cfg_type = std::decay_t<decltype(cfg)>;
          if constexpr (std::is_same_v<s3_configuration, cfg_type>) {
              vassert(
                std::holds_alternative<s3_self_configuration_result>(res),
                "Incompatible client configuration {} and self configuration "
                "result {}",
                cfg,
                res);

              cfg.url_style
                = std::get<s3_self_configuration_result>(res).url_style;

          } else if constexpr (std::is_same_v<abs_configuration, cfg_type>) {
              vassert(
                std::holds_alternative<abs_self_configuration_result>(res),
                "Incompatible client configuration {} and self configuration "
                "result {}",
                cfg,
                res);

              cfg.is_hns_enabled
                = std::get<abs_self_configuration_result>(res).is_hns_enabled;
          } else {
              static_assert(always_false_v<cfg_type>, "Unknown client type");
          }
      },
      cfg);
}

std::ostream& operator<<(std::ostream& o, const abs_configuration& c) {
    o << "{storage_account_name: " << c.storage_account_name()
      << ", shared_key:" << (c.shared_key.has_value() ? "****" : "none")
      << ", access_point_uri:" << c.uri() << ", server_addr:" << c.server_addr
      << ", max_idle_time:"
      << std::chrono::duration_cast<std::chrono::milliseconds>(c.max_idle_time)
           .count()
      << ", is_hns_enabled:" << c.is_hns_enabled << "}";
    return o;
}

std::ostream&
operator<<(std::ostream& o, const abs_self_configuration_result& r) {
    o << "{is_hns_enabled: " << r.is_hns_enabled << "}";
    return o;
}

std::ostream&
operator<<(std::ostream& o, const s3_self_configuration_result& r) {
    o << "{s3_url_style: " << r.url_style << "}";
    return o;
}

std::ostream&
operator<<(std::ostream& o, const client_self_configuration_output& r) {
    return std::visit(
      [&o](const auto& self_cfg) -> std::ostream& {
          using cfg_type = std::decay_t<decltype(self_cfg)>;
          if constexpr (std::
                          is_same_v<s3_self_configuration_result, cfg_type>) {
              return o << "{s3_self_configuration_result: " << self_cfg << "}";
          } else if constexpr (std::is_same_v<
                                 abs_self_configuration_result,
                                 cfg_type>) {
              return o << "{abs_self_configuration_result: " << self_cfg << "}";
          } else {
              static_assert(always_false_v<cfg_type>, "Unknown client type");
          }
      },
      r);
}

model::cloud_storage_backend
infer_backend_from_uri(const access_point_uri& uri) {
    auto result
      = string_switch<model::cloud_storage_backend>(uri())
          .match_expr("google", model::cloud_storage_backend::google_s3_compat)
          .match_expr(R"(127\.0\.0\.1)", model::cloud_storage_backend::aws)
          .match_expr("localhost", model::cloud_storage_backend::aws)
          .match_expr("minio", model::cloud_storage_backend::minio)
          .match_expr("amazon", model::cloud_storage_backend::aws)
          .match_expr(
            "oraclecloud", model::cloud_storage_backend::oracle_s3_compat)
          .default_match(model::cloud_storage_backend::unknown);
    return result;
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
    case model::cloud_credentials_source::azure_aks_oidc_federation:
    case model::cloud_credentials_source::azure_vm_instance_metadata:
        vlog(
          client_config_log.info,
          "cloud_storage_backend derived from cloud_credentials_source {} "
          "as azure",
          cloud_storage_credentials_source);
        return model::cloud_storage_backend::azure;
    case model::cloud_credentials_source::config_file:
        break;
    }

    auto& s3_config = std::get<s3_configuration>(client_config);
    const auto& uri = s3_config.uri;
    auto result = infer_backend_from_uri(uri);

    vlog(
      client_config_log.info,
      "Inferred backend {} using uri: {}",
      result,
      uri());

    return result;
}

std::ostream& operator<<(std::ostream& o, const client_configuration& c) {
    return std::visit(
      [&o](const auto& cfg) -> std::ostream& {
          using cfg_type = std::decay_t<decltype(cfg)>;
          if constexpr (std::is_same_v<s3_configuration, cfg_type>) {
              return o << "{s3_configuration: " << cfg << "}";
          } else if constexpr (std::is_same_v<abs_configuration, cfg_type>) {
              return o << "{abs_configuration: " << cfg << "}";
          } else {
              static_assert(always_false_v<cfg_type>, "Unknown client type");
          }
      },
      c);
}

} // namespace cloud_storage_clients
