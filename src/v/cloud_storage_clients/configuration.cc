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

#include <seastar/net/tls.hh>

namespace {

ss::future<ss::tls::credentials_builder> make_tls_credentials_builder(
  std::optional<cloud_storage_clients::ca_trust_file> trust_file) {
    return net::get_credentials_builder({
      .truststore = trust_file.transform(
        [](auto& f) { return net::certificate(std::filesystem::path(f)); }),
      .k_store = std::nullopt,
      .crl = config::shard_local_cfg().cloud_storage_crl_file().transform(
        [](auto& f) { return net::certificate(std::filesystem::path(f)); }),
      .min_tls_version = from_config(
        config::shard_local_cfg().tls_min_version()),
      .enable_renegotiation = false,
      .require_client_auth = false,
    });
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
  model::cloud_credentials_source cloud_credentials_source,
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
    client_cfg.cloud_credentials_source = cloud_credentials_source;

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
    client_cfg.service = cloud_roles::aws_service_name{"s3"};
    // defer host creation to client, after it has performed self_configure to
    // discover if the backend is in `virtual_host` or `path mode`
    client_cfg.uri = access_point_uri(base_endpoint_uri);

    client_cfg.disable_tls = overrides.disable_tls;
    client_cfg.tls_truststore_path = overrides.trust_file;

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
    client_cfg.max_idle_time = overrides.max_idle_time
                                 ? *overrides.max_idle_time
                                 : default_max_idle_time;

    client_cfg.is_gcs = cloud_storage_clients::infer_backend_from_configuration(
                          client_cfg, client_cfg.cloud_credentials_source)
                        == model::cloud_storage_backend::google_s3_compat;

    co_return client_cfg;
}

ss::shared_ptr<client_probe> s3_configuration::make_probe() const {
    return ss::make_shared<client_probe>(
      disable_metrics,
      disable_public_metrics,
      region,
      endpoint_url{server_addr.host()});
}

std::ostream& operator<<(std::ostream& o, const s3_configuration& c) {
    o << "{access_key:"
      << c.access_key.value_or(cloud_roles::public_key_str{""})
      << ",region:" << c.region() << ",service:" << c.service()
      << ",secret_key:****"
      << ",url_style:" << c.url_style << ",access_point_uri:" << c.uri()
      << ",server_addr:" << c.server_addr << ",max_idle_time:"
      << std::chrono::duration_cast<std::chrono::milliseconds>(c.max_idle_time)
           .count()
      << "}";
    return o;
}

ss::future<abs_configuration> abs_configuration::make_configuration(
  model::cloud_credentials_source cloud_credentials_source,
  const std::optional<cloud_roles::private_key_str>& shared_key,
  const cloud_roles::storage_account& storage_account_name,
  const default_overrides& overrides,
  net::metrics_disabled disable_metrics,
  net::public_metrics_disabled disable_public_metrics) {
    abs_configuration client_cfg;
    client_cfg.cloud_credentials_source = cloud_credentials_source;

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

    client_cfg.disable_tls = overrides.disable_tls;
    client_cfg.tls_truststore_path = overrides.trust_file;

    client_cfg.server_addr = net::unresolved_address(
      client_cfg.uri(),
      overrides.port ? *overrides.port : default_port,
      ss::net::inet_address::family::INET);
    client_cfg.disable_metrics = disable_metrics;
    client_cfg.disable_public_metrics = disable_public_metrics;
    client_cfg.max_idle_time = overrides.max_idle_time
                                 ? *overrides.max_idle_time
                                 : default_max_idle_time;
    co_return client_cfg;
}

ss::shared_ptr<client_probe> abs_configuration::make_probe() const {
    return ss::make_shared<client_probe>(
      disable_metrics,
      disable_public_metrics,
      storage_account_name,
      endpoint_url{server_addr.host()});
}

void apply_self_configuration_result(
  client_configuration& cfg, const client_self_configuration_output& res) {
    ss::visit(
      cfg,
      [&res](s3_configuration& cfg) {
          vassert(
            std::holds_alternative<s3_self_configuration_result>(res),
            "Incompatible client configuration {} and self configuration "
            "result {}",
            cfg,
            res);

          cfg.url_style = std::get<s3_self_configuration_result>(res).url_style;
      },
      [&res](abs_configuration& cfg) {
          vassert(
            std::holds_alternative<abs_self_configuration_result>(res),
            "Incompatible client configuration {} and self configuration "
            "result {}",
            cfg,
            res);

          cfg.is_hns_enabled
            = std::get<abs_self_configuration_result>(res).is_hns_enabled;
      });
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
    ss::visit(
      r,
      [&o](const s3_self_configuration_result& self_cfg) {
          o << "{s3_self_configuration_result: " << self_cfg << "}";
      },
      [&o](const abs_self_configuration_result& self_cfg) {
          o << "{abs_self_configuration_result: " << self_cfg << "}";
      });

    return o;
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
          .match_expr(
            "linodeobjects\\.com",
            model::cloud_storage_backend::linode_s3_compat)
          .default_match(model::cloud_storage_backend::unknown);
    return result;
}

model::cloud_storage_backend infer_backend_from_configuration(
  const client_configuration& client_config,
  model::cloud_credentials_source cloud_storage_credentials_source) {
    if (
      auto v = config::shard_local_cfg().cloud_storage_backend.value();
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
    ss::visit(
      c,
      [&o](const s3_configuration& cfg) {
          o << "{s3_configuration: " << cfg << "}";
      },
      [&o](const abs_configuration& cfg) {
          o << "{abs_configuration: " << cfg << "}";
      });

    return o;
}

cloud_roles::auth_refresh_bg_op::credentials_source_config
build_refresh_credentials_source(
  const client_configuration& config,
  model::cloud_credentials_source cloud_credentials_source) {
    if (
      cloud_credentials_source
      == model::cloud_credentials_source::config_file) {
        return ss::visit(
          config,
          [](const cloud_storage_clients::s3_configuration& s3_cfg)
            -> cloud_roles::auth_refresh_bg_op::credentials_source_config {
              return cloud_roles::aws_credentials{
                .access_key_id = s3_cfg.access_key.value(),
                .secret_access_key = s3_cfg.secret_key.value(),
                .session_token = std::nullopt,
                .region = s3_cfg.region,
                .service = s3_cfg.service};
          },
          [](const cloud_storage_clients::abs_configuration& abs_cfg)
            -> cloud_roles::auth_refresh_bg_op::credentials_source_config {
              return cloud_roles::abs_credentials{
                .storage_account = abs_cfg.storage_account_name,
                .shared_key = abs_cfg.shared_key.value()};
          });
    } else {
        return ss::visit(
          config,
          [](const cloud_storage_clients::s3_configuration& s3_cfg)
            -> cloud_roles::auth_refresh_bg_op::credentials_source_config {
              return cloud_roles::auth_refresh_bg_op::s3_compat_config{
                .service = s3_cfg.service, .region = s3_cfg.region};
          },
          [](const cloud_storage_clients::abs_configuration&)
            -> cloud_roles::auth_refresh_bg_op::credentials_source_config {
              return cloud_roles::auth_refresh_bg_op::abs_config{};
          });
    }
}

namespace {
ss::future<ss::shared_ptr<ss::tls::certificate_credentials>>
build_tls_credentials(
  ss::sstring name, ss::tls::credentials_builder cred_builder) {
    co_return co_await net::build_reloadable_credentials_with_probe<
      ss::tls::certificate_credentials>(
      std::move(cred_builder), "cloud_storage_client", std::move(name));
}
} // namespace

ss::future<ss::shared_ptr<ss::tls::certificate_credentials>>
build_tls_credentials(const client_configuration& config) {
    using val_t = ss::shared_ptr<ss::tls::certificate_credentials>;

    return ss::visit(
      config,
      [](const s3_configuration& s3_cfg) {
          if (!s3_cfg.disable_tls) {
              return make_tls_credentials_builder(s3_cfg.tls_truststore_path)
                .then([](ss::tls::credentials_builder builder) {
                    return build_tls_credentials("s3", std::move(builder));
                });
          }
          return ss::make_ready_future<val_t>(nullptr);
      },
      [](const abs_configuration& abs_cfg) {
          if (!abs_cfg.disable_tls) {
              return make_tls_credentials_builder(abs_cfg.tls_truststore_path)
                .then([](ss::tls::credentials_builder builder) {
                    return build_tls_credentials("abs", std::move(builder));
                });
          }
          return ss::make_ready_future<val_t>(nullptr);
      });
}

net::base_transport::configuration build_transport_configuration(
  const client_configuration& config,
  ss::shared_ptr<ss::tls::certificate_credentials> tls_credentials) {
    return ss::visit(config, [&tls_credentials](const auto& cfg) {
        return net::base_transport::configuration{
          .server_addr = cfg.server_addr,
          .credentials = tls_credentials,
          .tls_sni_hostname = cfg.tls_sni_hostname,
          .wait_for_tls_server_eof = cfg.wait_for_tls_server_eof,
        };
    });
}

} // namespace cloud_storage_clients
