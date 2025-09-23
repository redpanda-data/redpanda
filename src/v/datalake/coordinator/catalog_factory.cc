/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/coordinator/catalog_factory.h"

#include "absl/strings/numbers.h"
#include "config/configuration.h"
#include "config/types.h"
#include "datalake/credential_manager.h"
#include "datalake/logger.h"
#include "iceberg/catalog.h"
#include "iceberg/filesystem_catalog.h"
#include "iceberg/rest_catalog.h"
#include "iceberg/rest_client/catalog_client.h"
#include "iceberg/rest_client/client_probe.h"
#include "net/tls.h"
#include "net/tls_certificate_probe.h"

#include <ada.h>
namespace datalake::coordinator {
namespace {
template<typename T>
void throw_if_not_present(const config::property<std::optional<T>>& property) {
    if (!property().has_value()) {
        throw std::runtime_error(
          ssx::sformat(
            "Configuration property {} value must be present when using REST "
            "Iceberg catalog",
            property.name()));
    }
}

std::optional<net::certificate> get_truststore(config::configuration& cfg) {
    if (cfg.iceberg_rest_catalog_trust().has_value()) {
        return net::certificate(cfg.iceberg_rest_catalog_trust().value());
    }
    if (cfg.iceberg_rest_catalog_trust_file().has_value()) {
        return net::certificate(
          std::filesystem::path(cfg.iceberg_rest_catalog_trust_file().value()));
    }
    return std::nullopt;
}

std::optional<net::certificate> get_crl(config::configuration& cfg) {
    if (cfg.iceberg_rest_catalog_crl().has_value()) {
        return net::certificate(cfg.iceberg_rest_catalog_crl().value());
    }
    if (cfg.iceberg_rest_catalog_crl_file().has_value()) {
        return net::certificate(
          std::filesystem::path(cfg.iceberg_rest_catalog_crl_file().value()));
    }
    return std::nullopt;
}

ss::future<ss::shared_ptr<ss::tls::certificate_credentials>>
build_tls_credentials(config::configuration& cfg) {
    auto creds_builder = co_await net::get_credentials_builder({
      .truststore = get_truststore(cfg),
      .k_store = std::nullopt,
      .crl = get_crl(cfg),
      .min_tls_version = from_config(cfg.tls_min_version()),
      .enable_renegotiation = false,
      .require_client_auth = false,
    });

    co_return co_await creds_builder.build_reloadable_certificate_credentials();
};
struct endpoint_information {
    net::unresolved_address address;
    bool needs_tls;
    std::optional<iceberg::rest_client::base_path> base_path;
};

endpoint_information endpoint_to_address(const ss::sstring& url_str) {
    auto url = ada::parse(url_str);
    if (!url) {
        throw std::invalid_argument(
          fmt::format(
            "Malformed Iceberg REST catalog endpoint url: {}", url_str));
    }
    // Default port as used by the Iceberg catalogs
    uint16_t port = url->type == ada::scheme::HTTPS ? 443 : 8181;
    if (url->has_port()) {
        int32_t port_from_uri{0};
        auto parsed = absl::SimpleAtoi(url->get_port(), &port_from_uri);
        if (
          !parsed || port_from_uri < 0
          || port_from_uri > std::numeric_limits<uint16_t>::max()) {
            throw std::invalid_argument(
              fmt::format(
                "Malformed Iceberg REST catalog endpoint url: {}, unable to "
                "parse port",
                url_str));
        }
        port = static_cast<uint16_t>(port_from_uri);
    }
    std::optional<iceberg::rest_client::base_path> path
      = url->get_pathname() != ""
          ? std::make_optional<iceberg::rest_client::base_path>(
              url->get_pathname())
          : std::nullopt;

    return {
      .address
      = net::unresolved_address{ss::sstring(url->get_hostname()), port},
      .needs_tls = url->type == ada::scheme::HTTPS,
      .base_path = std::move(path)};
}

} // namespace

filesystem_catalog_factory::filesystem_catalog_factory(
  config::configuration& config,
  cloud_io::remote& remote,
  const cloud_storage_clients::bucket_name& bucket)
  : config_(&config)
  , remote_(&remote)
  , bucket_(bucket) {}

ss::future<std::unique_ptr<iceberg::catalog>>
filesystem_catalog_factory::create_catalog(ss::abort_source&) {
    vlog(
      datalake_log.info,
      "Creating filesystem catalog with bucket: {} and location: {}",
      bucket_,
      config_->iceberg_catalog_base_location());
    co_return std::make_unique<iceberg::filesystem_catalog>(
      *remote_, bucket_, config_->iceberg_catalog_base_location());
}

rest_catalog_factory::~rest_catalog_factory() {}

rest_catalog_factory::rest_catalog_factory(
  config::configuration& config,
  ss::metrics::label_instance label,
  datalake::credential_manager& cred_mgr)
  : config_(&config)
  , client_probe_(
      ss::make_shared<iceberg::rest_client::client_probe>(
        net::public_metrics_disabled(config.disable_public_metrics()),
        std::move(label)))
  , credential_manager_(cred_mgr) {}

rest_catalog_factory::credentials_and_token
rest_catalog_factory::make_credentials_or_token() {
    auto creds_and_token = credentials_and_token{};
    const auto auth_mode = config_->iceberg_rest_catalog_authentication_mode();
    switch (auth_mode) {
    case config::datalake_catalog_auth_mode::none:
        break;
    case config::datalake_catalog_auth_mode::bearer: {
        throw_if_not_present(config_->iceberg_rest_catalog_token);
        creds_and_token.token
          = std::make_optional<iceberg::rest_client::oauth_token>(
            config_->iceberg_rest_catalog_token().value());
        break;
    }
    case config::datalake_catalog_auth_mode::oauth2: {
        throw_if_not_present(config_->iceberg_rest_catalog_client_id);
        throw_if_not_present(config_->iceberg_rest_catalog_client_secret);
        creds_and_token.credentials
          = std::make_optional<iceberg::rest_client::credentials>(
            config_->iceberg_rest_catalog_client_id().value(),
            config_->iceberg_rest_catalog_client_secret().value(),
            config_->iceberg_rest_catalog_oauth2_server_uri(),
            config_->iceberg_rest_catalog_oauth2_scope());
        break;
    }
    case config::datalake_catalog_auth_mode::aws_sigv4: {
        // SigV4 credentials are handled by the applier and
        // background refresh op.
        break;
    }
    case config::datalake_catalog_auth_mode::gcp: {
        // GCP credentials are handled by the applier and background refresh op.
        break;
    }
    }
    return creds_and_token;
}

ss::future<std::unique_ptr<iceberg::catalog>>
rest_catalog_factory::create_catalog(ss::abort_source& as) {
    // TODO: add config level validation
    throw_if_not_present(config_->iceberg_rest_catalog_endpoint);

    auto endpoint_information = endpoint_to_address(
      config_->iceberg_rest_catalog_endpoint().value());

    net::base_transport::configuration transport_config{
      .server_addr = endpoint_information.address,
    };
    if (endpoint_information.needs_tls) {
        transport_config.tls_sni_hostname = endpoint_information.address.host();
        try {
            transport_config.credentials = co_await build_tls_credentials(
              *config_);
        } catch (...) {
            vlog(
              datalake_log.error,
              "Failed to create TLS credentials for Iceberg REST catalog: {}",
              std::current_exception());
            throw;
        }
    }
    auto http_client = std::make_unique<http::client>(
      std::move(transport_config), &as, client_probe_);

    auto creds_and_token = make_credentials_or_token();

    auto warehouse = config_->iceberg_rest_catalog_warehouse()
                       ? std::make_optional<iceberg::rest_client::warehouse>(
                           *config_->iceberg_rest_catalog_warehouse())
                       : std::nullopt;
    vlog(
      datalake_log.info,
      "Creating rest Iceberg catalog connected to: {}, with base path: {}, "
      "warehouse: {}",
      endpoint_information.address,
      endpoint_information.base_path,
      warehouse);
    auto client = std::make_unique<iceberg::rest_client::catalog_client>(
      std::move(http_client),
      config_->iceberg_rest_catalog_endpoint().value(),
      credential_manager_,
      std::move(creds_and_token.credentials),
      std::move(endpoint_information.base_path),           // base_path
      std::move(warehouse),                                // warehouse
      std::nullopt,                                        // api_version
      std::move(creds_and_token.token),                    // token
      nullptr,                                             // retry_policy
      config_->iceberg_rest_catalog_authentication_mode(), // auth_mode
      client_probe_);                                      // probe

    co_return std::make_unique<iceberg::rest_catalog>(
      std::move(client),
      config_->iceberg_rest_catalog_request_timeout_ms.bind(),
      config_->iceberg_rest_catalog_base_location());
}

std::unique_ptr<catalog_factory> get_catalog_factory(
  config::configuration& config,
  cloud_io::remote& remote,
  const cloud_storage_clients::bucket_name& bucket,
  ss::metrics::label_instance label,
  datalake::credential_manager& cred_mgr) {
    if (
      config.iceberg_catalog_type()
      == config::datalake_catalog_type::object_storage) {
        return std::make_unique<filesystem_catalog_factory>(
          config, remote, bucket);
    } else {
        return std::make_unique<rest_catalog_factory>(
          config, std::move(label), cred_mgr);
    }
}

} // namespace datalake::coordinator
