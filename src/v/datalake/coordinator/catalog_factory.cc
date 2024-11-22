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

#include "config/configuration.h"
#include "datalake/logger.h"
#include "iceberg/catalog.h"
#include "iceberg/filesystem_catalog.h"
#include "iceberg/rest_catalog.h"
#include "iceberg/rest_client/catalog_client.h"
#include "net/tls.h"
#include "net/tls_certificate_probe.h"
#include "thirdparty/ada/ada.h"

#include <absl/strings/numbers.h>
namespace datalake::coordinator {
namespace {
template<typename T>
void throw_if_not_present(const config::property<std::optional<T>>& property) {
    if (!property().has_value()) {
        throw std::runtime_error(ssx::sformat(
          "Configuration property {} value must be present when using REST "
          "Iceberg catalog",
          property.name()));
    }
}

ss::future<ss::shared_ptr<ss::tls::certificate_credentials>>
build_tls_credentials(config::configuration& cfg) {
    ss::tls::credentials_builder cred_builder;
    cred_builder.set_cipher_string(
      {config::tlsv1_2_cipher_string.data(),
       config::tlsv1_2_cipher_string.size()});
    cred_builder.set_ciphersuites(
      {config::tlsv1_3_ciphersuites.data(),
       config::tlsv1_3_ciphersuites.size()});
    cred_builder.set_minimum_tls_version(from_config(cfg.tls_min_version()));
    auto trust_file = cfg.iceberg_rest_catalog_trust_file();
    if (trust_file.has_value()) {
        auto file = trust_file.value();
        vlog(datalake_log.info, "Using non-default trust file {}", file);
        co_await cred_builder.set_x509_trust_file(
          file, ss::tls::x509_crt_format::PEM);
    } else {
        // Use system defaults, might not work on all systems
        auto ca_file = co_await net::find_ca_file();
        if (ca_file) {
            vlog(
              datalake_log.info,
              "Using automatically discovered trust file {}",
              ca_file.value());
            co_await cred_builder.set_x509_trust_file(
              ca_file.value(), ss::tls::x509_crt_format::PEM);
        } else {
            vlog(
              datalake_log.info,
              "Trust file can't be detected automatically, using system "
              "default");
            co_await cred_builder.set_system_trust();
        }
    }
    if (auto crl_file = cfg.iceberg_rest_catalog_crl_file();
        crl_file.has_value()) {
        co_await cred_builder.set_x509_crl_file(
          *crl_file, ss::tls::x509_crt_format::PEM);
    }
    co_return co_await cred_builder.build_reloadable_certificate_credentials();
};
struct endpoint_information {
    net::unresolved_address address;
    bool needs_tls;
    std::optional<iceberg::rest_client::base_path> base_path;
};

endpoint_information endpoint_to_address(const ss::sstring& url_str) {
    auto url = ada::parse(url_str);
    if (!url) {
        throw std::invalid_argument(fmt::format(
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
            throw std::invalid_argument(fmt::format(
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
filesystem_catalog_factory::create_catalog() {
    vlog(
      datalake_log.info,
      "Creating filesystem catalog with bucket: {} and location: {}",
      bucket_,
      config_->iceberg_catalog_base_location());
    co_return std::make_unique<iceberg::filesystem_catalog>(
      *remote_, bucket_, config_->iceberg_catalog_base_location());
}

rest_catalog_factory::rest_catalog_factory(config::configuration& config)
  : config_(&config) {}

ss::future<std::unique_ptr<iceberg::catalog>>
rest_catalog_factory::create_catalog() {
    // TODO: add config level validation
    throw_if_not_present(config_->iceberg_rest_catalog_endpoint);
    throw_if_not_present(config_->iceberg_rest_catalog_client_secret);
    throw_if_not_present(config_->iceberg_rest_catalog_client_id);

    auto endpoint_information = endpoint_to_address(
      config_->iceberg_rest_catalog_endpoint().value());

    net::base_transport::configuration transport_config{
      .server_addr = endpoint_information.address,
    };
    if (endpoint_information.needs_tls) {
        transport_config.credentials = co_await build_tls_credentials(*config_);
    }
    std::unique_ptr<http::abstract_client> http_client
      = static_cast<std::unique_ptr<http::abstract_client>>(
        std::make_unique<http::client>(std::move(transport_config)));
    auto prefix_path
      = config_->iceberg_rest_catalog_prefix()
          ? std::make_optional<iceberg::rest_client::prefix_path>(
              config_->iceberg_rest_catalog_prefix().value())
          : std::nullopt;
    auto token = config_->iceberg_rest_catalog_token()
                   ? std::make_optional<iceberg::rest_client::oauth_token>(
                       config_->iceberg_rest_catalog_token().value())
                   : std::nullopt;
    vlog(
      datalake_log.info,
      "Creating rest Iceberg catalog connected to: {}, with base path: {} and "
      "prefix: {}",
      endpoint_information.address,
      endpoint_information.base_path,
      prefix_path);
    // TODO: support OAuth token here
    auto client = std::make_unique<iceberg::rest_client::catalog_client>(
      std::move(http_client),
      config_->iceberg_rest_catalog_endpoint().value(),
      // TODO: make credentials optional, we should provide either
      // credentials or token
      iceberg::rest_client::credentials{
        .client_id = config_->iceberg_rest_catalog_client_id().value(),
        .client_secret = config_->iceberg_rest_catalog_client_secret().value()},
      std::move(endpoint_information.base_path), // base_path
      std::move(prefix_path),                    // prefix
      std::nullopt,                              // api_version
      std::move(token)                           // token
    );

    co_return std::make_unique<iceberg::rest_catalog>(
      std::move(client),
      config_->iceberg_rest_catalog_request_timeout_ms.bind());
}

std::unique_ptr<catalog_factory> get_catalog_factory(
  config::configuration& config,
  cloud_io::remote& remote,
  const cloud_storage_clients::bucket_name& bucket) {
    if (
      config.iceberg_catalog_type()
      == config::datalake_catalog_type::object_storage) {
        return std::make_unique<filesystem_catalog_factory>(
          config, remote, bucket);
    } else {
        return std::make_unique<rest_catalog_factory>(config);
    }
}
} // namespace datalake::coordinator
