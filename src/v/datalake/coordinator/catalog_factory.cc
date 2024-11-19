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

#include <absl/strings/numbers.h>
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
      std::move(cred_builder), "rest_catalog_client", std::move(name));
};

} // namespace

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

net::unresolved_address endpoint_to_address(const ss::sstring& url_str) {
    auto url = ada::parse(url_str);
    if (!url) {
        throw std::invalid_argument(fmt::format(
          "Malformed Iceberg REST catalog endpoint url: {}", url_str));
    }
    // Default port as used by the Iceberg catalogs
    uint16_t port = 8181;
    if (url_str.contains("https")) {
        port = 443;
    }
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
    return {ss::sstring(url->get_hostname()), port};
}

} // namespace
ss::future<std::unique_ptr<iceberg::catalog>> create_catalog(
  cloud_io::remote& io,
  const cloud_storage_clients::bucket_name& bucket,
  config::configuration& cfg,
  ss::sstring metric_detail) {
    if (cfg.iceberg_catalog_type == config::datalake_catalog_type::filesystem) {
        vlog(
          datalake_log.info,
          "Creating filesystem catalog with bucket: {} and location: {}",
          bucket,
          cfg.iceberg_catalog_base_location());
        co_return std::make_unique<iceberg::filesystem_catalog>(
          io, bucket, cfg.iceberg_catalog_base_location());
    } else {
        // TODO: add config level validation
        throw_if_not_present(cfg.iceberg_rest_catalog_endpoint);
        throw_if_not_present(cfg.iceberg_rest_catalog_secret);
        throw_if_not_present(cfg.iceberg_rest_catalog_user_id);

        vlog(
          datalake_log.info,
          "Creating rest Iceberg catalog connected to ",
          cfg.iceberg_rest_catalog_endpoint().value());

        auto addr = endpoint_to_address(
          cfg.iceberg_rest_catalog_endpoint().value());
        std::unique_ptr<http::abstract_client> http_client
          = static_cast<std::unique_ptr<http::abstract_client>>(
            std::make_unique<http::client>(net::base_transport::configuration{
              .server_addr = endpoint_to_address(
                cfg.iceberg_rest_catalog_endpoint().value()),
              .credentials = co_await build_tls_credentials(
                std::move(metric_detail), std::nullopt, datalake_log),
              .tls_sni_hostname = addr.host(),
            }));

        // TODO: support OAuth token here
        auto client = std::make_unique<iceberg::rest_client::catalog_client>(
          std::move(http_client),
          cfg.iceberg_rest_catalog_endpoint().value(),
          // TODO: make credentials optional, we should provide either
          // credentials or token
          iceberg::rest_client::credentials{
            .client_id = cfg.iceberg_rest_catalog_user_id().value(),
            .client_secret = cfg.iceberg_rest_catalog_secret().value()},
          iceberg::rest_client::base_path{"polaris/api/catalog"}, // base_path
          iceberg::rest_client::prefix_path{"my_catalog"},        // prefix
          std::nullopt,                                           // api_version
          std::nullopt                                            // token
        );

        co_return std::make_unique<iceberg::rest_catalog>(
          std::move(client),
          cfg.iceberg_rest_catalog_request_timeout_ms.bind());
    }
}

ss::future<std::unique_ptr<iceberg::catalog>>
catalog_factory::make_catalog(ss::sstring metric_detail) const {
    co_return co_await create_catalog(_io, _bucket, _cfg, metric_detail);
}

} // namespace datalake::coordinator
