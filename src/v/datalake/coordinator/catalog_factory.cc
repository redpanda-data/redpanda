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

net::unresolved_address endpoint_to_address(const ss::sstring& url_str) {
    auto url = ada::parse(url_str);
    if (!url) {
        throw std::invalid_argument(fmt::format(
          "Malformed Iceberg REST catalog endpoint url: {}", url_str));
    }
    // Default port as used by the Iceberg catalogs
    uint16_t port = 8181;
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

filesystem_catalog_factory::filesystem_catalog_factory(
  config::configuration& config,
  cloud_io::remote& remote,
  const cloud_storage_clients::bucket_name& bucket)
  : config_(&config)
  , remote_(&remote)
  , bucket_(std::move(bucket)) {}

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

    vlog(
      datalake_log.info,
      "Creating rest Iceberg catalog connected to ",
      config_->iceberg_rest_catalog_endpoint().value());

    std::unique_ptr<http::abstract_client> http_client
      = static_cast<std::unique_ptr<http::abstract_client>>(
        std::make_unique<http::client>(net::base_transport::configuration{
          .server_addr = endpoint_to_address(
            config_->iceberg_rest_catalog_endpoint().value())}));

    // TODO: support OAuth token here
    auto client = std::make_unique<iceberg::rest_client::catalog_client>(
      std::move(http_client),
      config_->iceberg_rest_catalog_endpoint().value(),
      // TODO: make credentials optional, we should provide either
      // credentials or token
      iceberg::rest_client::credentials{
        .client_id = config_->iceberg_rest_catalog_client_id().value(),
        .client_secret = config_->iceberg_rest_catalog_client_secret().value()},
      std::nullopt, // base_path
      std::nullopt, // prefix
      std::nullopt, // api_version
      std::nullopt  // token
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
