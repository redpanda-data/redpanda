/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/inventory/inv_ops.h"

#include "cloud_storage/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/util/variant_utils.hh>

#include <utility>

namespace {
// TODO (abhijat) - cluster config
constexpr auto frequency
  = cloud_storage::inventory::report_generation_frequency::daily;
constexpr auto format = cloud_storage::inventory::report_format::csv;
} // namespace

namespace cloud_storage::inventory {

inv_ops::inv_ops(ops_t ops)
  : _inv_ops{std::move(ops)} {}

ss::future<op_result<void>> inv_ops::create_inventory_configuration(
  cloud_storage_api& remote, retry_chain_node& parent) {
    return ss::visit(_inv_ops, [&remote, &parent](auto& ops) {
        return ops.create_inventory_configuration(
          remote, parent, frequency, format);
    });
}

ss::future<op_result<bool>> inv_ops::inventory_configuration_exists(
  cloud_storage_api& remote, retry_chain_node& parent) {
    return ss::visit(_inv_ops, [&remote, &parent](auto& ops) {
        return ops.inventory_configuration_exists(remote, parent);
    });
}

ss::future<op_result<inventory_creation_result>>
inv_ops::maybe_create_inventory_configuration(
  cloud_storage_api& remote, retry_chain_node& parent) {
    if (const auto exists = co_await inventory_configuration_exists(
          remote, parent);
        exists.has_value() && exists.value()) {
        co_return inventory_creation_result::already_exists;
    }

    if (const auto create_res = co_await create_inventory_configuration(
          remote, parent);
        create_res.has_value()) {
        co_return inventory_creation_result::success;
    }

    if (const auto exists = co_await inventory_configuration_exists(
          remote, parent);
        exists.has_value() && exists.value()) {
        co_return inventory_creation_result::already_exists;
    }

    co_return error_outcome::create_inv_cfg_failed;
}

ss::future<op_result<report_metadata>> inv_ops::latest_report_metadata(
  cloud_storage_api& remote, retry_chain_node& parent) {
    return ss::visit(_inv_ops, [&remote, &parent](auto& ops) {
        return ops.latest_report_metadata(remote, parent);
    });
}

inv_ops make_inv_ops(
  model::cloud_storage_backend backend,
  cloud_storage_clients::bucket_name bucket,
  inventory_config_id inventory_id,
  ss::sstring inventory_prefix) {
    switch (backend) {
        using enum model::cloud_storage_backend;
    case aws:
        return inv_ops{aws_ops{
          std::move(bucket),
          std::move(inventory_id),
          std::move(inventory_prefix)}};
    case google_s3_compat:
    case azure:
    case minio:
    case unknown:
        throw std::invalid_argument{
          fmt::format("inventory API not supported for {}", backend)};
    }
}

} // namespace cloud_storage::inventory
