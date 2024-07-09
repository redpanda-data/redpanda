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

#include "cloud_storage/inventory/types.h"
#include "cloud_storage/logger.h"
#include "utils/retry_chain_node.h"

#include <seastar/coroutine/all.hh>
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
    auto exists = co_await inventory_configuration_exists(remote, parent);
    if (exists.has_error()) {
        vlog(
          cst_log.error,
          "Failed to check if inventory exists before creation attempt");
        co_return exists.error();
    }

    if (exists.value()) {
        co_return inventory_creation_result::already_exists;
    }

    if (const auto create_res = co_await create_inventory_configuration(
          remote, parent);
        create_res.has_value()) {
        co_return inventory_creation_result::success;
    }

    vlog(
      cst_log.info,
      "Failed to create inventory, checking if it was created by another node");

    // At this point we have tried to create the inventory config and failed.
    // One possibility is that another node created the config and we lost the
    // race.
    exists = co_await inventory_configuration_exists(remote, parent);

    if (exists.has_value()) {
        if (exists.value()) {
            vlog(cst_log.debug, "Inventory created by another node");
            co_return inventory_creation_result::already_exists;
        }

        vlog(
          cst_log.warn,
          "Inventory does not exist after failed creation attempt");
    }

    if (exists.has_error()) {
        vlog(
          cst_log.warn,
          "Failed to check inventory status after failed creation attempt");
    }
    co_return error_outcome::create_inv_cfg_failed;
}

ss::future<op_result<report_metadata>> inv_ops::fetch_latest_report_metadata(
  cloud_storage_api& remote, retry_chain_node& parent) {
    return ss::visit(_inv_ops, [&remote, &parent](auto& ops) {
        return ops.fetch_latest_report_metadata(remote, parent);
    });
}

cloud_storage_clients::bucket_name inv_ops::bucket() const {
    return ss::visit(_inv_ops, [](const auto& ops) { return ops.bucket(); });
}

ss::future<inv_ops> make_inv_ops(
  cloud_storage_clients::bucket_name bucket,
  inventory_config_id inv_cfg_id,
  ss::sstring inv_reports_prefix) {
    if (inv_cfg_id().empty() || inv_reports_prefix.empty()) {
        throw std::runtime_error{fmt::format(
          "empty inventory id or report destination prefix: id: {}, prefix: "
          "{}",
          inv_cfg_id(),
          inv_reports_prefix)};
    }
    co_return inv_ops{aws_ops{bucket, inv_cfg_id, inv_reports_prefix}};
}

} // namespace cloud_storage::inventory
