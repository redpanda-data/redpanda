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

ss::future<cloud_storage::upload_result>
inv_ops::create_inventory_configuration(
  cloud_storage_api& remote, retry_chain_node& parent) {
    return ss::visit(_inv_ops, [&remote, &parent](auto& ops) {
        return ops.create_inventory_configuration(
          remote, parent, frequency, format);
    });
}

} // namespace cloud_storage::inventory
