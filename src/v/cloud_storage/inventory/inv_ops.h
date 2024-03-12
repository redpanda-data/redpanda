/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/inventory/aws_ops.h"
#include "model/metadata.h"

namespace cloud_storage::inventory {

using ops_t = inv_ops_variant<aws_ops>;

/// \brief A wrapper for vendor specific inventory API calls.
class inv_ops {
public:
    explicit inv_ops(ops_t ops);

    ss::future<op_result<void>>
    create_inventory_configuration(cloud_storage_api&, retry_chain_node&);

    ss::future<op_result<bool>>
    inventory_configuration_exists(cloud_storage_api&, retry_chain_node&);

    ss::future<op_result<inventory_creation_result>>
    maybe_create_inventory_configuration(cloud_storage_api&, retry_chain_node&);

    ss::future<op_result<report_metadata>>
    latest_report_metadata(cloud_storage_api&, retry_chain_node&);

private:
    ops_t _inv_ops;
};

inv_ops make_inv_ops(
  model::cloud_storage_backend backend,
  cloud_storage_clients::bucket_name bucket,
  inventory_config_id inventory_id,
  ss::sstring inventory_prefix);

} // namespace cloud_storage::inventory
