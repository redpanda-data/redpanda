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

namespace cloud_storage::inventory {

using ops_t = inv_ops_variant<aws_ops>;

/// \brief A wrapper for vendor specific inventory API calls.
class inv_ops {
public:
    explicit inv_ops(ops_t ops);

    ss::future<cloud_storage::upload_result>
    create_inventory_configuration(cloud_storage_api&, retry_chain_node&);

    ss::future<bool>
    inventory_configuration_exists(cloud_storage_api&, retry_chain_node&);

    ss::future<inventory_creation_result>
    maybe_create_inventory_configuration(cloud_storage_api&, retry_chain_node&);

private:
    ops_t _inv_ops;
};

} // namespace cloud_storage::inventory
