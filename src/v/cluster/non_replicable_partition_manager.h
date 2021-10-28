/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/non_replicable_partition.h"
#include "cluster/partition.h"
#include "storage/fwd.h"

#include <seastar/core/gate.hh>

#include <absl/container/flat_hash_map.h>

namespace cluster {

class non_replicable_partition_manager {
public:
    using ntp_table_container = absl::
      flat_hash_map<model::ntp, ss::lw_shared_ptr<non_replicable_partition>>;

    explicit non_replicable_partition_manager(
      ss::sharded<storage::api>& storage) noexcept;

    ss::future<> start() { return ss::now(); }
    ss::future<> stop_partitions();

    ss::lw_shared_ptr<non_replicable_partition>
    get(const model::ntp& ntp) const;
    ss::future<> manage(storage::ntp_config, ss::lw_shared_ptr<partition>);

    ss::future<> remove(const model::ntp&);

private:
    ss::future<> do_shutdown(ss::lw_shared_ptr<non_replicable_partition>);

private:
    ntp_table_container _ntp_table;
    ss::gate _gate;
    storage::api& _storage;

    friend std::ostream&
    operator<<(std::ostream&, const non_replicable_partition_manager&);
};

} // namespace cluster
