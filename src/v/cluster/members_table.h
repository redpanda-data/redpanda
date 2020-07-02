#pragma once

#include "cluster/types.h"
#include "model/metadata.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {

/// Class containing information about cluster members. The members class is
/// instantiated on each core. Cluster members updates are comming directly from
/// cluster::members_manager
class members_table {
public:
    using broker_ptr = ss::lw_shared_ptr<model::broker>;

    std::vector<broker_ptr> all_brokers() const;

    std::vector<model::node_id> all_broker_ids() const;

    /// Returns single broker if exists in cache
    std::optional<broker_ptr> get_broker(model::node_id) const;

    void update_brokers(patch<broker_ptr>);

private:
    using broker_cache_t = absl::flat_hash_map<model::node_id, broker_ptr>;
    broker_cache_t _brokers;
};
} // namespace cluster