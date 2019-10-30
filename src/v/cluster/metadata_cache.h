#pragma once

#include "cluster/types.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <unordered_map>
#include <vector>

namespace cluster {

class metadata_cache {
public:
    using cache_t = std::unordered_map<model::topic, topic_metadata_entry>;
    metadata_cache() noexcept = default;

    future<> stop() {
        return make_ready_future<>();
    }
    // API used by Kafka
    future<std::vector<model::topic_view>> all_topics() const;
    future<std::optional<model::topic_metadata>>
      get_topic_metadata(model::topic_view) const;

    // Cache update API - used by the controller
    void add_topic(model::topic_view);
    void remove_topic(model::topic_view);
    void update_partition_assignment(const partition_assignment&);
    void update_partition_leader(
      model::topic_view, model::partition_id, model::node_id);

private:
    cache_t _cache;
    cache_t::iterator find_topic_metadata(model::topic_view);
};
} // namespace cluster
