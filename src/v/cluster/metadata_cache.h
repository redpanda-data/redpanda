#pragma once

#include "cluster/types.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

#include <unordered_map>

namespace cluster {

class metadata_cache {
public:
    // struct holding the cache content
    struct metadata {
        std::vector<model::partition_metadata> partitions;
    };

    using cache_t = std::unordered_map<model::topic, metadata>;

    metadata_cache() noexcept = default;
    future<> stop() {
        return make_ready_future<>();
    }

    /// Returns future containing list of all topics that exists in the cluster.
    future<std::vector<model::topic>> all_topics() const;

    ///\brief Returns metadata of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    future<std::optional<model::topic_metadata>>
      get_topic_metadata(model::topic_view) const;

    ///\brief Add empty model::topic_metadata entry to cache
    ///
    /// This api is used when controller is recovering (or is notified)
    /// topic_configuration record type
    void add_topic(model::topic_view);

    ///\brief Removes the topic from cache
    ///
    /// Not yet used by the controller as removing topics is not yet supported
    void remove_topic(model::topic_view);

    ///\brief Updates the assignment of topic partion
    ///
    /// It is used by the controller when processing partition_assignment record
    /// types
    void update_partition_assignment(const partition_assignment&);

    ///\brief Updates leader of topic partition.
    ///
    /// It is not yet used by the controller, it will be used when controller
    /// will process leadership change notifications
    void update_partition_leader(
      model::topic_view, model::partition_id, model::node_id);

private:
    cache_t _cache;
    cache_t::iterator find_topic_metadata(model::topic_view);
};

model::topic_metadata
create_topic_metadata(const metadata_cache::cache_t::value_type&);

/// Looks for partition with requested id in topic_metadata type.
std::optional<std::reference_wrapper<model::partition_metadata>>
find_partition(metadata_cache::metadata&, model::partition_id);

} // namespace cluster
