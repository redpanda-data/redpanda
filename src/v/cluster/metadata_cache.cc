#include "cluster/metadata_cache.h"

#include "seastarx.h"

#include <seastar/core/thread.hh>

#include <boost/range/algorithm/copy.hpp>

namespace cluster {

future<std::vector<model::topic_view>> metadata_cache::all_topics() const {
    return make_ready_future<std::vector<model::topic_view>>();
}

future<std::optional<model::topic_metadata>>
metadata_cache::get_topic_metadata(model::topic_view topic) const {
    return make_ready_future<std::optional<model::topic_metadata>>();
}

} // namespace cluster
