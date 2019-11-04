#pragma once

#include "model/metadata.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <vector>

namespace cluster {

class metadata_cache {
public:
    metadata_cache() noexcept = default;

    future<> stop() {
        return make_ready_future<>();
    }

    future<std::vector<model::topic>> all_topics() const;
    future<std::optional<model::topic_metadata>>
      get_topic_metadata(model::topic_view) const;
};

} // namespace cluster
