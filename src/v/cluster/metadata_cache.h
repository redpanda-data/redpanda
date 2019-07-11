#pragma once

#include "filesystem/write_ahead_log.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <vector>

namespace cluster {

// FIXME: Don't implement in terms of the log.
class metadata_cache {
public:
    explicit metadata_cache(sharded<write_ahead_log>&) noexcept;

    future<> stop() {
        return make_ready_future<>();
    }

    future<std::vector<model::topic_view>> all_topics() const;
    future<std::optional<model::topic_metadata>>
      get_topic_metadata(model::topic_view) const;

private:
    sharded<write_ahead_log>& _log;
};

} // namespace cluster
