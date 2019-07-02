#pragma once

#include "filesystem/write_ahead_log.h"
#include "model/metadata.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <vector>

namespace cluster {

// FIXME: Don't implement in terms of the log.
class metadata_cache {
public:
    explicit metadata_cache(seastar::sharded<write_ahead_log>&) noexcept;

    seastar::future<> stop() {
        return seastar::make_ready_future<>();
    }

    seastar::future<std::vector<model::topic_view>> all_topics() const;
    seastar::future<std::optional<model::topic_metadata>>
      get_topic_metadata(model::topic_view) const;

private:
    seastar::sharded<write_ahead_log>& _log;
};

} // namespace cluster