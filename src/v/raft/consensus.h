#pragma once

#include "raft/client_cache.h"
#include "raft/configuration.h"
#include "seastarx.h"

#include <seastar/core/sharded.hh>

namespace raft {

class consensus {
public:
    consensus(configuration, sharded<client_cache>&);

    future<vote_reply> vote(vote_request);

    future<append_entries_reply> append_entries(append_entries_request);

    future<configuration_reply> configuration_update(configuration_request);

    const configuration cfg;

private:
    sharded<client_cache>& _clients;
};

} // namespace raft
