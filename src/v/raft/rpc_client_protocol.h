#pragma once

#include "model/metadata.h"
#include "outcome_future_utils.h"
#include "raft/consensus.h"
#include "raft/consensus_client_protocol.h"
#include "raft/errc.h"
#include "raft/raftgen_service.h"
#include "rpc/connection_cache.h"
#include "rpc/transport.h"

#include <system_error>

namespace raft {

/// Raft client protocol implementation underlied by RPC connections cache
class rpc_client_protocol final : public consensus_client_protocol::impl {
public:
    explicit rpc_client_protocol(ss::sharded<rpc::connection_cache>& cache)
      : _connection_cache(cache) {}

    ss::future<result<vote_reply>>
    vote(model::node_id, vote_request&&, clock_type::time_point) final;

    ss::future<result<append_entries_reply>> append_entries(
      model::node_id, append_entries_request&&, clock_type::time_point) final;

    ss::future<result<heartbeat_reply>> heartbeat(
      model::node_id, heartbeat_request&&, clock_type::time_point) final;

private:
    ss::sharded<rpc::connection_cache>& _connection_cache;
};

static consensus_client_protocol
make_rpc_client_protocol(ss::sharded<rpc::connection_cache>& clients) {
    return raft::make_consensus_client_protocol<raft::rpc_client_protocol>(
      clients);
}

} // namespace raft