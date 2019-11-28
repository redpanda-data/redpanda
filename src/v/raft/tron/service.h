#pragma once

#include "raft/tron/trongen_service.h"
#include "raft/tron/types.h"

namespace raft::tron {

struct service final : trongen_service {
    using trongen_service::trongen_service;
    future<stats_reply> stats(stats_request&&, rpc::streaming_context&) final {
        return make_ready_future<stats_reply>(stats_reply{});
    }
    future<put_reply> put(::raft::entry&&, rpc::streaming_context&) final {
        throw std::runtime_error("unimplemented method");
    }
};

} // namespace raft::tron
