#pragma once

#include "raft/consensus.h"
#include "raft/node_local_controller.h"
#include "raft/raftgen_service.h"

#include <seastar/core/sharded.hh>

namespace raft {
class service final : public raftgen_service {
public:
    using failure_probes = raftgen_service::failure_probes;
    service(
      scheduling_group sc,
      smp_service_group ssg,
      sharded<consensus>&,
      sharded<node_local_controller>&);
    ~service();

    [[gnu::always_inline]] future<vote_reply>
    vote(vote_request r, rpc::streaming_context& ctx) {
        return _probe.vote().then([this, r = std::move(r), &ctx]() mutable {
            return do_vote(std::move(r), ctx);
        });
    }

    [[gnu::always_inline]] future<append_entries_reply>
    append_entries(append_entries_request r, rpc::streaming_context& ctx) {
        return _probe.append_entries().then(
          [this, r = std::move(r), &ctx]() mutable {
              return do_append_entries(std::move(r), ctx);
          });
    }

    [[gnu::always_inline]] future<configuration_reply>
    configuration_update(configuration_request r, rpc::streaming_context& ctx) {
        return _probe.configuration_update().then(
          [this, r = std::move(r), &ctx]() mutable {
              return do_configuration_update(std::move(r), ctx);
          });
    }

private:
    future<vote_reply> do_vote(vote_request, rpc::streaming_context&);

    future<append_entries_reply>
    do_append_entries(append_entries_request, rpc::streaming_context&);

    future<configuration_reply>
    do_configuration_update(configuration_request, rpc::streaming_context&);

    failure_probes _probe;
    sharded<consensus>& _consensus;
    sharded<node_local_controller>& _nlc;
};
} // namespace raft
