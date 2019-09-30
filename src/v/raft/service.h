#pragma once

#include "raft/consensus.h"
#include "raft/raftgen_service.h"

#include <seastar/core/sharded.hh>

namespace raft {
// clang-format off
CONCEPT(
    template<typename ConsensusManager>
    concept bool RaftGroupManager() {
        return requires(ConsensusManager m, group_id g) {
            { m.consensus_for(g) } -> consensus&;
    };
})
CONCEPT(
    template<typename ShardLookup>
    concept bool ShardLookupManager() {
        return requires(ShardLookup m, group_id g) {
            { m.shard_for(g) } -> shard_id;
    };
})
// clang-format on

template<typename ConsensusManager, typename ShardLookup>
CONCEPT(
  requires RaftGroupManager<ConsensusManager>()
  && ShardLookupManager<ShardLookup>())
class service final : public raftgen_service {
public:
    using failure_probes = raftgen_service::failure_probes;
    service(
      scheduling_group sc,
      smp_service_group ssg,
      sharded<ConsensusManager>& mngr,
      ShardLookup& tbl)
      : raftgen_service(sc, ssg)
      , _group_manager(mngr)
      , _shard_table(tbl) {
        finjector::shard_local_badger().register_probe(
          failure_probes::name(), &_probe);
    }
    ~service() {
        finjector::shard_local_badger().deregister_probe(
          failure_probes::name());
    }

    [[gnu::always_inline]] future<vote_reply>
    vote(vote_request&& r, rpc::streaming_context& ctx) {
        return _probe.vote().then([this, r = std::move(r), &ctx]() mutable {
            return do_vote(std::move(r), ctx);
        });
    }

    [[gnu::always_inline]] future<append_entries_reply>
    append_entries(append_entries_request&& r, rpc::streaming_context& ctx) {
        return _probe.append_entries().then(
          [this, r = std::move(r), &ctx]() mutable {
              return do_append_entries(std::move(r), ctx);
          });
    }

private:
    future<vote_reply> do_vote(vote_request&& r, rpc::streaming_context&) {
        auto shard = _shard_table.shard_for(group_id(r.group));
        return with_scheduling_group(
          get_scheduling_group(), [this, shard, r = std::move(r)]() mutable {
              return _group_manager.invoke_on(
                shard,
                get_smp_service_group(),
                [this, r = std::move(r)](ConsensusManager& m) mutable {
                    return m.consensus_for(group_id(r.group))
                      .vote(std::move(r));
                });
          });
    }
    future<append_entries_reply>
    do_append_entries(append_entries_request&& r, rpc::streaming_context&) {
        auto shard = _shard_table.shard_for(group_id(r.meta.group));
        return with_scheduling_group(
          get_scheduling_group(), [this, shard, r = std::move(r)]() mutable {
              return _group_manager.invoke_on(
                shard,
                get_smp_service_group(),
                [this, r = std::move(r)](ConsensusManager& m) mutable {
                    return m.consensus_for(group_id(r.meta.group))
                      .append_entries(std::move(r));
                });
          });
    }

    failure_probes _probe;
    sharded<ConsensusManager>& _group_manager;
    ShardLookup& _shard_table;
};
} // namespace raft
