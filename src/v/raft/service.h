#pragma once

#include "raft/consensus.h"
#include "raft/raftgen_service.h"
#include "seastarx.h"
#include "utils/copy_range.h"

#include <seastar/core/sharded.hh>

namespace raft {
// clang-format off
CONCEPT(
    template<typename ConsensusManager>
    concept bool RaftGroupManager() {
        return requires(ConsensusManager m, group_id g) {
            { m.consensus_for(g) } -> ss::lw_shared_ptr<consensus>
    };
})
CONCEPT(
    template<typename ShardLookup>
    concept bool ShardLookupManager() {
        return requires(ShardLookup m, group_id g) {
            { m.shard_for(g) } -> ss::shard_id;
            { m.contains(g) } -> bool;
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
      ss::scheduling_group sc,
      ss::smp_service_group ssg,
      ss::sharded<ConsensusManager>& mngr,
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

    [[gnu::always_inline]] ss::future<heartbeat_reply>
    heartbeat(heartbeat_request&& r, rpc::streaming_context& ctx) final {
        std::vector<append_entries_request> reqs;
        reqs.reserve(r.meta.size());
        for (auto& m : r.meta) {
            reqs.push_back(raft::append_entries_request{
              .node_id = r.node_id,
              .meta = std::move(m),
              .batches = model::make_memory_record_batch_reader(
                ss::circular_buffer<model::record_batch>{})});
        }
        return ss::do_with(
                 std::move(reqs),
                 [this,
                  &ctx](std::vector<append_entries_request>& reqs) mutable {
                     return copy_range<std::vector<append_entries_reply>>(
                       reqs, [this, &ctx](append_entries_request& r) mutable {
                           return append_entries(std::move(r), ctx);
                       });
                 })
          .then([](std::vector<append_entries_reply> r) {
              return ss::make_ready_future<heartbeat_reply>(
                heartbeat_reply{std::move(r)});
          });
    }

    [[gnu::always_inline]] ss::future<vote_reply>
    vote(vote_request&& r, rpc::streaming_context& ctx) final {
        return _probe.vote().then([this, r = std::move(r), &ctx]() mutable {
            return do_vote(std::move(r), ctx);
        });
    }

    [[gnu::always_inline]] ss::future<append_entries_reply> append_entries(
      append_entries_request&& r, rpc::streaming_context& ctx) final {
        return _probe.append_entries().then(
          [this, r = std::move(r), &ctx]() mutable {
              return do_append_entries(std::move(r), ctx);
          });
    }

private:
    ss::future<vote_reply> make_failed_vote_reply() {
        return ss::make_ready_future<vote_reply>(vote_reply{
          .term = model::term_id{}(), .granted = false, .log_ok = false});
    }

    ss::future<vote_reply> do_vote(vote_request&& r, rpc::streaming_context&) {
        auto group = group_id(r.group);
        if (unlikely(!_shard_table.contains(group))) {
            return make_failed_vote_reply();
        }
        auto shard = _shard_table.shard_for(group);
        return with_scheduling_group(
          get_scheduling_group(), [this, shard, r = std::move(r)]() mutable {
              return _group_manager.invoke_on(
                shard,
                get_smp_service_group(),
                [this, r = std::move(r)](ConsensusManager& m) mutable {
                    auto c = m.consensus_for(group_id(r.group));
                    if (unlikely(!c)) {
                        return make_failed_vote_reply();
                    }
                    return c->vote(std::move(r));
                });
          });
    }

    ss::future<append_entries_reply>
    make_missing_group_reply(raft::group_id group) {
        return ss::make_ready_future<append_entries_reply>(append_entries_reply{
          .group = group(),
          .result = append_entries_reply::status::group_unavailable});
    }

    ss::future<append_entries_reply>
    do_append_entries(append_entries_request&& r, rpc::streaming_context&) {
        auto group = group_id(r.meta.group);
        if (unlikely(!_shard_table.contains(group))) {
            return make_missing_group_reply(group);
        }
        auto shard = _shard_table.shard_for(group);
        return with_scheduling_group(
          get_scheduling_group(), [this, shard, r = std::move(r)]() mutable {
              return _group_manager.invoke_on(
                shard,
                get_smp_service_group(),
                [this, r = std::move(r)](ConsensusManager& m) mutable {
                    auto group = group_id(r.meta.group);
                    auto c = m.consensus_for(group);
                    if (unlikely(!c)) {
                        return make_missing_group_reply(group);
                    }
                    return c->append_entries(std::move(r));
                });
          });
    }

    failure_probes _probe;
    ss::sharded<ConsensusManager>& _group_manager;
    ShardLookup& _shard_table;
};
} // namespace raft
