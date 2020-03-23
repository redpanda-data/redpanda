#pragma once

#include "model/record_batch_reader.h"
#include "raft/service.h"
#include "raft/tron/logger.h"
#include "raft/tron/trongen_service.h"
#include "raft/tron/types.h"
#include "raft/types.h"
#include "seastarx.h"

namespace raft::tron {
template<typename ConsensusManager, typename ShardLookup>
CONCEPT(
  requires raft::RaftGroupManager<ConsensusManager>()
  && raft::ShardLookupManager<ShardLookup>())
struct service final : trongen_service {
    service(
      ss::scheduling_group sc,
      ss::smp_service_group ssg,
      ss::sharded<ConsensusManager>& mngr,
      ShardLookup& tbl)
      : trongen_service(sc, ssg)
      , _group_manager(mngr)
      , _shard_table(tbl) {}
    ss::future<stats_reply>
    stats(stats_request&&, rpc::streaming_context&) final {
        return ss::make_ready_future<stats_reply>(stats_reply{});
    }
    ss::future<put_reply>
    replicate(model::record_batch_reader&& r, rpc::streaming_context&) final {
        auto shard = _shard_table.shard_for(raft::group_id(66));
        return with_scheduling_group(
          get_scheduling_group(), [this, shard, r = std::move(r)]() mutable {
              return _group_manager.invoke_on(
                shard,
                get_smp_service_group(),
                [this, r = std::move(r)](ConsensusManager& m) mutable {
                    return m.consensus_for(group_id(66))
                      ->replicate(
                        std::move(r),
                        raft::replicate_options(
                          raft::consistency_level::quorum_ack))
                      .then_wrapped([](ss::future<result<replicate_result>> f) {
                          put_reply ret;
                          try {
                              f.get();
                              ret.success = true;
                          } catch (...) {
                              ret.failure_reason = fmt::format(
                                "{}", std::current_exception());
                              tronlog.error(
                                "failed to replicate: {}", ret.failure_reason);
                          }
                          return ret;
                      });
                });
          });
    }
    ss::sharded<ConsensusManager>& _group_manager;
    ShardLookup& _shard_table;
};

} // namespace raft::tron
