/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/types.h"
#include "raft/fundamental.h"
#include "raft/recovery_rpc_service.h"
#include "raft/recovery_rpc_types.h"
#include "rpc/types.h"

#include <seastar/core/sharded.hh>

#include <type_traits>

namespace raft {

class recovery_rpc_handler final : public recovery_rpc_service {
public:
    recovery_rpc_handler(
      ss::scheduling_group sg,
      ss::smp_service_group ssg,
      ss::sharded<cluster::partition_manager>& pm,
      ss::sharded<cluster::shard_table>& st)
      : recovery_rpc_service(sg, ssg)
      , _sg(sg)
      , _pm(pm)
      , _st(st) {}

    ss::future<reset_learner_state_reply> reset_learner_state(
      reset_learner_state_request, rpc::streaming_context&) final;

private:
    static ss::future<reset_learner_state_reply> do_reset_learner_state(
      reset_learner_state_request, cluster::consensus_ptr);

    template<
      typename Req,
      typename Func,
      typename Ret = std::invoke_result_t<Func, Req, cluster::consensus_ptr>>
    Ret dispatch_request(Req&& r, Func&& f) {
        auto shard_for_opt = _st.local().shard_for(r.id);
        if (shard_for_opt) {
            return _pm.invoke_on(
              shard_for_opt.value(),
              [r = std::forward<Req>(r), f = std::forward<Func>(f)](
                cluster::partition_manager& pm) mutable {
                  auto c = pm.consensus_for(r.id);
                  return f(std::forward<Req>(r), c);
              });
        }

        return seastar::make_ready_future<typename Ret::value_type>();
    }

    ss::scheduling_group _sg;
    ss::sharded<cluster::partition_manager>& _pm;
    ss::sharded<cluster::shard_table>& _st;
    friend class in_memory_recovery_client;
};

} // namespace raft
