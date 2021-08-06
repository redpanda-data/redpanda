// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/metadata_dissemination_handler.h"

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/metadata_dissemination_types.h"
#include "cluster/partition_leaders_table.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "rpc/connection_cache.h"

namespace cluster {
static ntp_leaders
expand_leaders(const topic_table& topics, ntp_leaders current) {
    ntp_leaders all_leaders;
    all_leaders.reserve(current.size());
    for (auto& leader : current) {
        auto materialized_leaders = topics.materialized_children(leader.ntp);
        for (auto& materialized_leader : materialized_leaders) {
            ntp_leader new_leader{
              .ntp = std::move(materialized_leader),
              .term = leader.term,
              .leader_id = leader.leader_id};
            all_leaders.push_back(std::move(new_leader));
        }
        all_leaders.push_back(std::move(leader));
    }
    return all_leaders;
}

metadata_dissemination_handler::metadata_dissemination_handler(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<topic_table>& topics)
  : metadata_dissemination_rpc_service(sg, ssg)
  , _leaders(leaders)
  , _topics(topics) {}

ss::future<update_leadership_reply>
metadata_dissemination_handler::update_leadership(
  update_leadership_request&& req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return do_update_leadership(std::move(req));
      });
}

ss::future<update_leadership_reply>
metadata_dissemination_handler::do_update_leadership(
  update_leadership_request&& req) {
    return _leaders
      .invoke_on_all(
        [this, req = std::move(req)](partition_leaders_table& pl) mutable {
            auto leaders = expand_leaders(
              _topics.local(), std::move(req.leaders));
            for (auto& leader : leaders) {
                pl.update_partition_leader(
                  leader.ntp, leader.term, leader.leader_id);
            }
        })
      .then([] { return ss::make_ready_future<update_leadership_reply>(); });
}

static get_leadership_reply
make_get_leadership_reply(const partition_leaders_table& leaders) {
    ntp_leaders ret;
    leaders.for_each_leader([&ret](
                              model::topic_namespace_view tp_ns,
                              model::partition_id pid,
                              std::optional<model::node_id> leader,
                              model::term_id term) mutable {
        ret.emplace_back(ntp_leader{
          .ntp = model::ntp(tp_ns.ns, tp_ns.tp, pid),
          .term = term,
          .leader_id = leader});
    });

    return get_leadership_reply{std::move(ret)};
}

ss::future<get_leadership_reply> metadata_dissemination_handler::get_leadership(
  get_leadership_request&& req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return ss::make_ready_future<get_leadership_reply>(
            make_get_leadership_reply(_leaders.local()));
      });
}

} // namespace cluster
