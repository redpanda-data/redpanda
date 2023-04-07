// Copyright 2020 Redpanda Data, Inc.
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

#include <algorithm>
#include <iterator>

namespace {
std::vector<cluster::ntp_leader_revision>
from_ntp_leaders(std::vector<cluster::ntp_leader> old_leaders) {
    std::vector<cluster::ntp_leader_revision> leaders;
    leaders.reserve(old_leaders.size());
    std::transform(
      old_leaders.begin(),
      old_leaders.end(),
      std::back_inserter(leaders),
      [](cluster::ntp_leader& leader) {
          return cluster::ntp_leader_revision(
            std::move(leader.ntp),
            leader.term,
            leader.leader_id,
            model::revision_id{} /* explicitly default */
          );
      });
    return leaders;
}
} // namespace
namespace cluster {
metadata_dissemination_handler::metadata_dissemination_handler(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<partition_leaders_table>& leaders)
  : metadata_dissemination_rpc_service(sg, ssg)
  , _leaders(leaders) {}

ss::future<update_leadership_reply>
metadata_dissemination_handler::update_leadership(
  update_leadership_request&& req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return do_update_leadership(from_ntp_leaders(std::move(req.leaders)));
      });
}

ss::future<update_leadership_reply>
metadata_dissemination_handler::update_leadership_v2(
  update_leadership_request_v2&& req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return do_update_leadership(std::move(req.leaders));
      });
}

ss::future<update_leadership_reply>
metadata_dissemination_handler::do_update_leadership(
  std::vector<ntp_leader_revision> leaders) {
    vlog(clusterlog.trace, "Received a metadata update");
    return _leaders
      .invoke_on_all(
        [leaders = std::move(leaders)](partition_leaders_table& pl) mutable {
            for (auto& leader : leaders) {
                pl.update_partition_leader(
                  leader.ntp, leader.revision, leader.term, leader.leader_id);
            }
        })
      .then([] { return ss::make_ready_future<update_leadership_reply>(); });
}

static get_leadership_reply
make_get_leadership_reply(const partition_leaders_table& leaders) {
    std::vector<ntp_leader> ret;
    leaders.for_each_leader([&ret](
                              model::topic_namespace_view tp_ns,
                              model::partition_id pid,
                              std::optional<model::node_id> leader,
                              model::term_id term) mutable {
        ret.emplace_back(model::ntp(tp_ns.ns, tp_ns.tp, pid), term, leader);
    });

    return get_leadership_reply{std::move(ret)};
}

ss::future<get_leadership_reply> metadata_dissemination_handler::get_leadership(
  get_leadership_request&&, rpc::streaming_context&) {
    return ss::with_scheduling_group(get_scheduling_group(), [this]() mutable {
        return ss::make_ready_future<get_leadership_reply>(
          make_get_leadership_reply(_leaders.local()));
    });
}

} // namespace cluster
