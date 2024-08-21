// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/metadata_dissemination_handler.h"

#include "base/likely.h"
#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/metadata_dissemination_types.h"
#include "cluster/partition_leaders_table.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "ssx/async_algorithm.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/smp.hh>

#include <boost/range/irange.hpp>

#include <algorithm>
#include <exception>
#include <iterator>

namespace cluster {
metadata_dissemination_handler::metadata_dissemination_handler(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<partition_leaders_table>& leaders)
  : metadata_dissemination_rpc_service(sg, ssg)
  , _leaders(leaders) {}

ss::future<update_leadership_reply>
metadata_dissemination_handler::update_leadership_v2(
  update_leadership_request_v2 req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return do_update_leadership(std::move(req.leaders));
      });
}

ss::future<update_leadership_reply>
metadata_dissemination_handler::do_update_leadership(
  chunked_vector<ntp_leader_revision> leaders) {
    vlog(clusterlog.trace, "Received a metadata update");
    return ss::do_with(
             std::move(leaders),
             [this](const chunked_vector<ntp_leader_revision>& leaders) {
                 return ss::parallel_for_each(
                   boost::irange<ss::shard_id>(0, ss::smp::count),
                   [this, &leaders](ss::shard_id shard) {
                       return ss::smp::submit_to(shard, [this, &leaders] {
                           return ssx::async_for_each(
                             leaders,
                             [this](const ntp_leader_revision& leader) {
                                 _leaders.local().update_partition_leader(
                                   leader.ntp,
                                   leader.revision,
                                   leader.term,
                                   leader.leader_id);
                             });
                       });
                   });
             })
      .then([] { return update_leadership_reply{}; });
}

namespace {
ss::future<get_leadership_reply>
make_get_leadership_reply(const partition_leaders_table& leaders) {
    try {
        fragmented_vector<ntp_leader> ret;
        co_await leaders.for_each_leader([&ret](
                                           model::topic_namespace_view tp_ns,
                                           model::partition_id pid,
                                           std::optional<model::node_id> leader,
                                           model::term_id term) mutable {
            ret.emplace_back(model::ntp(tp_ns.ns, tp_ns.tp, pid), term, leader);
        });
        co_return get_leadership_reply{
          std::move(ret), get_leadership_reply::is_success::yes};
    } catch (...) {
        vlog(
          clusterlog.info,
          "exception thrown while collecting leadership metadata - {}",
          std::current_exception());
        co_return get_leadership_reply{
          {}, get_leadership_reply::is_success::no};
    }
}
} // namespace

ss::future<get_leadership_reply> metadata_dissemination_handler::get_leadership(
  get_leadership_request, rpc::streaming_context&) {
    return ss::with_scheduling_group(get_scheduling_group(), [this]() mutable {
        return make_get_leadership_reply(_leaders.local());
    });
}

} // namespace cluster
