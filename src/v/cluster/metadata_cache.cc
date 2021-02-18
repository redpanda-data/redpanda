// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/metadata_cache.h"

#include "cluster/members_table.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timestamp.h"

#include <seastar/core/sharded.hh>

#include <fmt/format.h>

#include <algorithm>
#include <iterator>
#include <optional>

namespace cluster {

metadata_cache::metadata_cache(
  ss::sharded<topic_table>& tp,
  ss::sharded<members_table>& m,
  ss::sharded<partition_leaders_table>& leaders)
  : _topics_state(tp)
  , _members_table(m)
  , _leaders(leaders) {}

std::vector<model::topic_namespace> metadata_cache::all_topics() const {
    return _topics_state.local().all_topics();
}

void fill_partition_leaders(
  partition_leaders_table& leaders, model::topic_metadata& tp_md) {
    for (auto& p : tp_md.partitions) {
        p.leader_node = leaders.get_leader(tp_md.tp_ns, p.id);
    }
}

std::optional<model::topic_metadata>
metadata_cache::get_topic_metadata(model::topic_namespace_view tp) const {
    auto md = _topics_state.local().get_topic_metadata(tp);
    if (!md) {
        return md;
    }
    fill_partition_leaders(_leaders.local(), *md);

    return md;
}
std::optional<topic_configuration>
metadata_cache::get_topic_cfg(model::topic_namespace_view tp) const {
    return _topics_state.local().get_topic_cfg(tp);
}

std::optional<model::timestamp_type>
metadata_cache::get_topic_timestamp_type(model::topic_namespace_view tp) const {
    return _topics_state.local().get_topic_timestamp_type(tp);
}

std::vector<model::topic_metadata> metadata_cache::all_topics_metadata() const {
    auto all_md = _topics_state.local().all_topics_metadata();
    for (auto& md : all_md) {
        fill_partition_leaders(_leaders.local(), md);
    }
    return all_md;
}

std::optional<broker_ptr> metadata_cache::get_broker(model::node_id nid) const {
    return _members_table.local().get_broker(nid);
}

std::vector<broker_ptr> metadata_cache::all_brokers() const {
    return _members_table.local().all_brokers();
}

std::vector<model::node_id> metadata_cache::all_broker_ids() const {
    return _members_table.local().all_broker_ids();
}

bool metadata_cache::contains(
  model::topic_namespace_view tp, const model::partition_id pid) const {
    return _topics_state.local().contains(tp, pid);
}

ss::future<model::node_id> metadata_cache::get_leader(
  const model::ntp& ntp,
  ss::lowres_clock::time_point tout,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    return _leaders.local().wait_for_leader(ntp, tout, as);
}

/// If present returns a leader of raft0 group
std::optional<model::node_id> metadata_cache::get_controller_leader_id() {
    return _leaders.local().get_leader(model::controller_ntp);
}

} // namespace cluster
