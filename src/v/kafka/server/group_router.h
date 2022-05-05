/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/feature_table.h"
#include "cluster/fwd.h"
#include "cluster/shard_table.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/heartbeat.h"
#include "kafka/protocol/join_group.h"
#include "kafka/protocol/leave_group.h"
#include "kafka/protocol/list_groups.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/schemata/delete_groups_response.h"
#include "kafka/protocol/sync_group.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/group_manager.h"
#include "kafka/types.h"
#include "seastarx.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include <exception>
#include <type_traits>

namespace kafka {

/**
 * \brief Forward group operations the owning core.
 *
 * Routing an operation is a two step process. First, the coordinator key is
 * mapped to its associated ntp using the coordinator_ntp_mapper. Given the ntp
 * the owning shard is found using the cluster::shard_table. Finally, a x-core
 * operation on the destination shard's group manager is invoked.
 */
class group_router final {
    template<typename Request, typename FwdFunc>
    auto route(Request&& r, FwdFunc func) {
        // get response type from FwdFunc it has return future<response>.
        using return_type = std::invoke_result_t<
          FwdFunc,
          decltype(std::declval<group_manager>()),
          Request&&>;
        using resp_type = typename return_type::value_type;

        auto m = shard_for(r.data.group_id);
        if (!m || _disabled) {
            return ss::make_ready_future<resp_type>(
              resp_type(r, error_code::not_coordinator));
        }
        r.ntp = std::move(m->first);
        return with_scheduling_group(
          _sg, [this, func, shard = m->second, r = std::move(r)]() mutable {
              return get_group_manager().invoke_on(
                shard,
                _ssg,
                [func, r = std::move(r)](group_manager& mgr) mutable {
                    return std::invoke(func, mgr, std::move(r));
                });
          });
    }

    template<typename Request, typename FwdFunc>
    auto route_tx(Request&& r, FwdFunc func) {
        // get response type from FwdFunc it has return future<response>.
        using return_type = std::invoke_result_t<
          FwdFunc,
          decltype(std::declval<group_manager>()),
          Request&&>;
        using resp_type = typename return_type::value_type;

        auto m = shard_for(r.group_id);
        if (!m || _disabled) {
            resp_type reply;
            // route_tx routes internal intra cluster so it uses
            // cluster::tx_errc instead of kafka::error_code
            // because the latter is part of the kafka protocol
            // we can't extend it
            reply.ec = cluster::tx_errc::not_coordinator;
            return ss::make_ready_future<resp_type>(reply);
        }
        r.ntp = std::move(m->first);
        return with_scheduling_group(
          _sg, [this, func, shard = m->second, r = std::move(r)]() mutable {
              return get_group_manager().invoke_on(
                shard,
                _ssg,
                [func, r = std::move(r)](group_manager& mgr) mutable {
                    return std::invoke(func, mgr, std::move(r));
                });
          });
    }
    template<typename Request, typename FwdFunc>
    auto route_stages(Request r, FwdFunc func) {
        using return_type = std::invoke_result_t<
          FwdFunc,
          decltype(std::declval<group_manager>()),
          Request&&>;
        using resp_type = typename return_type::value_type;
        auto m = shard_for(r.data.group_id);
        if (!m || _disabled) {
            return group::stages(resp_type(r, error_code::not_coordinator));
        }
        r.ntp = std::move(m->first);
        auto dispatched = std::make_unique<ss::promise<>>();
        auto dispatched_f = dispatched->get_future();
        auto f = with_scheduling_group(
          _sg,
          [this,
           shard = m->second,
           r = std::move(r),
           dispatched = std::move(dispatched),
           func]() mutable {
              return get_group_manager().invoke_on(
                shard,
                _ssg,
                [r = std::move(r),
                 dispatched = std::move(dispatched),
                 source_shard = ss::this_shard_id(),
                 func](group_manager& mgr) mutable {
                    auto stages = std::invoke(func, mgr, std::move(r));
                    /**
                     * dispatched future is always ready before committed one,
                     * we do not have to use gate in here
                     */
                    return stages.dispatched
                      .then_wrapped([source_shard, d = std::move(dispatched)](
                                      ss::future<> f) mutable {
                          if (f.failed()) {
                              (void)ss::smp::submit_to(
                                source_shard,
                                [d = std::move(d),
                                 e = f.get_exception()]() mutable {
                                    d->set_exception(e);
                                    d.reset();
                                });
                              return;
                          }
                          (void)ss::smp::submit_to(
                            source_shard, [d = std::move(d)]() mutable {
                                d->set_value();
                                d.reset();
                            });
                      })
                      .then([f = std::move(stages.result)]() mutable {
                          return std::move(f);
                      });
                });
          });
        return return_type(std::move(dispatched_f), std::move(f));
    }

public:
    group_router(
      ss::scheduling_group sched_group,
      ss::smp_service_group smp_group,
      ss::sharded<group_manager>& gr_manager,
      ss::sharded<group_manager>& consumer_offsets_gr_manager,
      ss::sharded<cluster::shard_table>& shards,
      ss::sharded<coordinator_ntp_mapper>& coordinators,
      ss::sharded<coordinator_ntp_mapper>& consumer_offsets_coordinators,
      ss::sharded<cluster::feature_table>& feature_table)
      : _sg(sched_group)
      , _ssg(smp_group)
      , _group_manager(gr_manager)
      , _consumer_offsets_group_manager(consumer_offsets_gr_manager)
      , _shards(shards)
      , _coordinators(coordinators)
      , _consumer_offsets_coordinators(consumer_offsets_coordinators)
      , _feature_table(feature_table) {}

    auto join_group(join_group_request&& request) {
        return route_stages(std::move(request), &group_manager::join_group);
    }

    auto sync_group(sync_group_request&& request) {
        return route_stages(std::move(request), &group_manager::sync_group);
    }

    auto heartbeat(heartbeat_request&& request) {
        return route(std::move(request), &group_manager::heartbeat);
    }

    auto leave_group(leave_group_request&& request) {
        return route(std::move(request), &group_manager::leave_group);
    }

    group::offset_commit_stages offset_commit(offset_commit_request&& request) {
        return route_stages(std::move(request), &group_manager::offset_commit);
    }

    auto txn_offset_commit(txn_offset_commit_request&& request) {
        return route(std::move(request), &group_manager::txn_offset_commit);
    }

    auto commit_tx(cluster::commit_group_tx_request request) {
        return route_tx(std::move(request), &group_manager::commit_tx);
    }

    auto begin_tx(cluster::begin_group_tx_request request) {
        return route_tx(std::move(request), &group_manager::begin_tx);
    }

    auto prepare_tx(cluster::prepare_group_tx_request request) {
        return route_tx(std::move(request), &group_manager::prepare_tx);
    }

    auto abort_tx(cluster::abort_group_tx_request request) {
        return route_tx(std::move(request), &group_manager::abort_tx);
    }

    auto offset_fetch(offset_fetch_request&& request) {
        return route(std::move(request), &group_manager::offset_fetch);
    }

    // return groups from across all shards, and if any core was still loading
    ss::future<std::pair<error_code, std::vector<listed_group>>> list_groups() {
        using type = std::pair<error_code, std::vector<listed_group>>;
        return get_group_manager().map_reduce0(
          [](group_manager& mgr) { return mgr.list_groups(); },
          type{},
          [](type a, type b) {
              // reduce errors into `a` and retain the first
              if (a.first == error_code::none) {
                  a.first = b.first;
              }
              a.second.insert(a.second.end(), b.second.begin(), b.second.end());
              return a;
          });
    }

    ss::future<described_group> describe_group(kafka::group_id g) {
        auto m = shard_for(g);
        if (!m) {
            return ss::make_ready_future<described_group>(
              describe_groups_response::make_empty_described_group(
                std::move(g), error_code::not_coordinator));
        }
        return with_scheduling_group(
          _sg, [this, g = std::move(g), m = std::move(m)]() mutable {
              return get_group_manager().invoke_on(
                m->second,
                _ssg,
                [g = std::move(g),
                 ntp = std::move(m->first)](group_manager& mgr) mutable {
                    return mgr.describe_group(ntp, g);
                });
          });
    }

    ss::future<std::vector<deletable_group_result>>
    delete_groups(std::vector<group_id> groups);

    ss::sharded<coordinator_ntp_mapper>& coordinator_mapper() {
        if (use_consumer_offsets_topic()) {
            return _consumer_offsets_coordinators;
        }
        return _coordinators;
    }

    void disable() { _disabled = true; }

    void enable() { _disabled = false; }

    ss::sharded<group_manager>& get_group_manager() {
        if (use_consumer_offsets_topic()) {
            return _consumer_offsets_group_manager;
        }
        return _group_manager;
    }

private:
    using sharded_groups = absl::
      node_hash_map<ss::shard_id, std::vector<std::pair<model::ntp, group_id>>>;

    std::optional<std::pair<model::ntp, ss::shard_id>>
    shard_for(const group_id& group) {
        if (auto ntp = coordinator_mapper().local().ntp_for(group); ntp) {
            if (auto shard_id = _shards.local().shard_for(*ntp); shard_id) {
                return std::make_pair(std::move(*ntp), *shard_id);
            }
        }
        return std::nullopt;
    }

    bool use_consumer_offsets_topic() {
        return _feature_table.local().is_active(
          cluster::feature::consumer_offsets);
    }

    ss::future<std::vector<deletable_group_result>> route_delete_groups(
      ss::shard_id, std::vector<std::pair<model::ntp, group_id>>);

    ss::future<> parallel_route_delete_groups(
      std::vector<deletable_group_result>&, sharded_groups&);

    ss::scheduling_group _sg;
    ss::smp_service_group _ssg;
    ss::sharded<group_manager>& _group_manager;
    ss::sharded<group_manager>& _consumer_offsets_group_manager;
    ss::sharded<cluster::shard_table>& _shards;
    ss::sharded<coordinator_ntp_mapper>& _coordinators;
    ss::sharded<coordinator_ntp_mapper>& _consumer_offsets_coordinators;
    ss::sharded<cluster::feature_table>& _feature_table;
    bool _disabled = false;
};

} // namespace kafka
