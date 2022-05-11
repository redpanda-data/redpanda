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
#include "kafka/server/group_router.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_scheduling_group.hh>

namespace kafka {

template<typename Request, typename FwdFunc>
auto group_router::route(Request&& r, FwdFunc func) {
    // get response type from FwdFunc it has return future<response>.
    using return_type = std::invoke_result_t<
      FwdFunc,
      decltype(std::declval<group_manager>()),
      Request&&>;
    using resp_type = typename return_type::value_type;

    auto m = shard_for(r.data.group_id);
    if (!m || _disabled) {
        vlog(klog.trace, "in route() not coordinator for {}", r.data.group_id);
        return ss::make_ready_future<resp_type>(
          resp_type(r, error_code::not_coordinator));
    }
    r.ntp = std::move(m->first);
    return with_scheduling_group(
      _sg, [this, func, shard = m->second, r = std::move(r)]() mutable {
          return get_group_manager().invoke_on(
            shard, _ssg, [func, r = std::move(r)](group_manager& mgr) mutable {
                return std::invoke(func, mgr, std::move(r));
            });
      });
}

template<typename Request, typename FwdFunc>
auto group_router::route_tx(Request&& r, FwdFunc func) {
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
            shard, _ssg, [func, r = std::move(r)](group_manager& mgr) mutable {
                return std::invoke(func, mgr, std::move(r));
            });
      });
}
template<typename Request, typename FwdFunc>
auto group_router::route_stages(Request r, FwdFunc func) {
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

ss::future<std::vector<deletable_group_result>>
group_router::route_delete_groups(
  ss::shard_id shard, std::vector<std::pair<model::ntp, group_id>> groups) {
    return ss::with_scheduling_group(
      _sg, [this, shard, groups = std::move(groups)]() mutable {
          return get_group_manager().invoke_on(
            shard,
            _ssg,
            [groups = std::move(groups)](group_manager& mgr) mutable {
                return mgr.delete_groups(std::move(groups));
            });
      });
}

ss::future<> group_router::parallel_route_delete_groups(
  std::vector<deletable_group_result>& results,
  sharded_groups& groups_by_shard) {
    return ss::parallel_for_each(
      groups_by_shard, [this, &results](sharded_groups::value_type& groups) {
          return route_delete_groups(groups.first, std::move(groups.second))
            .then([&results](std::vector<deletable_group_result> new_results) {
                results.insert(
                  results.end(), new_results.begin(), new_results.end());
            });
      });
}

ss::future<std::vector<deletable_group_result>>
group_router::delete_groups(std::vector<group_id> groups) {
    // partial results
    std::vector<deletable_group_result> results;

    // partition groups by owner shard
    sharded_groups groups_by_shard;
    for (auto& group : groups) {
        if (unlikely(_disabled)) {
            results.push_back(deletable_group_result{
              .group_id = std::move(group),
              .error_code = error_code::not_coordinator,
            });
            continue;
        }

        if (auto m = shard_for(group); m) {
            groups_by_shard[m->second].emplace_back(
              std::make_pair(std::move(m->first), std::move(group)));
        } else {
            results.push_back(deletable_group_result{
              .group_id = std::move(group),
              .error_code = error_code::not_coordinator,
            });
        }
    }

    return ss::do_with(
      std::move(results),
      std::move(groups_by_shard),
      [this](
        std::vector<deletable_group_result>& results,
        sharded_groups& groups_by_shard) {
          return parallel_route_delete_groups(results, groups_by_shard)
            .then([&results] { return std::move(results); });
      });
}

ss::future<described_group> group_router::describe_group(kafka::group_id g) {
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
group::join_group_stages
group_router::join_group(join_group_request&& request) {
    return route_stages(std::move(request), &group_manager::join_group);
}

group::sync_group_stages
group_router::sync_group(sync_group_request&& request) {
    return route_stages(std::move(request), &group_manager::sync_group);
}

ss::future<heartbeat_response>
group_router::heartbeat(heartbeat_request&& request) {
    return route(std::move(request), &group_manager::heartbeat);
}

ss::future<leave_group_response>
group_router::leave_group(leave_group_request&& request) {
    return route(std::move(request), &group_manager::leave_group);
}

ss::future<offset_fetch_response>
group_router::offset_fetch(offset_fetch_request&& request) {
    return route(std::move(request), &group_manager::offset_fetch);
}

group::offset_commit_stages
group_router::offset_commit(offset_commit_request&& request) {
    return route_stages(std::move(request), &group_manager::offset_commit);
}

ss::future<txn_offset_commit_response>
group_router::txn_offset_commit(txn_offset_commit_request&& request) {
    return route(std::move(request), &group_manager::txn_offset_commit);
}

ss::future<cluster::commit_group_tx_reply>
group_router::commit_tx(cluster::commit_group_tx_request request) {
    return route_tx(std::move(request), &group_manager::commit_tx);
}

ss::future<cluster::begin_group_tx_reply>
group_router::begin_tx(cluster::begin_group_tx_request request) {
    return route_tx(std::move(request), &group_manager::begin_tx);
}

ss::future<cluster::prepare_group_tx_reply>
group_router::prepare_tx(cluster::prepare_group_tx_request request) {
    return route_tx(std::move(request), &group_manager::prepare_tx);
}

ss::future<cluster::abort_group_tx_reply>
group_router::abort_tx(cluster::abort_group_tx_request request) {
    return route_tx(std::move(request), &group_manager::abort_tx);
}

ss::future<std::pair<error_code, std::vector<listed_group>>>
group_router::list_groups() {
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

} // namespace kafka
