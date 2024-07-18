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

#include "kafka/server/logger.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_scheduling_group.hh>

#include <algorithm>
#include <iterator>

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
    if (!m) {
        vlog(klog.trace, "in route() not coordinator for {}", r.data.group_id);
        return ss::make_ready_future<resp_type>(
          resp_type(r, error_code::not_coordinator));
    }
    r.ntp = std::move(m->first);
    return with_scheduling_group(
      _sg, [this, func, shard = m->second, r = std::move(r)]() mutable {
          return get_group_manager().invoke_on(shard, _ssg, func, std::move(r));
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
    if (!m) {
        resp_type reply;
        // route_tx routes internal intra cluster so it uses
        // cluster::tx::errc instead of kafka::error_code
        // because the latter is part of the kafka protocol
        // we can't extend it
        reply.ec = cluster::tx::errc::not_coordinator;
        return ss::make_ready_future<resp_type>(reply);
    }
    r.ntp = std::move(m->first);
    return with_scheduling_group(
      _sg, [this, func, shard = m->second, r = std::move(r)]() mutable {
          return get_group_manager().invoke_on(shard, _ssg, func, std::move(r));
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
    if (!m) {
        return group::stages(resp_type(r, error_code::not_coordinator));
    }
    r.ntp = std::move(m->first);
    auto dispatched = std::make_unique<ss::promise<>>();
    auto dispatched_f = dispatched->get_future();
    auto coordinator_shard = m->second;

    auto f = with_scheduling_group(
      _sg,
      [this,
       coordinator_shard = coordinator_shard,
       r = std::move(r),
       dispatched = std::move(dispatched),
       func]() mutable {
          return get_group_manager()
            .invoke_on(
              coordinator_shard,
              _ssg,
              [r = std::move(r), func](group_manager& mgr) mutable {
                  group::stages<resp_type> stages = std::invoke(
                    func, mgr, std::move(r));
                  /**
                   * dispatched future is always ready before committed one,
                   * we do not have to use gate in here
                   */
                  auto dstage = stages.dispatched.then_wrapped(
                    [rfut = std::move(stages.result)](ss::future<> f) mutable {
                        auto e = f.failed() ? f.get_exception() : nullptr;
                        return std::make_tuple(
                          e, std::make_unique<decltype(rfut)>(std::move(rfut)));
                    });

                  return dstage;
              })
            .then([this,
                   coordinator_shard = coordinator_shard,
                   d = std::move(dispatched)](auto f) {
                // In the immediately preceding continuation we did not return
                // the stages.result future directly, rather we wrapped it with
                // a unique_ptr. This means that the future doesn't receive the
                // cross-shard treatment and is only safe to use on the
                // coordinator shard, not this one. So next we pass this future
                // back to the coordinator to unwrap it and return it unwrapped,
                // so it can be used by this shard (and, as a side effect the
                // dispatched future is also set).
                //
                // Why do we need this second trip rather than just returning
                // the unwrapped future above?
                //
                // Well, this second stage may take a very long time resolve and
                // may depend on coordination with clients: e.g., the JoinGroup
                // request won't be responded to until all other current members
                // have also sent their JoinGroup requests: all the joiners have
                // their responses held until that moment. Above, the invoke_on
                // is under the influence of the kafka smp_service_group (_ssg),
                // with a limit of 5,000 outstanding requests on any code to the
                // coordinator: this means that a group with more than 5,000
                // members cannot successfully be joined: at most join 5,000
                // requests can be outstanding at once, any more will block on
                // the ssg, and the existing ones won't complete because they
                // are waiting for the blocked ones: a type of "soft" deadlock
                // (soft because the server eventually) times out the join
                // process an the process repeats.
                //
                // This is solved by not returning the state.result future
                // directly above, but wrapping it: this means the returned
                // future resolves as soon as the dispatch phase completes,
                // freeing up the service group slot for another request to
                // dispatch. We extract the stage.result below in a new
                // invoke_on call *without any ssg*: this uses the default ssg
                // which does not have any limit, avoiding the deadlock.

                auto dispatched_exception = std::move(std::get<0>(f));
                if (dispatched_exception) {
                    d->set_exception(dispatched_exception);
                } else {
                    d->set_value();
                }
                return get_group_manager().invoke_on(
                  coordinator_shard,
                  [result_ptr = std::move(std::get<1>(f))](
                    group_manager&) mutable { return std::move(*result_ptr); });
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
group_router::delete_groups(chunked_vector<group_id> groups) {
    // partial results
    std::vector<deletable_group_result> results;

    // partition groups by owner shard
    sharded_groups groups_by_shard;
    for (auto& group : groups) {
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

ss::future<offset_delete_response>
group_router::offset_delete(offset_delete_request&& request) {
    return route(std::move(request), &group_manager::offset_delete);
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

ss::future<cluster::abort_group_tx_reply>
group_router::abort_tx(cluster::abort_group_tx_request request) {
    return route_tx(std::move(request), &group_manager::abort_tx);
}

ss::future<std::pair<error_code, chunked_vector<listed_group>>>
group_router::list_groups(group_manager::list_groups_filter_data filter_data) {
    using type = std::pair<error_code, chunked_vector<listed_group>>;
    return get_group_manager().map_reduce0(
      [filter_data = std::move(filter_data)](group_manager& mgr) {
          return mgr.list_groups(filter_data);
      },
      type{},
      [](type a, type b) {
          // reduce errors into `a` and retain the first
          if (a.first == error_code::none) {
              a.first = b.first;
          }
          auto& av = a.second;
          auto& bv = b.second;
          av.reserve(av.size() + bv.size());
          std::copy(bv.begin(), bv.end(), std::back_inserter(av));
          return a;
      });
}

} // namespace kafka
