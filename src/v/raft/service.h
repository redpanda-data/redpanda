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

#include "absl/container/flat_hash_map.h"
#include "base/likely.h"
#include "base/seastarx.h"
#include "model/metadata.h"
#include "raft/consensus.h"
#include "raft/raftgen_service.h"
#include "raft/types.h"
#include "ssx/async_algorithm.h"
#include "ssx/when_all.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/switch_to.hh>

#include <vector>

namespace raft {
// clang-format off
template<typename ConsensusManager>
concept RaftGroupManager = requires(ConsensusManager m, group_id g) {
    { m.consensus_for(g) } -> std::same_as<ss::lw_shared_ptr<consensus>>;
};

template<typename ShardLookup>
concept ShardLookupManager = requires(ShardLookup m, group_id g) {
   { m.shard_for(g) } -> std::same_as<std::optional<ss::shard_id>>;
};

template<typename ConsensusManager, typename ShardLookup>
  requires RaftGroupManager<ConsensusManager>
  && ShardLookupManager<ShardLookup>
// clang-format on
class service final : public raftgen_service {
public:
    using failure_probes = raftgen_service::failure_probes;
    service(
      ss::scheduling_group sc,
      ss::smp_service_group ssg,
      ss::scheduling_group hb_sg,
      ss::sharded<ConsensusManager>& mngr,
      ShardLookup& tbl,
      clock_type::duration heartbeat_interval,
      model::node_id self)
      : raftgen_service(sc, ssg)
      , _hb_sg(hb_sg)
      , _group_manager(mngr)
      , _shard_table(tbl)
      , _heartbeat_interval(heartbeat_interval)
      , _self(self) {
        finjector::shard_local_badger().register_probe(
          failure_probes::name(), &_probe);
    }
    ~service() override {
        finjector::shard_local_badger().deregister_probe(
          failure_probes::name());
    }

    ss::future<heartbeat_reply_v2>
    heartbeat_v2(heartbeat_request_v2 r, rpc::streaming_context&) final {
        co_await ss::coroutine::switch_to(_hb_sg);
        const auto source = r.source();
        const auto target = r.target();
        auto grouped = group_hbeats_by_shard(std::move(r));

        chunked_vector<ss::future<shard_heartbeat_replies>> futures;
        futures.reserve(grouped.shard_requests.size());
        for (auto& [shard, req] : grouped.shard_requests) {
            // dispatch to each core in parallel
            futures.push_back(dispatch_hbeats_to_core_v2(
              shard, source, target, std::move(req)));
        }
        // replies for groups that are not yet registered at this node
        chunked_vector<full_heartbeat_reply> group_missing_replies;
        group_missing_replies.reserve(grouped.group_missing_requests.size());
        std::transform(
          std::begin(grouped.group_missing_requests),
          std::end(grouped.group_missing_requests),
          std::back_inserter(group_missing_replies),
          [](const group_heartbeat& r) {
              return full_heartbeat_reply{
                .group = r.group, .result = reply_result::group_unavailable};
          });

        auto replies = co_await ssx::when_all_succeed<
          chunked_vector<shard_heartbeat_replies>>(std::move(futures));

        heartbeat_reply_v2 reply(_self, source);
        ssx::async_counter cnt;
        // flatten responses
        for (shard_heartbeat_replies& shard_replies : replies) {
            co_await ssx::async_for_each_counter(
              cnt,
              shard_replies.lw_replies.begin(),
              shard_replies.lw_replies.end(),
              [&reply](lw_reply& lw_reply) {
                  reply.add(lw_reply.group, lw_reply.result);
              });
            co_await ssx::async_for_each_counter(
              cnt,
              shard_replies.full_heartbeats.begin(),
              shard_replies.full_heartbeats.end(),
              [&reply](full_heartbeat_reply& full_reply) {
                  reply.add(
                    full_reply.group, full_reply.result, full_reply.data);
              });
        }

        for (auto& m : group_missing_replies) {
            reply.add(m.group, m.result);
        }

        co_return reply;
    }

    [[gnu::always_inline]] ss::future<vote_reply>
    vote(vote_request r, rpc::streaming_context&) final {
        return _probe.vote().then([this, r = std::move(r)]() mutable {
            return dispatch_request(
              std::move(r),
              &service::make_failed_vote_reply,
              [](vote_request&& r, consensus_ptr c) {
                  return c->vote(std::move(r));
              });
        });
    }

    [[gnu::always_inline]] ss::future<append_entries_reply>

    append_entries(append_entries_request r, rpc::streaming_context&) final {
        return _probe.append_entries().then([this, r = std::move(r)]() mutable {
            auto gr = r.target_group();
            return dispatch_request(
              std::move(r),
              [gr]() { return make_missing_group_reply(gr); },
              [](append_entries_request&& r, consensus_ptr c) {
                  return c->append_entries(std::move(r));
              });
        });
    }
    [[gnu::always_inline]] ss::future<append_entries_reply>
    append_entries_full_serde(
      append_entries_request_serde_wrapper r, rpc::streaming_context&) final {
        return _probe.append_entries().then([this, r = std::move(r)]() mutable {
            auto request = std::move(r).release();
            const raft::group_id gr = request.target_group();
            return dispatch_request(
              std::move(request),
              [gr]() { return make_missing_group_reply(gr); },
              [](append_entries_request&& req, consensus_ptr c) {
                  return c->append_entries(std::move(req));
              });
        });
    }

    [[gnu::always_inline]] ss::future<install_snapshot_reply> install_snapshot(
      install_snapshot_request r, rpc::streaming_context&) final {
        return _probe.install_snapshot().then([this,
                                               r = std::move(r)]() mutable {
            return dispatch_request(
              std::move(install_snapshot_request_foreign_wrapper(std::move(r))),
              &service::make_failed_install_snapshot_reply,
              [](
                install_snapshot_request_foreign_wrapper&& r, consensus_ptr c) {
                  return c->install_snapshot(r.copy());
              });
        });
    }

    [[gnu::always_inline]] ss::future<timeout_now_reply>
    timeout_now(timeout_now_request r, rpc::streaming_context&) final {
        return _probe.timeout_now().then([this, r = std::move(r)]() mutable {
            return dispatch_request(
              std::move(r),
              &service::make_failed_timeout_now_reply,
              [](timeout_now_request&& r, consensus_ptr c) {
                  return c->timeout_now(std::move(r));
              });
        });
    }

    [[gnu::always_inline]] ss::future<get_compaction_mcco_reply>
    get_compaction_mcco(
      get_compaction_mcco_request r, rpc::streaming_context&) final {
        return _probe.get_compaction_mcco().then(
          [this, r = std::move(r)]() mutable {
              return dispatch_request(
                std::move(r),
                &service::make_failed_get_compaction_mcco_reply,
                [](get_compaction_mcco_request&& r, consensus_ptr c) {
                    return ssx::now(
                      c->get_compaction_coordinator()
                        .do_get_local_replica_offsets(std::move(r)));
                });
          });
    }

    [[gnu::always_inline]] ss::future<distribute_compaction_mtro_reply>
    distribute_compaction_mtro(
      distribute_compaction_mtro_request r, rpc::streaming_context&) final {
        return _probe.distribute_compaction_mtro().then(
          [this, r = std::move(r)]() mutable {
              return dispatch_request(
                std::move(r),
                &service::make_failed_distribute_compaction_mtro_reply,
                [](distribute_compaction_mtro_request&& r, consensus_ptr c) {
                    return ssx::now(
                      c->get_compaction_coordinator()
                        .do_distribute_group_offsets(std::move(r)));
                });
          });
    }

private:
    using consensus_ptr = seastar::lw_shared_ptr<consensus>;

    struct shard_groupped_hbeat_requests {
        absl::flat_hash_map<ss::shard_id, ss::chunked_fifo<heartbeat_metadata>>
          shard_requests;
        std::vector<heartbeat_metadata> group_missing_requests;
    };
    struct shard_heartbeats {
        ss::chunked_fifo<full_heartbeat> full_heartbeats;
        ss::chunked_fifo<group_id> lw_heartbeats;
    };
    struct lw_reply {
        lw_reply(group_id gr, reply_result result)
          : group(gr)
          , result(result) {}

        group_id group;
        reply_result result;
    };
    struct shard_heartbeat_replies {
        ss::chunked_fifo<full_heartbeat_reply> full_heartbeats;
        ss::chunked_fifo<lw_reply> lw_replies;
    };
    struct shard_groupped_hbeat_requests_v2 {
        absl::flat_hash_map<ss::shard_id, shard_heartbeats> shard_requests;
        chunked_vector<group_heartbeat> group_missing_requests;
    };

    static ss::future<vote_reply> make_failed_vote_reply() {
        return ss::make_ready_future<vote_reply>(vote_reply{
          .term = model::term_id{}, .granted = false, .log_ok = false});
    }

    static ss::future<install_snapshot_reply>
    make_failed_install_snapshot_reply() {
        return ss::make_ready_future<install_snapshot_reply>(
          install_snapshot_reply{
            .term = model::term_id{}, .bytes_stored = 0, .success = false});
    }

    static ss::future<append_entries_reply>
    make_missing_group_reply(raft::group_id group) {
        return ss::make_ready_future<append_entries_reply>(append_entries_reply{
          .group = group, .result = reply_result::group_unavailable});
    }

    static ss::future<timeout_now_reply> make_failed_timeout_now_reply() {
        return ss::make_ready_future<timeout_now_reply>(timeout_now_reply{});
    }

    static ss::future<get_compaction_mcco_reply>
    make_failed_get_compaction_mcco_reply() {
        return ss::make_ready_future<get_compaction_mcco_reply>(
          get_compaction_mcco_reply{});
    }

    static ss::future<distribute_compaction_mtro_reply>
    make_failed_distribute_compaction_mtro_reply() {
        return ss::make_ready_future<distribute_compaction_mtro_reply>(
          distribute_compaction_mtro_reply{});
    }

    template<typename Req, typename ErrorFactory, typename Func>
    auto dispatch_request(Req&& req, ErrorFactory&& ef, Func&& f) {
        const auto group = req.target_group();
        const auto shard = _shard_table.shard_for(group);
        if (unlikely(!shard)) {
            return ef();
        }

        return with_scheduling_group(
          get_scheduling_group(),
          [this,
           shard = *shard,
           r = std::forward<Req>(req),
           f = std::forward<Func>(f),
           ef = std::forward<ErrorFactory>(ef)]() mutable {
              return _group_manager.invoke_on(
                shard,
                get_smp_service_group(),
                [r = std::forward<Req>(r),
                 f = std::forward<Func>(f),
                 ef = std::forward<ErrorFactory>(ef)](
                  ConsensusManager& m) mutable {
                    auto c = m.consensus_for(r.target_group());
                    if (unlikely(!c)) {
                        return ef();
                    }
                    return f(std::forward<Req>(r), c);
                });
          });
    }

    ss::future<std::vector<append_entries_reply>> dispatch_hbeats_to_core(
      ss::shard_id shard, ss::chunked_fifo<heartbeat_metadata> heartbeats) {
        return with_scheduling_group(
          get_scheduling_group(),
          [this, shard, r = std::move(heartbeats)]() mutable {
              return _group_manager.invoke_on(
                shard,
                get_smp_service_group(),
                [this, r = std::move(r)](ConsensusManager& m) mutable {
                    return dispatch_hbeats_to_groups(m, std::move(r));
                });
          });
    }

    ss::future<shard_heartbeat_replies> dispatch_hbeats_to_core_v2(
      ss::shard_id shard,
      model::node_id source_node,
      model::node_id target_node,
      shard_heartbeats heartbeats) {
        return with_scheduling_group(
          get_scheduling_group(),
          [this,
           shard,
           r = std::move(heartbeats),
           source_node,
           target_node]() mutable {
              return _group_manager.invoke_on(
                shard,
                get_smp_service_group(),
                [this, r = std::move(r), source_node, target_node](
                  ConsensusManager& m) mutable {
                    return dispatch_hbeats_to_groups(
                      m, source_node, target_node, std::move(r));
                });
          });
    }

    static append_entries_request
    make_append_entries_request(const heartbeat_metadata& hb) {
        return {
          hb.node_id,
          hb.target_node_id,
          hb.meta,
          {},
          0,
          flush_after_append::no};
    }
    static append_entries_request make_append_entries_request(
      model::node_id source_node,
      model::node_id target_node,
      const group_heartbeat& hb) {
        return {
          raft::vnode(source_node, hb.data->source_revision),
          raft::vnode(target_node, hb.data->target_revision),
          raft::protocol_metadata{
            .group = hb.group,
            .commit_index = hb.data->commit_index,
            .prev_log_index = hb.data->prev_log_index,
            .prev_log_term = hb.data->prev_log_term,
            .last_visible_index = hb.data->last_visible_index,
            // for heartbeats dirty_offset and prev_log_index are always the
            // same
            .dirty_offset = hb.data->prev_log_index,
          },
          {},
          0,
          flush_after_append::no};
    }

    ss::future<std::vector<append_entries_reply>> dispatch_hbeats_to_groups(
      ConsensusManager& m, ss::chunked_fifo<heartbeat_metadata> reqs) {
        std::vector<ss::future<append_entries_reply>> futures;
        futures.reserve(reqs.size());
        // dispatch requests in parallel
        auto timeout = clock_type::now() + _heartbeat_interval;
        std::transform(
          reqs.begin(),
          reqs.end(),
          std::back_inserter(futures),
          [this, &m, timeout](heartbeat_metadata& hb) mutable {
              auto group = hb.meta.group;
              auto f = dispatch_append_entries(
                m, make_append_entries_request(hb));
              return ss::with_timeout(timeout, std::move(f))
                .handle_exception_type([group](const ss::timed_out_error&) {
                    return append_entries_reply{
                      .group = group, .result = reply_result::follower_busy};
                });
          });

        return ss::when_all_succeed(futures.begin(), futures.end());
    }

    ss::future<shard_heartbeat_replies> dispatch_hbeats_to_groups(
      ConsensusManager& m,
      model::node_id source_node,
      model::node_id target_node,
      shard_heartbeats reqs) {
        shard_heartbeat_replies replies;
        replies.lw_replies.reserve(reqs.lw_heartbeats.size());
        replies.full_heartbeats.reserve(reqs.full_heartbeats.size());
        /**
         * Dispatch lightweight heartbeats
         */
        co_await ssx::async_for_each(
          reqs.lw_heartbeats.begin(),
          reqs.lw_heartbeats.end(),
          [&m, &replies, source_node, target_node](group_id gr) {
              auto c = m.consensus_for(gr);
              if (unlikely(!c)) {
                  replies.lw_replies.emplace_back(
                    gr, reply_result::group_unavailable);
                  return;
              }
              auto result = c->lightweight_heartbeat(source_node, target_node);
              replies.lw_replies.emplace_back(gr, result);
          });

        chunked_vector<ss::future<full_heartbeat_reply>> futures;
        const auto timeout = clock_type::now() + _heartbeat_interval;
        futures.reserve(reqs.full_heartbeats.size());

        for (auto& full_hb : reqs.full_heartbeats) {
            auto f = dispatch_full_heartbeat(
              source_node, target_node, m, full_hb);
            f = ss::with_timeout(timeout, std::move(f))
                  .handle_exception_type([group = full_hb.group](
                                           const ss::timed_out_error&) {
                      return full_heartbeat_reply{
                        .group = group, .result = reply_result::follower_busy};
                  });
            futures.push_back(std::move(f));
        }

        auto full_hbs = co_await ss::when_all_succeed(
          futures.begin(), futures.end());

        std::move(
          full_hbs.begin(),
          full_hbs.end(),
          std::back_inserter(replies.full_heartbeats));

        co_return replies;
    }

    shard_groupped_hbeat_requests
    group_hbeats_by_shard(std::vector<heartbeat_metadata> reqs) {
        shard_groupped_hbeat_requests ret;

        for (auto& r : reqs) {
            const auto shard = _shard_table.shard_for(r.meta.group);
            if (unlikely(!shard)) {
                ret.group_missing_requests.push_back(r);
                continue;
            }

            auto [it, _] = ret.shard_requests.try_emplace(*shard);
            it->second.push_back(r);
        }

        return ret;
    }

    shard_groupped_hbeat_requests_v2
    group_hbeats_by_shard(heartbeat_request_v2 hb_request) {
        shard_groupped_hbeat_requests_v2 ret;

        for (const auto& full_beat : hb_request.full_heartbeats()) {
            const auto shard = _shard_table.shard_for(full_beat.group);
            if (unlikely(!shard)) {
                ret.group_missing_requests.push_back(
                  group_heartbeat{
                    .group = full_beat.group, .data = full_beat.data});
                continue;
            }

            auto [it, _] = ret.shard_requests.try_emplace(*shard);
            it->second.full_heartbeats.push_back(
              full_heartbeat{.group = full_beat.group, .data = full_beat.data});
        }
        hb_request.for_each_lw_heartbeat([this, &ret](int64_t group_id) {
            const auto lw_beat = raft::group_id(group_id);
            const auto shard = _shard_table.shard_for(lw_beat);
            if (unlikely(!shard)) {
                ret.group_missing_requests.push_back(
                  group_heartbeat{.group = lw_beat});
                return;
            }

            auto [it, _] = ret.shard_requests.try_emplace(*shard);
            it->second.lw_heartbeats.push_back(lw_beat);
        });

        return ret;
    }

    ss::future<append_entries_reply>
    dispatch_append_entries(ConsensusManager& m, append_entries_request&& r) {
        auto c = m.consensus_for(r.metadata().group);
        if (unlikely(!c)) {
            return make_missing_group_reply(r.metadata().group);
        }
        return c->append_entries(std::move(r));
    }

    ss::future<full_heartbeat_reply> dispatch_full_heartbeat(
      model::node_id source_node,
      model::node_id target_node,
      ConsensusManager& m,
      full_heartbeat req) {
        auto c = m.consensus_for(req.group);
        if (unlikely(!c)) {
            co_return full_heartbeat_reply{
              .group = req.group, .result = reply_result::group_unavailable};
        }
        co_return co_await c->full_heartbeat(
          req.group, source_node, target_node, req.data);
    }
    ss::scheduling_group _hb_sg;
    failure_probes _probe;
    ss::sharded<ConsensusManager>& _group_manager;
    ShardLookup& _shard_table;
    clock_type::duration _heartbeat_interval;
    model::node_id _self;
};
} // namespace raft
