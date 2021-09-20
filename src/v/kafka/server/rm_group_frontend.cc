// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/rm_group_frontend.h"

#include "cluster/controller.h"
#include "cluster/id_allocator_frontend.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/tm_stm.h"
#include "cluster/topics_frontend.h"
#include "cluster/tx_gateway.h"
#include "config/configuration.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/group.h"
#include "kafka/server/group_router.h"
#include "kafka/server/logger.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>

namespace kafka {
using namespace std::chrono_literals;

/*
 * create the internal metadata topic for group membership
 */
ss::future<bool> try_create_consumer_group_topic(
  kafka::coordinator_ntp_mapper& mapper,
  cluster::topics_frontend& topics_frontend) {
    // the new internal metadata topic for group membership
    cluster::topic_configuration topic{
      mapper.ns(),
      mapper.topic(),
      config::shard_local_cfg().group_topic_partitions(),
      config::shard_local_cfg().default_topic_replication()};

    topic.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;

    return topics_frontend
      .autocreate_topics(
        {std::move(topic)}, config::shard_local_cfg().create_topic_timeout_ms())
      .then([](std::vector<cluster::topic_result> res) {
          /*
           * kindly ask client to retry on error
           */
          vassert(res.size() == 1, "expected exactly one result");
          if (res[0].ec != cluster::errc::success) {
              return false;
          }
          return true;
      })
      .handle_exception([]([[maybe_unused]] std::exception_ptr e) {
          // various errors may returned such as a timeout, or if the
          // controller group doesn't have a leader. client will retry.
          return false;
      });
}

rm_group_frontend::rm_group_frontend(
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<cluster::partition_leaders_table>& leaders,
  cluster::controller* controller,
  ss::sharded<kafka::coordinator_ntp_mapper>& coordinator_mapper,
  ss::sharded<kafka::group_router>& group_router)
  : _metadata_cache(metadata_cache)
  , _connection_cache(connection_cache)
  , _leaders(leaders)
  , _controller(controller)
  , _coordinator_mapper(coordinator_mapper)
  , _group_router(group_router)
  , _metadata_dissemination_retries(
      config::shard_local_cfg().metadata_dissemination_retries.value())
  , _metadata_dissemination_retry_delay_ms(
      config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value()) {
}

ss::future<cluster::begin_group_tx_reply> rm_group_frontend::begin_group_tx(
  kafka::group_id group_id,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    auto ntp_opt = _coordinator_mapper.local().ntp_for(group_id);
    if (!ntp_opt) {
        vlog(
          klog.trace,
          "can't find ntp for {}, creating a consumer group topic",
          group_id);
        auto has_created = co_await try_create_consumer_group_topic(
          _coordinator_mapper.local(),
          _controller->get_topics_frontend().local());
        if (!has_created) {
            vlog(klog.warn, "can't create consumer group topic", group_id);
            co_return cluster::begin_group_tx_reply{
              .ec = cluster::tx_errc::partition_not_exists};
        }
    }

    auto retries = _metadata_dissemination_retries;
    auto delay_ms = _metadata_dissemination_retry_delay_ms;
    std::optional<model::node_id> leader_opt;
    cluster::tx_errc ec;
    while (!leader_opt && 0 < retries--) {
        ntp_opt = _coordinator_mapper.local().ntp_for(group_id);
        if (!ntp_opt) {
            vlog(klog.trace, "can't find ntp for {}, retrying", group_id);
            ec = cluster::tx_errc::partition_not_exists;
            co_await ss::sleep(delay_ms);
            continue;
        }

        auto ntp = std::move(ntp_opt.value());
        auto nt = model::topic_namespace_view(ntp.ns, ntp.tp.topic);
        if (!_metadata_cache.local().contains(nt, ntp.tp.partition)) {
            vlog(klog.trace, "can't find meta info for {}, retrying", ntp);
            ec = cluster::tx_errc::partition_not_exists;
            co_await ss::sleep(delay_ms);
            continue;
        }

        leader_opt = _leaders.local().get_leader(ntp);
        if (!leader_opt) {
            vlog(klog.trace, "can't find a leader for {}", ntp);
            ec = cluster::tx_errc::leader_not_found;
            co_await ss::sleep(delay_ms);
        }
    }

    if (!leader_opt) {
        co_return cluster::begin_group_tx_reply{.ec = ec};
    }

    auto leader = leader_opt.value();
    auto _self = _controller->self();

    if (leader == _self) {
        cluster::begin_group_tx_request req;
        req.group_id = group_id;
        req.pid = pid;
        req.tx_seq = tx_seq;
        req.timeout = timeout;
        co_return co_await begin_group_tx_locally(std::move(req));
    }

    vlog(klog.trace, "dispatching begin group tx to {} from {}", leader, _self);
    co_return co_await dispatch_begin_group_tx(
      leader, group_id, pid, tx_seq, timeout);
}

ss::future<cluster::begin_group_tx_reply>
rm_group_frontend::dispatch_begin_group_tx(
  model::node_id leader,
  kafka::group_id group_id,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    return _connection_cache.local()
      .with_node_client<cluster::tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        leader,
        timeout,
        [group_id, pid, tx_seq, timeout](
          cluster::tx_gateway_client_protocol cp) {
            return cp.begin_group_tx(
              cluster::begin_group_tx_request{
                .group_id = group_id,
                .pid = pid,
                .tx_seq = tx_seq,
                .timeout = timeout},
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<cluster::begin_group_tx_reply>)
      .then([](result<cluster::begin_group_tx_reply> r) {
          if (r.has_error()) {
              vlog(
                klog.warn, "got error {} on remote begin group tx", r.error());
              return cluster::begin_group_tx_reply{
                .ec = cluster::tx_errc::timeout};
          }

          return r.value();
      });
}

ss::future<cluster::begin_group_tx_reply>
rm_group_frontend::begin_group_tx_locally(cluster::begin_group_tx_request req) {
    return _group_router.local().begin_tx(std::move(req));
}

ss::future<cluster::prepare_group_tx_reply> rm_group_frontend::prepare_group_tx(
  kafka::group_id group_id,
  model::term_id etag,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    auto ntp_opt = _coordinator_mapper.local().ntp_for(group_id);
    if (!ntp_opt) {
        vlog(klog.warn, "can't find ntp for {} ", group_id);
        co_return cluster::prepare_group_tx_reply{
          .ec = cluster::tx_errc::partition_not_exists};
    }
    auto ntp = std::move(ntp_opt.value());

    auto nt = model::topic_namespace(ntp.ns, ntp.tp.topic);
    if (!_metadata_cache.local().contains(nt, ntp.tp.partition)) {
        vlog(klog.warn, "can't find meta info for {}", ntp);
        co_return cluster::prepare_group_tx_reply{
          .ec = cluster::tx_errc::partition_not_exists};
    }

    auto leader_opt = _leaders.local().get_leader(ntp);
    if (!leader_opt) {
        vlog(klog.warn, "can't find a leader for {}", ntp);
        co_return cluster::prepare_group_tx_reply{
          .ec = cluster::tx_errc::leader_not_found};
    }
    auto leader = leader_opt.value();
    auto _self = _controller->self();

    if (leader == _self) {
        cluster::prepare_group_tx_request req;
        req.group_id = group_id;
        req.etag = etag;
        req.pid = pid;
        req.tx_seq = tx_seq;
        req.timeout = timeout;
        co_return co_await prepare_group_tx_locally(std::move(req));
    }

    vlog(klog.trace, "dispatching begin group tx to {} from {}", leader, _self);
    co_return co_await dispatch_prepare_group_tx(
      leader, group_id, etag, pid, tx_seq, timeout);
}

ss::future<cluster::prepare_group_tx_reply>
rm_group_frontend::dispatch_prepare_group_tx(
  model::node_id leader,
  kafka::group_id group_id,
  model::term_id etag,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    return _connection_cache.local()
      .with_node_client<cluster::tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        leader,
        timeout,
        [group_id, etag, pid, tx_seq, timeout](
          cluster::tx_gateway_client_protocol cp) {
            return cp.prepare_group_tx(
              cluster::prepare_group_tx_request{
                .group_id = group_id,
                .etag = etag,
                .pid = pid,
                .tx_seq = tx_seq,
                .timeout = timeout},
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<cluster::prepare_group_tx_reply>)
      .then([](result<cluster::prepare_group_tx_reply> r) {
          if (r.has_error()) {
              vlog(
                klog.warn,
                "got error {} on remote prepare group tx",
                r.error());
              return cluster::prepare_group_tx_reply{
                .ec = cluster::tx_errc::timeout};
          }

          return r.value();
      });
}

ss::future<cluster::prepare_group_tx_reply>
rm_group_frontend::prepare_group_tx_locally(
  cluster::prepare_group_tx_request req) {
    return _group_router.local().prepare_tx(std::move(req));
}

ss::future<cluster::commit_group_tx_reply> rm_group_frontend::commit_group_tx(
  kafka::group_id group_id,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    auto ntp_opt = _coordinator_mapper.local().ntp_for(group_id);
    if (!ntp_opt) {
        vlog(klog.warn, "can't find ntp for {}", group_id);
        co_return cluster::commit_group_tx_reply{
          .ec = cluster::tx_errc::partition_not_exists};
    }

    auto ntp = std::move(ntp_opt.value());

    auto nt = model::topic_namespace(ntp.ns, ntp.tp.topic);
    if (!_metadata_cache.local().contains(nt, ntp.tp.partition)) {
        vlog(klog.warn, "can' find meta info for {}", ntp);
        co_return cluster::commit_group_tx_reply{
          .ec = cluster::tx_errc::partition_not_exists};
    }

    auto leader_opt = _leaders.local().get_leader(ntp);
    if (!leader_opt) {
        vlog(klog.warn, "can't find a leader for {}", ntp);
        co_return cluster::commit_group_tx_reply{
          .ec = cluster::tx_errc::leader_not_found};
    }
    auto leader = leader_opt.value();
    auto _self = _controller->self();

    if (leader == _self) {
        cluster::commit_group_tx_request req;
        req.group_id = group_id;
        req.pid = pid;
        req.tx_seq = tx_seq;
        req.timeout = timeout;
        co_return co_await commit_group_tx_locally(std::move(req));
    }

    vlog(
      klog.trace, "dispatching commit group tx to {} from {}", leader, _self);
    co_return co_await dispatch_commit_group_tx(
      leader, group_id, pid, tx_seq, timeout);
}

ss::future<cluster::commit_group_tx_reply>
rm_group_frontend::dispatch_commit_group_tx(
  model::node_id leader,
  kafka::group_id group_id,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    return _connection_cache.local()
      .with_node_client<cluster::tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        leader,
        timeout,
        [group_id, pid, tx_seq, timeout](
          cluster::tx_gateway_client_protocol cp) {
            return cp.commit_group_tx(
              cluster::commit_group_tx_request{
                .pid = pid,
                .tx_seq = tx_seq,
                .group_id = group_id,
                .timeout = timeout},
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<cluster::commit_group_tx_reply>)
      .then([](result<cluster::commit_group_tx_reply> r) {
          if (r.has_error()) {
              vlog(klog.warn, "got error {} on remote commit tx", r.error());
              return cluster::commit_group_tx_reply{
                .ec = cluster::tx_errc::timeout};
          }

          return r.value();
      });
}

ss::future<cluster::commit_group_tx_reply>
rm_group_frontend::commit_group_tx_locally(
  cluster::commit_group_tx_request req) {
    return _group_router.local().commit_tx(std::move(req));
}

ss::future<cluster::abort_group_tx_reply> rm_group_frontend::abort_group_tx(
  kafka::group_id group_id,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    auto ntp_opt = _coordinator_mapper.local().ntp_for(group_id);
    if (!ntp_opt) {
        vlog(klog.warn, "can't find ntp for {} ", group_id);
        co_return cluster::abort_group_tx_reply{
          .ec = cluster::tx_errc::partition_not_exists};
    }
    auto ntp = std::move(ntp_opt.value());

    auto nt = model::topic_namespace(ntp.ns, ntp.tp.topic);
    if (!_metadata_cache.local().contains(nt, ntp.tp.partition)) {
        vlog(klog.warn, "can't find meta info for {}", ntp);
        co_return cluster::abort_group_tx_reply{
          .ec = cluster::tx_errc::partition_not_exists};
    }

    auto leader_opt = _leaders.local().get_leader(ntp);
    if (!leader_opt) {
        vlog(klog.warn, "can't find a leader for {}", ntp);
        co_return cluster::abort_group_tx_reply{
          .ec = cluster::tx_errc::leader_not_found};
    }
    auto leader = leader_opt.value();
    auto _self = _controller->self();

    if (leader == _self) {
        cluster::abort_group_tx_request req;
        req.group_id = group_id;
        req.pid = pid;
        req.tx_seq = tx_seq;
        req.timeout = timeout;
        co_return co_await abort_group_tx_locally(std::move(req));
    }

    vlog(klog.trace, "dispatching abort group tx to {} from {}", leader, _self);
    co_return co_await dispatch_abort_group_tx(
      leader, group_id, pid, tx_seq, timeout);
}

ss::future<cluster::abort_group_tx_reply>
rm_group_frontend::dispatch_abort_group_tx(
  model::node_id leader,
  kafka::group_id group_id,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    return _connection_cache.local()
      .with_node_client<cluster::tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        leader,
        timeout,
        [group_id, pid, tx_seq, timeout](
          cluster::tx_gateway_client_protocol cp) {
            return cp.abort_group_tx(
              cluster::abort_group_tx_request{
                .group_id = group_id,
                .pid = pid,
                .tx_seq = tx_seq,
                .timeout = timeout},
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<cluster::abort_group_tx_reply>)
      .then([](result<cluster::abort_group_tx_reply> r) {
          if (r.has_error()) {
              vlog(
                klog.warn, "got error {} on remote abort group tx", r.error());
              return cluster::abort_group_tx_reply{
                .ec = cluster::tx_errc::timeout};
          }

          return r.value();
      });
}

ss::future<cluster::abort_group_tx_reply>
rm_group_frontend::abort_group_tx_locally(cluster::abort_group_tx_request req) {
    return _group_router.local().abort_tx(std::move(req));
}

} // namespace kafka
