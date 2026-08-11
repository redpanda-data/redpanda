// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/rm_partition_frontend.h"

#include "cluster/controller.h"
#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/rm_stm.h"
#include "cluster/shard_table.h"
#include "cluster/tx_gateway_service.h"
#include "config/configuration.h"
#include "rpc/connection_cache.h"
#include "types.h"

#include <seastar/core/coroutine.hh>

namespace cluster {
using namespace std::chrono_literals;

rm_partition_frontend::rm_partition_frontend(
  ss::smp_service_group ssg,
  ss::sharded<cluster::partition_manager>& partition_manager,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  cluster::controller* controller)
  : _ssg(ssg)
  , _partition_manager(partition_manager)
  , _shard_table(shard_table)
  , _metadata_cache(metadata_cache)
  , _connection_cache(connection_cache)
  , _leaders(leaders)
  , _controller(controller)
  , _metadata_dissemination_retries(
      config::shard_local_cfg().metadata_dissemination_retries.value())
  , _metadata_dissemination_retry_delay_ms(
      config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value()) {
}

bool rm_partition_frontend::is_leader_of(const model::ntp& ntp) const {
    auto leader = _leaders.local().get_leader(ntp);
    if (!leader) {
        return false;
    }
    auto _self = _controller->self();
    return leader == _self;
}

ss::future<begin_tx_reply> rm_partition_frontend::begin_tx(
  model::ntp ntp,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms,
  model::timeout_clock::duration timeout,
  model::partition_id tm) {
    auto nt = model::topic_namespace_view(ntp.ns, ntp.tp.topic);

    if (!_metadata_cache.local().contains(nt, ntp.tp.partition)) {
        vlog(txlog.warn, "can't find {} in the metadata cache", ntp);
        co_return begin_tx_reply{ntp, tx::errc::partition_not_exists};
    }

    if (_metadata_cache.local().is_disabled(nt, ntp.tp.partition)) {
        vlog(txlog.warn, "partition {} is disabled by user", ntp);
        co_return begin_tx_reply{ntp, tx::errc::partition_disabled};
    }

    auto leader_opt = _leaders.local().get_leader(ntp);
    if (!leader_opt) {
        vlog(txlog.warn, "{} is leaderless", ntp);
        co_return begin_tx_reply{ntp, tx::errc::leader_not_found};
    }

    auto leader = leader_opt.value();
    auto _self = _controller->self();

    begin_tx_reply result;
    if (leader == _self) {
        vlog(
          txlog.trace,
          "executing name:begin_tx, ntp:{}, pid:{}, tx_seq:{} "
          "timeout:{}, coordinator: {} locally",
          ntp,
          pid,
          tx_seq,
          transaction_timeout_ms,
          tm);
        result = co_await begin_tx_locally(
          ntp, pid, tx_seq, transaction_timeout_ms, tm);
        vlog(
          txlog.trace,
          "received name:begin_tx, ntp:{}, pid:{}, tx_seq:{}, coordinator: {}, "
          "ec:{}, etag: {} locally",
          ntp,
          pid,
          tx_seq,
          tm,
          result.ec,
          result.etag);
    } else {
        vlog(
          txlog.trace,
          "dispatching name:begin_tx, ntp:{}, pid:{}, "
          "tx_seq:{} timeout:{}, coordinator: {}, "
          "from:{}, "
          "to:{}",
          ntp,
          pid,
          tx_seq,
          transaction_timeout_ms,
          tm,
          _self,
          leader);
        result = co_await dispatch_begin_tx(
          leader, ntp, pid, tx_seq, transaction_timeout_ms, timeout, tm);
        vlog(
          txlog.trace,
          "received name:begin_tx, ntp:{}, pid:{}, tx_seq:{}, coordinator:{}, "
          "ec:{}, etag: {}",
          ntp,
          pid,
          tx_seq,
          tm,
          result.ec,
          result.etag);
    }

    co_return result;
}

ss::future<begin_tx_reply> rm_partition_frontend::dispatch_begin_tx(
  model::node_id leader,
  model::ntp ntp,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms,
  model::timeout_clock::duration timeout,
  model::partition_id tm) {
    return _connection_cache.local()
      .with_node_client<cluster::tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        leader,
        timeout,
        [ntp, pid, tx_seq, transaction_timeout_ms, timeout, tm](
          tx_gateway_client_protocol cp) mutable {
            return cp.begin_tx(
              begin_tx_request{
                std::move(ntp), pid, tx_seq, transaction_timeout_ms, tm},
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<begin_tx_reply>)
      .then([ntp, leader](result<begin_tx_reply> r) {
          if (r.has_error()) {
              vlog(
                txlog.warn,
                "error dispatching begin_tx request for {} against partition "
                "leader: {} - {}",
                ntp,
                leader,
                r.error().message());
              return begin_tx_reply{ntp, tx::errc::timeout};
          }

          return r.value();
      });
}

ss::future<begin_tx_reply> rm_partition_frontend::begin_tx_locally(
  model::ntp ntp,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms,
  model::partition_id tm) {
    vlog(
      txlog.trace,
      "processing name:begin_tx, ntp:{}, pid:{}, tx_seq:{}, coordinator: {}",
      ntp,
      pid,
      tx_seq,
      tm);
    auto reply = co_await do_begin_tx(
      ntp, pid, tx_seq, transaction_timeout_ms, tm);
    vlog(
      txlog.trace,
      "sending name:begin_tx, ntp:{}, pid:{}, tx_seq:{}, coordinator: {}, "
      "ec:{}, etag:{}",
      ntp,
      pid,
      tx_seq,
      tm,
      reply.ec,
      reply.etag);
    co_return reply;
}

ss::future<begin_tx_reply> rm_partition_frontend::do_begin_tx(
  model::ntp ntp,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms,
  model::partition_id tm) {
    if (!is_leader_of(ntp)) {
        return ssx::now(
          begin_tx_reply{std::move(ntp), tx::errc::leader_not_found});
    }

    auto shard = _shard_table.local().shard_for(ntp);
    if (!shard) {
        return ssx::now(
          begin_tx_reply{std::move(ntp), tx::errc::shard_not_found});
    }

    return _partition_manager.invoke_on(
      *shard,
      _ssg,
      [ntp = std::move(ntp), pid, tx_seq, transaction_timeout_ms, tm, this](
        cluster::partition_manager& mgr) mutable {
          return do_begin_tx_on_partition_shard(
            std::move(ntp), pid, tx_seq, transaction_timeout_ms, tm, mgr);
      });
}
namespace {
ss::future<result<ss::rwlock::holder, tx::errc>>
hold_writes_enabled(ss::lw_shared_ptr<partition> partition) {
    auto units_result = co_await partition->hold_writes_enabled();
    if (units_result.has_value()) {
        co_return result<ss::rwlock::holder, tx::errc>{
          std::move(units_result.value())};
    }

    auto err = units_result.error();
    if (err.category() == cluster::error_category()) {
        /**
         * Handle different types of errors that can occur when trying to
         * grab a write enable lock.
         */
        switch (cluster::errc(err.value())) {
        case cluster::errc::not_leader:
            co_return tx::errc::leader_not_found;
        case cluster::errc::resource_is_being_migrated:
            vlog(
              txlog.warn,
              "partition {} is not writable, errc: {}",
              partition->ntp(),
              units_result.error());
            co_return tx::errc::partition_writes_locked;
        case cluster::errc::timeout:
            co_return tx::errc::timeout;
        case cluster::errc::partition_disabled:
            co_return tx::errc::partition_disabled;
        default:
            break;
        }
    } else if (err == raft::errc::not_leader) {
        co_return tx::errc::leader_not_found;
    } else if (err == raft::errc::timeout) {
        co_return tx::errc::timeout;
    }
    vlog(
      txlog.error,
      "error holding a write enable lock for {}, errc: {}",
      partition->ntp(),
      err);
    co_return tx::errc::unknown_server_error;
}
} // namespace

ss::future<begin_tx_reply>
rm_partition_frontend::do_begin_tx_on_partition_shard(
  model::ntp ntp,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms,
  model::partition_id tm,
  cluster::partition_manager& mgr) {
    auto partition = mgr.get(ntp);
    if (!partition) {
        co_return begin_tx_reply{std::move(ntp), tx::errc::partition_not_found};
    }

    auto maybe_partition_units = co_await hold_writes_enabled(partition);
    if (!maybe_partition_units.has_value()) {
        co_return begin_tx_reply{std::move(ntp), maybe_partition_units.error()};
    }

    auto stm = partition->rm_stm();
    if (!stm) {
        vlog(txlog.warn, "partition {} doesn't have rm_stm", ntp);
        co_return begin_tx_reply{std::move(ntp), tx::errc::stm_not_found};
    }

    auto topic_md = _metadata_cache.local().get_topic_metadata_ref(
      model::topic_namespace_view(ntp));
    if (!topic_md) {
        co_return begin_tx_reply{
          std::move(ntp), tx::errc::partition_not_exists};
    }
    auto topic_revision = topic_md->get().get_revision();

    auto etag = co_await stm->begin_tx(pid, tx_seq, transaction_timeout_ms, tm);
    if (!etag.has_value()) {
        co_return begin_tx_reply{std::move(ntp), etag.error()};
    }
    co_return begin_tx_reply{
      std::move(ntp), etag.value(), tx::errc::none, topic_revision};
}

ss::future<commit_tx_reply> rm_partition_frontend::commit_tx(
  model::ntp ntp,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    auto nt = model::topic_namespace(ntp.ns, ntp.tp.topic);

    if (!_metadata_cache.local().contains(nt, ntp.tp.partition)) {
        return ss::make_ready_future<commit_tx_reply>(
          commit_tx_reply{tx::errc::partition_not_exists});
    }

    if (_metadata_cache.local().is_disabled(nt, ntp.tp.partition)) {
        return ss::make_ready_future<commit_tx_reply>(
          commit_tx_reply{tx::errc::partition_disabled});
    }

    auto leader = _leaders.local().get_leader(ntp);
    if (!leader) {
        vlog(txlog.warn, "can't find a leader for {} pid:{}", ntp, pid);
        return ss::make_ready_future<commit_tx_reply>(
          commit_tx_reply{tx::errc::leader_not_found});
    }

    auto _self = _controller->self();

    if (leader == _self) {
        return commit_tx_locally(ntp, pid, tx_seq, timeout);
    }

    vlog(
      txlog.trace,
      "dispatching name:commit_tx, ntp:{}, pid:{}, tx_seq:{}, from:{}, to:{}",
      ntp,
      pid,
      tx_seq,
      _self,
      leader);

    return dispatch_commit_tx(leader.value(), ntp, pid, tx_seq, timeout)
      .then([ntp, pid, tx_seq](commit_tx_reply reply) {
          vlog(
            txlog.trace,
            "received name:commit_tx, ntp:{}, pid:{}, tx_seq:{}, ec:{}",
            ntp,
            pid,
            tx_seq,
            reply.ec);
          return reply;
      });
}

ss::future<commit_tx_reply> rm_partition_frontend::dispatch_commit_tx(
  model::node_id leader,
  model::ntp ntp,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    return _connection_cache.local()
      .with_node_client<cluster::tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        leader,
        timeout,
        [ntp, pid, tx_seq, timeout](tx_gateway_client_protocol cp) mutable {
            return cp.commit_tx(
              commit_tx_request{std::move(ntp), pid, tx_seq, timeout},
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<commit_tx_reply>)
      .then([ntp, leader](result<commit_tx_reply> r) {
          if (r.has_error()) {
              vlog(
                txlog.warn,
                "error dispatching commit_tx request for {} against partition "
                "leader: {} - {}",
                ntp,
                leader,
                r.error().message());
              return commit_tx_reply{tx::errc::timeout};
          }

          return r.value();
      });
}

ss::future<commit_tx_reply> rm_partition_frontend::commit_tx_locally(
  model::ntp ntp,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    vlog(
      txlog.trace,
      "processing name:commit_tx, ntp:{}, pid:{}, tx_seq:{}",
      ntp,
      pid,
      tx_seq);
    auto reply = co_await do_commit_tx(ntp, pid, tx_seq, timeout);
    vlog(
      txlog.trace,
      "sending name:commit_tx, ntp:{}, pid:{}, tx_seq:{}, ec:{}",
      ntp,
      pid,
      tx_seq,
      reply.ec);
    co_return reply;
}

ss::future<commit_tx_reply> rm_partition_frontend::do_commit_tx(
  model::ntp ntp,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    if (!is_leader_of(ntp)) {
        return ss::make_ready_future<commit_tx_reply>(
          commit_tx_reply{tx::errc::leader_not_found});
    }

    auto shard = _shard_table.local().shard_for(ntp);

    if (!shard) {
        return ss::make_ready_future<commit_tx_reply>(
          commit_tx_reply{tx::errc::shard_not_found});
    }

    return _partition_manager.invoke_on(
      *shard,
      _ssg,
      [pid, ntp, tx_seq, timeout](cluster::partition_manager& mgr) mutable {
          auto partition = mgr.get(ntp);
          if (!partition) {
              return ss::make_ready_future<commit_tx_reply>(
                commit_tx_reply{tx::errc::partition_not_found});
          }

          auto stm = partition->rm_stm();

          if (!stm) {
              vlog(txlog.warn, "can't get tx stm of the {}' partition", ntp);
              return ss::make_ready_future<commit_tx_reply>(
                commit_tx_reply{tx::errc::stm_not_found});
          }

          return stm->commit_tx(pid, tx_seq, timeout).then([](tx::errc ec) {
              return commit_tx_reply{ec};
          });
      });
}

ss::future<abort_tx_reply> rm_partition_frontend::abort_tx(
  model::ntp ntp,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    auto nt = model::topic_namespace(ntp.ns, ntp.tp.topic);

    if (!_metadata_cache.local().contains(nt, ntp.tp.partition)) {
        return ss::make_ready_future<abort_tx_reply>(
          abort_tx_reply{tx::errc::partition_not_exists});
    }

    if (_metadata_cache.local().is_disabled(nt, ntp.tp.partition)) {
        return ss::make_ready_future<abort_tx_reply>(
          abort_tx_reply{tx::errc::partition_disabled});
    }

    auto leader = _leaders.local().get_leader(ntp);
    if (!leader) {
        vlog(txlog.warn, "can't find a leader for {}", ntp);
        return ss::make_ready_future<abort_tx_reply>(
          abort_tx_reply{tx::errc::leader_not_found});
    }

    auto _self = _controller->self();

    if (leader == _self) {
        return abort_tx_locally(ntp, pid, tx_seq, timeout);
    }

    vlog(
      txlog.trace,
      "dispatching name:abort_tx, ntp:{}, pid:{}, tx_seq:{}, from:{}, to:{}",
      ntp,
      pid,
      tx_seq,
      _self,
      leader);

    return dispatch_abort_tx(leader.value(), ntp, pid, tx_seq, timeout)
      .then([ntp, pid, tx_seq](abort_tx_reply reply) {
          vlog(
            txlog.trace,
            "received name:abort_tx, ntp:{}, pid:{}, tx_seq:{}, ec:{}",
            ntp,
            pid,
            tx_seq,
            reply.ec);
          return reply;
      });
}

ss::future<abort_tx_reply> rm_partition_frontend::dispatch_abort_tx(
  model::node_id leader,
  model::ntp ntp,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    return _connection_cache.local()
      .with_node_client<cluster::tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        leader,
        timeout,
        [ntp, pid, tx_seq, timeout](tx_gateway_client_protocol cp) mutable {
            return cp.abort_tx(
              abort_tx_request{std::move(ntp), pid, tx_seq, timeout},
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<abort_tx_reply>)
      .then([ntp, leader](result<abort_tx_reply> r) {
          if (r.has_error()) {
              vlog(
                txlog.warn,
                "error dispatching commit_tx request for {} against partition "
                "leader: {} - {}",
                ntp,
                leader,
                r.error().message());
              return abort_tx_reply{tx::errc::timeout};
          }

          return r.value();
      });
}

ss::future<abort_tx_reply> rm_partition_frontend::abort_tx_locally(
  model::ntp ntp,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    vlog(
      txlog.trace,
      "processing name:abort_tx, ntp:{}, pid:{}, tx_seq:{}",
      ntp,
      pid,
      tx_seq);
    auto reply = co_await do_abort_tx(ntp, pid, tx_seq, timeout);
    vlog(
      txlog.trace,
      "sending name:abort_tx, ntp:{}, pid:{}, tx_seq:{}, ec:{}",
      ntp,
      pid,
      tx_seq,
      reply.ec);
    co_return reply;
}

ss::future<abort_tx_reply> rm_partition_frontend::do_abort_tx(
  model::ntp ntp,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    if (!is_leader_of(ntp)) {
        return ss::make_ready_future<abort_tx_reply>(
          abort_tx_reply{tx::errc::leader_not_found});
    }

    auto shard = _shard_table.local().shard_for(ntp);

    if (!shard) {
        return ss::make_ready_future<abort_tx_reply>(
          abort_tx_reply{tx::errc::shard_not_found});
    }

    return _partition_manager.invoke_on(
      *shard,
      _ssg,
      [pid, ntp, tx_seq, timeout](cluster::partition_manager& mgr) mutable {
          auto partition = mgr.get(ntp);
          if (!partition) {
              return ss::make_ready_future<abort_tx_reply>(
                abort_tx_reply{tx::errc::partition_not_found});
          }

          auto stm = partition->rm_stm();

          if (!stm) {
              vlog(txlog.warn, "can't get tx stm of the {}' partition", ntp);
              return ss::make_ready_future<abort_tx_reply>(
                abort_tx_reply{tx::errc::stm_not_found});
          }

          return stm->abort_tx(pid, tx_seq, timeout).then([](tx::errc ec) {
              return cluster::abort_tx_reply{ec};
          });
      });
}

ss::future<get_producers_reply>
rm_partition_frontend::get_producers_locally(get_producers_request request) {
    get_producers_reply reply;
    auto partition = _partition_manager.local().get(request.ntp);
    if (!partition || !partition->is_leader()) {
        reply.error_code = tx::errc::not_coordinator;
        co_return reply;
    }
    reply.error_code = tx::errc::none;
    auto stm = partition->raft()->stm_manager()->get<rm_stm>();
    if (!stm) {
        // maybe an internal (non data) partition
        co_return reply;
    }
    const auto& producers = stm->get_producers();
    reply.producer_count = producers.size();
    for (const auto& [pid, state] : producers) {
        producer_state_info producer_info;
        producer_info.pid = state->id();
        // fill in the idempotent producer state.
        const auto& requests = state->idempotent_request_state();
        for (const auto& request : requests.inflight_requests()) {
            idempotent_request_info request_info;
            request_info.first_sequence = request->first_sequence();
            request_info.last_sequence = request->last_sequence();
            request_info.term = request->term();
            producer_info.inflight_requests.push_back(std::move(request_info));
        }

        for (const auto& request : requests.finished_requests()) {
            idempotent_request_info request_info;
            request_info.first_sequence = request->first_sequence();
            request_info.last_sequence = request->last_sequence();
            request_info.term = request->term();
            producer_info.finished_requests.push_back(std::move(request_info));
        }
        producer_info.last_update = state->last_update_timestamp();

        // Fill in transactional producer state, if any.
        const auto& tx_state = state->transaction_state();
        if (state->has_transaction_in_progress() && tx_state) {
            producer_info.tx_begin_offset = tx_state->first;
            producer_info.tx_end_offset = tx_state->last;
            producer_info.tx_seq = tx_state->sequence;
            producer_info.tx_timeout = tx_state->timeout;
            producer_info.coordinator_partition
              = tx_state->coordinator_partition;
        }
        reply.producers.push_back(std::move(producer_info));
        if (reply.producers.size() > request.max_producers_to_include) {
            break;
        }
    }
    co_return reply;
}

} // namespace cluster
