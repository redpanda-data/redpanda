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
#include "cluster/shard_table.h"
#include "cluster/tx_gateway_service.h"
#include "cluster/tx_helpers.h"
#include "config/configuration.h"
#include "errc.h"
#include "rpc/connection_cache.h"
#include "types.h"
#include "vformat.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>

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
        co_return begin_tx_reply{ntp, tx_errc::partition_not_exists};
    }

    auto leader_opt = _leaders.local().get_leader(ntp);
    if (!leader_opt) {
        vlog(txlog.warn, "{} is leaderless", ntp);
        co_return begin_tx_reply{ntp, tx_errc::leader_not_found};
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
          tx_gateway_client_protocol cp) {
            return cp.begin_tx(
              begin_tx_request{ntp, pid, tx_seq, transaction_timeout_ms, tm},
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<begin_tx_reply>)
      .then([ntp](result<begin_tx_reply> r) {
          if (r.has_error()) {
              vlog(txlog.warn, "got error {} on remote begin tx", r.error());
              return begin_tx_reply{ntp, tx_errc::timeout};
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
        return ss::make_ready_future<begin_tx_reply>(
          begin_tx_reply{ntp, tx_errc::leader_not_found});
    }

    auto shard = _shard_table.local().shard_for(ntp);

    if (!shard) {
        return ss::make_ready_future<begin_tx_reply>(
          begin_tx_reply{ntp, tx_errc::shard_not_found});
    }

    return _partition_manager.invoke_on(
      *shard,
      _ssg,
      [ntp, pid, tx_seq, transaction_timeout_ms, tm, this](
        cluster::partition_manager& mgr) mutable {
          auto partition = mgr.get(ntp);
          if (!partition) {
              return ss::make_ready_future<begin_tx_reply>(
                begin_tx_reply{ntp, tx_errc::partition_not_found});
          }

          auto stm = partition->rm_stm();

          if (!stm) {
              vlog(txlog.warn, "partition {} doesn't have rm_stm", ntp);
              return ss::make_ready_future<begin_tx_reply>(
                begin_tx_reply{ntp, tx_errc::stm_not_found});
          }

          auto topic_md = _metadata_cache.local().get_topic_metadata(
            model::topic_namespace_view(ntp));
          if (!topic_md) {
              return ss::make_ready_future<begin_tx_reply>(
                begin_tx_reply{ntp, tx_errc::partition_not_exists});
          }
          auto topic_revision = topic_md->get_revision();

          return stm->begin_tx(pid, tx_seq, transaction_timeout_ms, tm)
            .then([ntp, topic_revision](checked<model::term_id, tx_errc> etag) {
                if (!etag.has_value()) {
                    return begin_tx_reply{ntp, etag.error()};
                }
                return begin_tx_reply{
                  ntp, etag.value(), tx_errc::none, topic_revision};
            });
      });
}

ss::future<prepare_tx_reply> rm_partition_frontend::prepare_tx(
  model::ntp ntp,
  model::term_id etag,
  model::partition_id tm,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    auto nt = model::topic_namespace(ntp.ns, ntp.tp.topic);

    if (!_metadata_cache.local().contains(nt, ntp.tp.partition)) {
        return ss::make_ready_future<prepare_tx_reply>(
          prepare_tx_reply{tx_errc::partition_not_exists});
    }

    auto leader = _leaders.local().get_leader(ntp);
    if (!leader) {
        vlog(txlog.warn, "can't find a leader for {}", ntp);
        return ss::make_ready_future<prepare_tx_reply>(
          prepare_tx_reply{tx_errc::leader_not_found});
    }

    auto _self = _controller->self();

    if (leader == _self) {
        return prepare_tx_locally(ntp, etag, tm, pid, tx_seq, timeout);
    }

    vlog(
      txlog.trace,
      "dispatching name:prepare_tx, ntp:{}, etag:{}, pid:{}, tx_seq:{}, "
      "coordinator:{}, from:{}, to:{}",
      ntp,
      etag,
      pid,
      tx_seq,
      tm,
      _self,
      leader);

    return dispatch_prepare_tx(
             leader.value(), ntp, etag, tm, pid, tx_seq, timeout)
      .then([ntp, etag, tm, pid, tx_seq](prepare_tx_reply reply) {
          vlog(
            txlog.trace,
            "received name:prepare_tx, ntp:{}, etag:{}, pid:{}, tx_seq:{}, "
            "coordinator:{}, ec:{}",
            ntp,
            etag,
            pid,
            tx_seq,
            tm,
            reply.ec);
          return reply;
      });
}

ss::future<prepare_tx_reply> rm_partition_frontend::dispatch_prepare_tx(
  model::node_id leader,
  model::ntp ntp,
  model::term_id etag,
  model::partition_id tm,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    return _connection_cache.local()
      .with_node_client<cluster::tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        leader,
        timeout,
        [ntp, etag, tm, pid, tx_seq, timeout](tx_gateway_client_protocol cp) {
            return cp.prepare_tx(
              prepare_tx_request{ntp, etag, tm, pid, tx_seq, timeout},
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<prepare_tx_reply>)
      .then([](result<prepare_tx_reply> r) {
          if (r.has_error()) {
              vlog(txlog.warn, "got error {} on remote prepare tx", r.error());
              return prepare_tx_reply{tx_errc::timeout};
          }

          return r.value();
      });
}

ss::future<prepare_tx_reply> rm_partition_frontend::prepare_tx_locally(
  model::ntp ntp,
  model::term_id etag,
  model::partition_id tm,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    vlog(
      txlog.trace,
      "processing name:prepare_tx, ntp:{}, etag:{}, pid:{}, tx_seq:{}, "
      "coordinator:{}",
      ntp,
      etag,
      pid,
      tx_seq,
      tm);
    auto reply = co_await do_prepare_tx(ntp, etag, tm, pid, tx_seq, timeout);
    vlog(
      txlog.trace,
      "sending name:prepare_tx, ntp:{}, etag:{}, pid:{}, tx_seq:{}, "
      "coordinator:{}, ec:{}",
      ntp,
      etag,
      pid,
      tx_seq,
      tm,
      reply.ec);
    co_return reply;
}

ss::future<prepare_tx_reply> rm_partition_frontend::do_prepare_tx(
  model::ntp ntp,
  model::term_id etag,
  model::partition_id tm,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    if (!is_leader_of(ntp)) {
        return ss::make_ready_future<prepare_tx_reply>(
          prepare_tx_reply{tx_errc::leader_not_found});
    }

    auto shard = _shard_table.local().shard_for(ntp);

    if (!shard) {
        return ss::make_ready_future<prepare_tx_reply>(
          prepare_tx_reply{tx_errc::shard_not_found});
    }

    return _partition_manager.invoke_on(
      *shard,
      _ssg,
      [ntp, etag, tm, pid, tx_seq, timeout](
        cluster::partition_manager& mgr) mutable {
          auto partition = mgr.get(ntp);
          if (!partition) {
              return ss::make_ready_future<prepare_tx_reply>(
                prepare_tx_reply{tx_errc::partition_not_found});
          }

          auto stm = partition->rm_stm();

          if (!stm) {
              vlog(txlog.warn, "can't get tx stm of the {}' partition", ntp);
              return ss::make_ready_future<prepare_tx_reply>(
                prepare_tx_reply{tx_errc::stm_not_found});
          }

          return stm->prepare_tx(etag, tm, pid, tx_seq, timeout)
            .then([](tx_errc ec) { return prepare_tx_reply{ec}; });
      });
}

ss::future<commit_tx_reply> rm_partition_frontend::commit_tx(
  model::ntp ntp,
  model::producer_identity pid,
  model::tx_seq tx_seq,
  model::timeout_clock::duration timeout) {
    auto nt = model::topic_namespace(ntp.ns, ntp.tp.topic);

    if (!_metadata_cache.local().contains(nt, ntp.tp.partition)) {
        return ss::make_ready_future<commit_tx_reply>(
          commit_tx_reply{tx_errc::partition_not_exists});
    }

    auto leader = _leaders.local().get_leader(ntp);
    if (!leader) {
        vlog(txlog.warn, "can't find a leader for {} pid:{}", ntp, pid);
        return ss::make_ready_future<commit_tx_reply>(
          commit_tx_reply{tx_errc::leader_not_found});
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
        [ntp, pid, tx_seq, timeout](tx_gateway_client_protocol cp) {
            return cp.commit_tx(
              commit_tx_request{ntp, pid, tx_seq, timeout},
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<commit_tx_reply>)
      .then([](result<commit_tx_reply> r) {
          if (r.has_error()) {
              vlog(txlog.warn, "got error {} on remote commit tx", r.error());
              return commit_tx_reply{tx_errc::timeout};
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
          commit_tx_reply{tx_errc::leader_not_found});
    }

    auto shard = _shard_table.local().shard_for(ntp);

    if (!shard) {
        return ss::make_ready_future<commit_tx_reply>(
          commit_tx_reply{tx_errc::shard_not_found});
    }

    return _partition_manager.invoke_on(
      *shard,
      _ssg,
      [pid, ntp, tx_seq, timeout](cluster::partition_manager& mgr) mutable {
          auto partition = mgr.get(ntp);
          if (!partition) {
              return ss::make_ready_future<commit_tx_reply>(
                commit_tx_reply{tx_errc::partition_not_found});
          }

          auto stm = partition->rm_stm();

          if (!stm) {
              vlog(txlog.warn, "can't get tx stm of the {}' partition", ntp);
              return ss::make_ready_future<commit_tx_reply>(
                commit_tx_reply{tx_errc::stm_not_found});
          }

          return stm->commit_tx(pid, tx_seq, timeout).then([](tx_errc ec) {
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
          abort_tx_reply{tx_errc::partition_not_exists});
    }

    auto leader = _leaders.local().get_leader(ntp);
    if (!leader) {
        vlog(txlog.warn, "can't find a leader for {}", ntp);
        return ss::make_ready_future<abort_tx_reply>(
          abort_tx_reply{tx_errc::leader_not_found});
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
        [ntp, pid, tx_seq, timeout](tx_gateway_client_protocol cp) {
            return cp.abort_tx(
              abort_tx_request{ntp, pid, tx_seq, timeout},
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<abort_tx_reply>)
      .then([](result<abort_tx_reply> r) {
          if (r.has_error()) {
              vlog(txlog.warn, "got error {} on remote abort tx", r.error());
              return abort_tx_reply{tx_errc::timeout};
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
          abort_tx_reply{tx_errc::leader_not_found});
    }

    auto shard = _shard_table.local().shard_for(ntp);

    if (!shard) {
        return ss::make_ready_future<abort_tx_reply>(
          abort_tx_reply{tx_errc::shard_not_found});
    }

    return _partition_manager.invoke_on(
      *shard,
      _ssg,
      [pid, ntp, tx_seq, timeout](cluster::partition_manager& mgr) mutable {
          auto partition = mgr.get(ntp);
          if (!partition) {
              return ss::make_ready_future<abort_tx_reply>(
                abort_tx_reply{tx_errc::partition_not_found});
          }

          auto stm = partition->rm_stm();

          if (!stm) {
              vlog(txlog.warn, "can't get tx stm of the {}' partition", ntp);
              return ss::make_ready_future<abort_tx_reply>(
                abort_tx_reply{tx_errc::stm_not_found});
          }

          return stm->abort_tx(pid, tx_seq, timeout).then([](tx_errc ec) {
              return cluster::abort_tx_reply{ec};
          });
      });
}

} // namespace cluster
