/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/leader_router.h"

#include "cloud_topics/level_one/domain/domain_manager.h"
#include "cloud_topics/level_one/domain/domain_supervisor.h"
#include "cloud_topics/level_one/metastore/rpc_types.h"
#include "cloud_topics/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/shard_table.h"
#include "hashing/murmur.h"
#include "model/namespace.h"
#include "rpc/connection_cache.h"

namespace cloud_topics::l1 {

namespace {

ss::future<rpc::add_objects_reply> do_add_objects(
  domain_supervisor& domain_supervisor,
  const model::ntp& ntp,
  rpc::add_objects_request req) {
    auto domain_mgr = domain_supervisor.get(ntp);
    if (!domain_mgr) {
        co_return rpc::add_objects_reply{.ec = rpc::errc::not_leader};
    }
    co_return co_await domain_mgr->add_objects(std::move(req));
}

ss::future<rpc::replace_objects_reply> do_replace_objects(
  domain_supervisor& domain_supervisor,
  const model::ntp& ntp,
  rpc::replace_objects_request req) {
    auto domain_mgr = domain_supervisor.get(ntp);
    if (!domain_mgr) {
        co_return rpc::replace_objects_reply{.ec = rpc::errc::not_leader};
    }
    co_return co_await domain_mgr->replace_objects(std::move(req));
}

ss::future<rpc::get_first_offset_ge_reply> do_get_first_offset_ge(
  domain_supervisor& domain_supervisor,
  const model::ntp& ntp,
  rpc::get_first_offset_ge_request req) {
    auto domain_mgr = domain_supervisor.get(ntp);
    if (!domain_mgr) {
        co_return rpc::get_first_offset_ge_reply{.ec = rpc::errc::not_leader};
    }
    co_return co_await domain_mgr->get_first_offset_ge(std::move(req));
}

ss::future<rpc::get_first_timestamp_ge_reply> do_get_first_timestamp_ge(
  domain_supervisor& domain_supervisor,
  const model::ntp& ntp,
  rpc::get_first_timestamp_ge_request req) {
    auto domain_mgr = domain_supervisor.get(ntp);
    if (!domain_mgr) {
        co_return rpc::get_first_timestamp_ge_reply{
          .ec = rpc::errc::not_leader};
    }
    co_return co_await domain_mgr->get_first_timestamp_ge(std::move(req));
}

ss::future<rpc::get_first_offset_for_bytes_reply> do_get_first_offset_for_bytes(
  domain_supervisor& domain_supervisor,
  const model::ntp& ntp,
  rpc::get_first_offset_for_bytes_request req) {
    auto domain_mgr = domain_supervisor.get(ntp);
    if (!domain_mgr) {
        co_return rpc::get_first_offset_for_bytes_reply{
          .ec = rpc::errc::not_leader};
    }
    co_return co_await domain_mgr->get_first_offset_for_bytes(std::move(req));
}

ss::future<rpc::get_offsets_reply> do_get_offsets(
  domain_supervisor& domain_supervisor,
  const model::ntp& ntp,
  rpc::get_offsets_request req) {
    auto domain_mgr = domain_supervisor.get(ntp);
    if (!domain_mgr) {
        co_return rpc::get_offsets_reply{.ec = rpc::errc::not_leader};
    }
    co_return co_await domain_mgr->get_offsets(std::move(req));
}

ss::future<rpc::get_compaction_info_reply> do_get_compaction_info(
  domain_supervisor& domain_supervisor,
  const model::ntp& ntp,
  rpc::get_compaction_info_request req) {
    auto domain_mgr = domain_supervisor.get(ntp);
    if (!domain_mgr) {
        co_return rpc::get_compaction_info_reply{.ec = rpc::errc::not_leader};
    }
    co_return co_await domain_mgr->get_compaction_info(std::move(req));
}

ss::future<rpc::get_term_for_offset_reply> do_get_term_for_offset(
  domain_supervisor& domain_supervisor,
  const model::ntp& ntp,
  rpc::get_term_for_offset_request req) {
    auto domain_mgr = domain_supervisor.get(ntp);
    if (!domain_mgr) {
        co_return rpc::get_term_for_offset_reply{.ec = rpc::errc::not_leader};
    }
    co_return co_await domain_mgr->get_term_for_offset(std::move(req));
}

ss::future<rpc::get_end_offset_for_term_reply> do_get_end_offset_for_term(
  domain_supervisor& domain_supervisor,
  const model::ntp& ntp,
  rpc::get_end_offset_for_term_request req) {
    auto domain_mgr = domain_supervisor.get(ntp);
    if (!domain_mgr) {
        co_return rpc::get_end_offset_for_term_reply{
          .ec = rpc::errc::not_leader};
    }
    co_return co_await domain_mgr->get_end_offset_for_term(std::move(req));
}

ss::future<rpc::set_start_offset_reply> do_set_start_offset(
  domain_supervisor& domain_supervisor,
  const model::ntp& ntp,
  rpc::set_start_offset_request req) {
    auto domain_mgr = domain_supervisor.get(ntp);
    if (!domain_mgr) {
        co_return rpc::set_start_offset_reply{.ec = rpc::errc::not_leader};
    }
    co_return co_await domain_mgr->set_start_offset(std::move(req));
}

ss::future<rpc::remove_topics_reply> do_remove_topics(
  domain_supervisor& domain_supervisor,
  const model::ntp& ntp,
  rpc::remove_topics_request req) {
    auto domain_mgr = domain_supervisor.get(ntp);
    if (!domain_mgr) {
        co_return rpc::remove_topics_reply{
          .ec = rpc::errc::not_leader, .not_removed = {}};
    }
    co_return co_await domain_mgr->remove_topics(std::move(req));
}

} // namespace

template<auto Func, typename req_t>
requires requires(frontend::proto_t f, req_t req, ::rpc::client_opts opts) {
    (f.*Func)(std::move(req), std::move(opts));
}
ss::future<typename req_t::resp_t>
frontend::remote_dispatch(req_t request, model::node_id leader_id) {
    using resp_t = req_t::resp_t;
    auto res = co_await _connection_cache->local()
                 .with_node_client<proto_t>(
                   _self,
                   ss::this_shard_id(),
                   leader_id,
                   rpc_timeout,
                   [request = std::move(request)](proto_t proto) mutable {
                       return (proto.*Func)(
                         std::move(request),
                         ::rpc::client_opts{
                           model::timeout_clock::now() + rpc_timeout});
                   })
                 .then(&::rpc::get_ctx_data<resp_t>);
    if (res.has_error()) {
        vlog(
          cd_log.warn,
          "got error {} sending to L1 STM leader {} from node {}",
          res.error().message(),
          leader_id,
          _self);
        co_return resp_t{.ec = rpc::errc::timed_out};
    }
    co_return std::move(res.value());
}

template<auto LocalFunc, auto RemoteFunc, typename req_t>
requires requires(
  cloud_topics::l1::frontend f, const model::ntp& ntp, req_t req) {
    (f.*LocalFunc)(std::move(req), ntp, ss::shard_id{0});
    request_has_metastore_partition<req_t>
      || request_has_topic_id_partition<req_t>;
}
ss::future<typename req_t::resp_t>
frontend::process(req_t req, bool local_only) {
    using resp_t = req_t::resp_t;
    auto exists = co_await ensure_topic_exists();
    if (!exists) {
        co_return resp_t{.ec = rpc::errc::not_leader};
    }
    model::partition_id metastore_pid;
    if constexpr (request_has_topic_id_partition<req_t>) {
        auto pid_opt = metastore_partition(req.tp);
        if (!pid_opt.has_value()) {
            co_return resp_t{.ec = rpc::errc::not_leader};
        }
        metastore_pid = *pid_opt;
    } else {
        metastore_pid = req.metastore_partition;
    }
    model::ntp l1_ntp{
      model::kafka_internal_namespace,
      model::l1_metastore_topic,
      metastore_pid};

    auto leader = _leaders->local().get_leader(l1_ntp);
    if (leader == _self) {
        auto shard = _shard_table->local().shard_for(l1_ntp);
        if (shard.has_value()) {
            co_return co_await (this->*LocalFunc)(
              std::move(req), std::move(l1_ntp), shard.value());
        }
    } else if (leader.has_value() && !local_only) {
        co_return co_await remote_dispatch<RemoteFunc>(
          std::move(req), leader.value());
    }
    co_return resp_t{.ec = rpc::errc::not_leader};
}

template ss::future<rpc::add_objects_reply>
  frontend::remote_dispatch<&frontend::client::add_objects>(
    rpc::add_objects_request, model::node_id);
template ss::future<rpc::add_objects_reply> frontend::process<
  &frontend::add_objects_locally,
  &frontend::client::add_objects>(rpc::add_objects_request, bool);

template ss::future<rpc::replace_objects_reply>
  frontend::remote_dispatch<&frontend::client::replace_objects>(
    rpc::replace_objects_request, model::node_id);
template ss::future<rpc::replace_objects_reply> frontend::process<
  &frontend::replace_objects_locally,
  &frontend::client::replace_objects>(rpc::replace_objects_request, bool);

template ss::future<rpc::get_first_offset_ge_reply>
  frontend::remote_dispatch<&frontend::client::get_first_offset_ge>(
    rpc::get_first_offset_ge_request, model::node_id);
template ss::future<rpc::get_first_offset_ge_reply> frontend::process<
  &frontend::get_first_offset_ge_locally,
  &frontend::client::get_first_offset_ge>(
  rpc::get_first_offset_ge_request, bool);

template ss::future<rpc::get_first_timestamp_ge_reply>
  frontend::remote_dispatch<&frontend::client::get_first_timestamp_ge>(
    rpc::get_first_timestamp_ge_request, model::node_id);
template ss::future<rpc::get_first_timestamp_ge_reply> frontend::process<
  &frontend::get_first_timestamp_ge_locally,
  &frontend::client::get_first_timestamp_ge>(
  rpc::get_first_timestamp_ge_request, bool);

template ss::future<rpc::get_offsets_reply>
  frontend::remote_dispatch<&frontend::client::get_offsets>(
    rpc::get_offsets_request, model::node_id);
template ss::future<rpc::get_offsets_reply> frontend::process<
  &frontend::get_offsets_locally,
  &frontend::client::get_offsets>(rpc::get_offsets_request, bool);

template ss::future<rpc::get_compaction_info_reply>
  frontend::remote_dispatch<&frontend::client::get_compaction_info>(
    rpc::get_compaction_info_request, model::node_id);
template ss::future<rpc::get_compaction_info_reply> frontend::process<
  &frontend::get_compaction_info_locally,
  &frontend::client::get_compaction_info>(
  rpc::get_compaction_info_request, bool);

template ss::future<rpc::get_term_for_offset_reply>
  frontend::remote_dispatch<&frontend::client::get_term_for_offset>(
    rpc::get_term_for_offset_request, model::node_id);
template ss::future<rpc::get_term_for_offset_reply> frontend::process<
  &frontend::get_term_for_offset_locally,
  &frontend::client::get_term_for_offset>(
  rpc::get_term_for_offset_request, bool);

template ss::future<rpc::get_end_offset_for_term_reply>
  frontend::remote_dispatch<&frontend::client::get_end_offset_for_term>(
    rpc::get_end_offset_for_term_request, model::node_id);
template ss::future<rpc::get_end_offset_for_term_reply> frontend::process<
  &frontend::get_end_offset_for_term_locally,
  &frontend::client::get_end_offset_for_term>(
  rpc::get_end_offset_for_term_request, bool);

template ss::future<rpc::set_start_offset_reply>
  frontend::remote_dispatch<&frontend::client::set_start_offset>(
    rpc::set_start_offset_request, model::node_id);
template ss::future<rpc::set_start_offset_reply> frontend::process<
  &frontend::set_start_offset_locally,
  &frontend::client::set_start_offset>(rpc::set_start_offset_request, bool);

frontend::frontend(
  model::node_id self,
  ss::sharded<cluster::metadata_cache>* metadata,
  ss::sharded<cluster::partition_leaders_table>* leaders,
  ss::sharded<cluster::shard_table>* shards,
  ss::sharded<::rpc::connection_cache>* connections,
  ss::sharded<domain_supervisor>* domain_supervisor)
  : _self(self)
  , _metadata(metadata)
  , _leaders(leaders)
  , _shard_table(shards)
  , _connection_cache(connections)
  , _domain_supervisor(&domain_supervisor->local()) {}

ss::future<> frontend::stop() { return _gate.close(); }

ss::future<bool> frontend::ensure_topic_exists() {
    return _domain_supervisor->maybe_create_metastore_topic();
}

std::optional<model::partition_id>
frontend::metastore_partition(const model::topic_id_partition& tp) const {
    const auto md = _metadata->local().get_topic_metadata_ref(
      model::l1_metastore_nt);
    if (!md) {
        return std::nullopt;
    }
    iobuf temp;
    write(temp, tp);
    auto bytes = iobuf_to_bytes(temp);
    auto partition = murmur2(bytes.data(), bytes.size())
                     % md->get().get_configuration().partition_count;
    return model::partition_id{static_cast<int32_t>(partition)};
}

std::optional<int> frontend::num_metastore_partitions() const {
    const auto md = _metadata->local().get_topic_metadata_ref(
      model::l1_metastore_nt);
    if (!md) {
        return std::nullopt;
    }
    return md->get().get_configuration().partition_count;
}

ss::future<rpc::add_objects_reply> frontend::add_objects_locally(
  rpc::add_objects_request request,
  const model::ntp& metastore_ntp,
  ss::shard_id shard) {
    co_return co_await container().invoke_on(
      shard, [metastore_ntp, req = std::move(request)](frontend& fe) mutable {
          return do_add_objects(
            *(fe._domain_supervisor), metastore_ntp, std::move(req));
      });
}

ss::future<rpc::add_objects_reply> frontend::add_objects(
  rpc::add_objects_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::add_objects_locally,
      &client::add_objects>(std::move(request), bool(local_only_exec));
}

ss::future<rpc::replace_objects_reply> frontend::replace_objects_locally(
  rpc::replace_objects_request request,
  const model::ntp& metastore_ntp,
  ss::shard_id shard) {
    co_return co_await container().invoke_on(
      shard, [metastore_ntp, req = std::move(request)](frontend& fe) mutable {
          return do_replace_objects(
            *(fe._domain_supervisor), metastore_ntp, std::move(req));
      });
}

ss::future<rpc::replace_objects_reply> frontend::replace_objects(
  rpc::replace_objects_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::replace_objects_locally,
      &client::replace_objects>(std::move(request), bool(local_only_exec));
}

ss::future<rpc::get_first_offset_ge_reply>
frontend::get_first_offset_ge_locally(
  rpc::get_first_offset_ge_request request,
  const model::ntp& metastore_ntp,
  ss::shard_id shard) {
    co_return co_await container().invoke_on(
      shard, [metastore_ntp, req = std::move(request)](frontend& fe) mutable {
          return do_get_first_offset_ge(
            *(fe._domain_supervisor), metastore_ntp, std::move(req));
      });
}

ss::future<rpc::get_first_offset_ge_reply> frontend::get_first_offset_ge(
  rpc::get_first_offset_ge_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::get_first_offset_ge_locally,
      &client::get_first_offset_ge>(std::move(request), bool(local_only_exec));
}

ss::future<rpc::get_first_timestamp_ge_reply>
frontend::get_first_timestamp_ge_locally(
  rpc::get_first_timestamp_ge_request request,
  const model::ntp& metastore_ntp,
  ss::shard_id shard) {
    co_return co_await container().invoke_on(
      shard, [metastore_ntp, req = std::move(request)](frontend& fe) mutable {
          return do_get_first_timestamp_ge(
            *(fe._domain_supervisor), metastore_ntp, std::move(req));
      });
}

ss::future<rpc::get_first_offset_for_bytes_reply>
frontend::get_first_offset_for_bytes_locally(
  rpc::get_first_offset_for_bytes_request request,
  const model::ntp& metastore_ntp,
  ss::shard_id shard) {
    co_return co_await container().invoke_on(
      shard, [metastore_ntp, req = std::move(request)](frontend& fe) mutable {
          return do_get_first_offset_for_bytes(
            *(fe._domain_supervisor), metastore_ntp, std::move(req));
      });
}

ss::future<rpc::get_first_timestamp_ge_reply> frontend::get_first_timestamp_ge(
  rpc::get_first_timestamp_ge_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::get_first_timestamp_ge_locally,
      &client::get_first_timestamp_ge>(
      std::move(request), bool(local_only_exec));
}

ss::future<rpc::get_first_offset_for_bytes_reply>
frontend::get_first_offset_for_bytes(
  rpc::get_first_offset_for_bytes_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::get_first_offset_for_bytes_locally,
      &client::get_first_offset_for_bytes>(
      std::move(request), bool(local_only_exec));
}

ss::future<rpc::get_offsets_reply> frontend::get_offsets_locally(
  rpc::get_offsets_request request,
  const model::ntp& metastore_ntp,
  ss::shard_id shard) {
    co_return co_await container().invoke_on(
      shard, [metastore_ntp, req = std::move(request)](frontend& fe) mutable {
          return do_get_offsets(
            *(fe._domain_supervisor), metastore_ntp, std::move(req));
      });
}

ss::future<rpc::get_offsets_reply> frontend::get_offsets(
  rpc::get_offsets_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::get_offsets_locally,
      &client::get_offsets>(std::move(request), bool(local_only_exec));
}

ss::future<rpc::get_compaction_info_reply>
frontend::get_compaction_info_locally(
  rpc::get_compaction_info_request request,
  const model::ntp& metastore_ntp,
  ss::shard_id shard) {
    co_return co_await container().invoke_on(
      shard, [metastore_ntp, req = std::move(request)](frontend& fe) mutable {
          return do_get_compaction_info(
            *(fe._domain_supervisor), metastore_ntp, std::move(req));
      });
}

ss::future<rpc::get_compaction_info_reply> frontend::get_compaction_info(
  rpc::get_compaction_info_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::get_compaction_info_locally,
      &client::get_compaction_info>(std::move(request), bool(local_only_exec));
}

ss::future<rpc::get_term_for_offset_reply>
frontend::get_term_for_offset_locally(
  rpc::get_term_for_offset_request request,
  const model::ntp& metastore_ntp,
  ss::shard_id shard) {
    co_return co_await container().invoke_on(
      shard, [metastore_ntp, req = std::move(request)](frontend& fe) mutable {
          return do_get_term_for_offset(
            *(fe._domain_supervisor), metastore_ntp, std::move(req));
      });
}

ss::future<rpc::get_term_for_offset_reply> frontend::get_term_for_offset(
  rpc::get_term_for_offset_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::get_term_for_offset_locally,
      &client::get_term_for_offset>(std::move(request), bool(local_only_exec));
}

ss::future<rpc::get_end_offset_for_term_reply>
frontend::get_end_offset_for_term_locally(
  rpc::get_end_offset_for_term_request request,
  const model::ntp& metastore_ntp,
  ss::shard_id shard) {
    co_return co_await container().invoke_on(
      shard, [metastore_ntp, req = std::move(request)](frontend& fe) mutable {
          return do_get_end_offset_for_term(
            *(fe._domain_supervisor), metastore_ntp, std::move(req));
      });
}

ss::future<rpc::get_end_offset_for_term_reply>
frontend::get_end_offset_for_term(
  rpc::get_end_offset_for_term_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::get_end_offset_for_term_locally,
      &client::get_end_offset_for_term>(
      std::move(request), bool(local_only_exec));
}

ss::future<rpc::set_start_offset_reply> frontend::set_start_offset_locally(
  rpc::set_start_offset_request request,
  const model::ntp& metastore_ntp,
  ss::shard_id shard) {
    co_return co_await container().invoke_on(
      shard, [metastore_ntp, req = std::move(request)](frontend& fe) mutable {
          return do_set_start_offset(
            *(fe._domain_supervisor), metastore_ntp, std::move(req));
      });
}

ss::future<rpc::set_start_offset_reply> frontend::set_start_offset(
  rpc::set_start_offset_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::set_start_offset_locally,
      &client::set_start_offset>(std::move(request), bool(local_only_exec));
}

ss::future<rpc::remove_topics_reply> frontend::remove_topics_locally(
  rpc::remove_topics_request request,
  const model::ntp& metastore_ntp,
  ss::shard_id shard) {
    co_return co_await container().invoke_on(
      shard, [metastore_ntp, req = std::move(request)](frontend& fe) mutable {
          return do_remove_topics(
            *(fe._domain_supervisor), metastore_ntp, std::move(req));
      });
}

ss::future<rpc::remove_topics_reply> frontend::remove_topics(
  rpc::remove_topics_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::remove_topics_locally,
      &client::remove_topics>(std::move(request), bool(local_only_exec));
}

} // namespace cloud_topics::l1
