/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/pacemaker.h"

#include "coproc/logger.h"
#include "coproc/ntp_context.h"
#include "coproc/offset_storage_utils.h"
#include "coproc/types.h"
#include "model/validation.h"
#include "rpc/reconnect_transport.h"
#include "rpc/types.h"
#include "ssx/future-util.h"
#include "storage/directories.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>

#include <absl/container/flat_hash_set.h>

#include <algorithm>

namespace coproc {

rpc::transport_configuration
wasm_transport_cfg(const ss::socket_address& addr) {
    return rpc::transport_configuration{
      .server_addr = addr,
      .max_queued_bytes = static_cast<uint32_t>(
        config::shard_local_cfg().coproc_max_inflight_bytes.value()),
      .credentials = nullptr,
      .disable_metrics = rpc::metrics_disabled(
        config::shard_local_cfg().disable_metrics())};
}

rpc::backoff_policy wasm_transport_backoff() {
    using namespace std::chrono_literals;
    return rpc::make_exponential_backoff_policy<rpc::clock_type>(1s, 10s);
}

pacemaker::pacemaker(ss::socket_address addr, ss::sharded<storage::api>& api)
  : _shared_res(
    rpc::reconnect_transport(
      wasm_transport_cfg(addr), wasm_transport_backoff()),
    api.local()) {}

ss::future<> pacemaker::start() {
    _offs.timer.set_callback([this] {
        (void)ss::with_gate(_offs.gate, [this] {
            return save_offsets(_offs.snap, _ntps).then([this] {
                if (!_offs.timer.armed()) {
                    _offs.timer.arm(_offs.duration);
                }
            });
        });
    });
    co_await ss::recursive_touch_directory(offsets_snapshot_path().string());
    auto ncc = co_await recover_offsets(_offs.snap, _shared_res.api.log_mgr());
    _ntps = std::move(ncc);
    _offs.timer.arm(_offs.duration);
}

ss::future<> pacemaker::stop() {
    /// First shutdown the offset keepers loop
    _offs.timer.cancel();
    co_await _offs.gate.close();
    std::vector<script_id> ids;
    ids.reserve(_scripts.size());
    for (const auto& [id, _] : _scripts) {
        ids.push_back(id);
    }
    /// Next de-register all coprocessors
    bool success = true;
    for (const script_id& id : ids) {
        success = co_await remove_source(id) == errc::success;
        if (!success) {
            break;
        }
    }
    if (!success) {
        vlog(coproclog.error, "Failed to gracefully shutdown all copro fibers");
    }
    /// Finally close the connection to the wasm engine
    vlog(coproclog.info, "Closing connection to coproc wasm engine");
    co_await _shared_res.transport.stop();
}

std::vector<errc> pacemaker::add_source(
  script_id id, std::vector<topic_namespace_policy> topics) {
    ntp_context_cache ctxs;
    std::vector<errc> acks;
    vassert(
      _scripts.find(id) == _scripts.end(),
      "add_source() detects already existing script_id: {}",
      id);
    do_add_source(id, ctxs, acks, topics);
    if (ctxs.empty()) {
        /// Failure occurred or no matching topics/ntps found
        /// Reasons are returned in the 'acks' structure
        return acks;
    }
    auto script_ctx = std::make_unique<script_context>(
      id, _shared_res, std::move(ctxs));
    const auto& [itr, success] = _scripts.emplace(id, std::move(script_ctx));
    vassert(success, "Double coproc insert detected");
    (void)itr->second->start();
    return acks;
}

errc check_topic_policy(const model::topic& topic, topic_ingestion_policy tip) {
    if (tip != topic_ingestion_policy::latest) {
        return errc::invalid_ingestion_policy;
    }
    if (model::is_materialized_topic(topic)) {
        return errc::materialized_topic;
    }
    if (model::validate_kafka_topic_name(topic).value() != 0) {
        return errc::invalid_topic;
    }
    return errc::success;
}

void pacemaker::do_add_source(
  script_id id,
  ntp_context_cache& ctxs,
  std::vector<errc>& acks,
  const std::vector<topic_namespace_policy>& topics) {
    for (const topic_namespace_policy& tnp : topics) {
        const errc r = check_topic_policy(tnp.tn.tp, tnp.policy);
        if (r != errc::success) {
            acks.push_back(r);
            continue;
        }
        auto logs = _shared_res.api.log_mgr().get(tnp.tn);
        if (logs.empty()) {
            acks.push_back(errc::topic_does_not_exist);
            continue;
        }
        for (auto& [ntp, log] : logs) {
            auto found = _ntps.find(ntp);
            ss::lw_shared_ptr<ntp_context> ntp_ctx;
            if (found != _ntps.end()) {
                ntp_ctx = found->second;
            } else {
                ntp_ctx = ss::make_lw_shared<ntp_context>(log);
                _ntps.emplace(ntp, ntp_ctx);
            }
            vassert(
              ntp_ctx->offsets.emplace(id, ntp_context::offset_pair()).second,
              "Script id expected to not exist: {}",
              id);
            ctxs.emplace(ntp, ntp_ctx);
        }
        acks.push_back(errc::success);
    }
}

ss::future<errc> pacemaker::remove_source(script_id id) {
    auto handle = _scripts.extract(id);
    if (handle.empty()) {
        co_return errc::script_id_does_not_exist;
    }
    std::unique_ptr<script_context> ctx = std::move(handle.mapped());
    co_await ctx->shutdown();
    /// shutdown explicity clears out strong references to ntp
    /// contexts. It is known to remove them from the pacemakers cache
    /// when the use_count() == 1, as there are then known to be no
    /// more subscribing scripts for the ntp
    absl::erase_if(_ntps, [](const ntp_context_cache::value_type& p) {
        return p.second.use_count() == 1;
    });
    co_return errc::success;
}

bool pacemaker::local_script_id_exists(script_id id) {
    return _scripts.find(id) != _scripts.end();
}

bool pacemaker::ntp_is_registered(const model::ntp& ntp) {
    auto found = _ntps.find(ntp);
    return found != _ntps.end() && found->second;
}

} // namespace coproc
