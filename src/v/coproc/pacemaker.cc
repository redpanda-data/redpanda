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
#include "storage/api.h"
#include "storage/directories.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>

#include <absl/container/flat_hash_set.h>

#include <algorithm>
#include <exception>

namespace coproc {

rpc::transport_configuration
wasm_transport_cfg(const unresolved_address& addr) {
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

pacemaker::pacemaker(unresolved_address addr, ss::sharded<storage::api>& api)
  : _shared_res(
    rpc::reconnect_transport(
      wasm_transport_cfg(addr), wasm_transport_backoff()),
    api.local()) {
    _offs.timer.set_callback([this] {
        (void)ss::with_gate(_gate, [this] {
            return save_offsets(_offs.snap, _ntps).then([this] {
                if (!_offs.timer.armed()) {
                    _offs.timer.arm(_offs.duration);
                }
            });
        });
    });
}

ss::future<> pacemaker::start() {
    co_await ss::recursive_touch_directory(offsets_snapshot_path().string());
    _ntps = co_await recover_offsets(_offs.snap, _shared_res.api.log_mgr());
    if (!_offs.timer.armed()) {
        _offs.timer.arm(_offs.duration);
    }
}

ss::future<> pacemaker::reset() {
    _offs.timer.cancel();
    ntp_context_cache ncc = std::exchange(_ntps, {});
    auto removed = co_await remove_all_sources();
    vlog(coproclog.info, "Pacemaker reset {} scripts", removed.size());
    std::swap(_ntps, ncc);
    if (!_offs.timer.armed()) {
        _offs.timer.arm(_offs.duration);
    }
}

ss::future<> pacemaker::stop() {
    /// Ensure no more timers are fired
    _offs.timer.cancel();
    /// Waiting on the gate here will cause a deadlock since the signal to stop
    /// all fibers hasn't yet been sent out (remove_all_sources). However we
    /// wish to close the gate before any async actions begin in this method to
    /// prevent add_sources from adding new scripts.
    ss::future<> gate_closed = _gate.close();
    /// Deregister and clean-up resources
    const size_t n_active_scripts = _scripts.size();
    const auto results = co_await remove_all_sources();
    const size_t n_removed = std::accumulate(
      results.cbegin(),
      results.cend(),
      size_t(0),
      [](size_t acc, const std::pair<script_id, errc>& pair) {
          return pair.second == errc::success ? acc + 1 : acc;
      });
    if (n_removed != n_active_scripts) {
        vlog(coproclog.error, "Failed to gracefully shutdown all copro fibers");
    }
    /// Finally close the connection to the wasm engine
    vlog(coproclog.info, "Closing connection to coproc wasm engine");
    co_await _shared_res.transport.stop();
    co_await std::move(gate_closed);
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
    const auto [_, success] = _scripts.emplace(id, std::move(script_ctx));
    vassert(success, "Double coproc insert detected");
    vlog(coproclog.debug, "Adding source with id: {}", id);
    (void)ss::with_gate(_gate, [this, id] {
        auto found = _scripts.find(id);
        if (found == _scripts.end()) {
            return ss::now();
        }
        return found->second->start().handle_exception_type(
          [this, id](const script_failed_exception& e) {
              /// A script must be deregistered due to an internal script error.
              /// The wasm engine determines the case, most likley the apply()
              /// method has thrown or there is a syntax error within the script
              /// itself.
              vlog(coproclog.info, "Handling script_failed_exception: {}", e);
              vassert(
                id == e.get_id(),
                "script_failed_handler id mismatch detected, observed: {} "
                "expected: {}",
                e.get_id(),
                id);
              return container().invoke_on_all([id](pacemaker& p) {
                  return p.remove_source(id).discard_result();
              });
          });
    });
    return acks;
}

static void set_start_offset(
  script_id id,
  ss::lw_shared_ptr<ntp_context> ntp_ctx,
  topic_ingestion_policy tip) {
    if (tip == topic_ingestion_policy::earliest) {
        ntp_ctx->offsets[id] = ntp_context::offset_pair{};
    } else if (tip == topic_ingestion_policy::latest) {
        model::offset last = ntp_ctx->log.offsets().dirty_offset;
        ntp_ctx->offsets[id] = ntp_context::offset_pair{
          .last_read = last, .last_acked = last};
    } else if (tip == topic_ingestion_policy::stored) {
        /// If this succeeds, there was no stored offset anyway, default option
        /// is to start at the beginning of the log
        ntp_ctx->offsets.emplace(id, ntp_context::offset_pair{});
    } else {
        __builtin_unreachable();
    }
}

void pacemaker::do_add_source(
  script_id id,
  ntp_context_cache& ctxs,
  std::vector<errc>& acks,
  const std::vector<topic_namespace_policy>& topics) {
    for (const topic_namespace_policy& tnp : topics) {
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
            set_start_offset(id, ntp_ctx, tnp.policy);
            ctxs.emplace(ntp, ntp_ctx);
        }
        acks.push_back(errc::success);
    }
}

ss::future<errc> pacemaker::remove_source(script_id id) {
    vlog(coproclog.debug, "Removing source with id: {}", id);
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

ss::future<absl::btree_map<script_id, errc>> pacemaker::remove_all_sources() {
    std::vector<script_id> ids;
    ids.reserve(_scripts.size());
    for (const auto& [id, _] : _scripts) {
        ids.push_back(id);
    }
    absl::btree_map<script_id, errc> results_map;
    for (const script_id& id : ids) {
        errc result = co_await remove_source(id);
        results_map.emplace(id, result);
    }
    co_return results_map;
}

bool pacemaker::local_script_id_exists(script_id id) {
    return _scripts.find(id) != _scripts.end();
}

} // namespace coproc
