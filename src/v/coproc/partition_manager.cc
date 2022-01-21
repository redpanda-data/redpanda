/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/partition_manager.h"

#include "cluster/logger.h"
#include "cluster/partition.h"
#include "coproc/logger.h"
#include "storage/api.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

namespace coproc {

partition_manager::partition_manager(
  ss::sharded<storage::api>& storage) noexcept
  : _storage(storage.local()) {}

ss::lw_shared_ptr<partition>
partition_manager::get(const model::ntp& ntp) const {
    if (auto it = _ntp_table.find(ntp); it != _ntp_table.end()) {
        return it->second;
    }
    return nullptr;
}

ss::future<> partition_manager::manage(
  storage::ntp_config ntp_cfg, ss::lw_shared_ptr<cluster::partition> src) {
    auto holder = _gate.hold();
    storage::log log = co_await _storage.log_mgr().manage(std::move(ntp_cfg));
    vlog(
      coproclog.info,
      "Non replicable log created manage completed, ntp: {}, rev: {}, {} "
      "segments, {} bytes",
      log.config().ntp(),
      log.config().get_revision(),
      log.segment_count(),
      log.size_bytes());

    auto nrp = ss::make_lw_shared<partition>(log, src);
    auto [_, success] = _ntp_table.emplace(log.config().ntp(), nrp);
    vassert(
      success,
      "coproc::partition_manager contained item for key that was expected to "
      "not exist");
    _manage_watchers.notify(nrp->ntp(), nrp);
    co_await nrp->start();
}

ss::future<> partition_manager::stop_partitions() {
    co_await _gate.close();
    auto partitions = std::exchange(_ntp_table, {});
    co_await ss::parallel_for_each(
      partitions, [this](ntp_table_container::value_type& e) {
          return do_shutdown(e.second);
      });
}

ss::future<> partition_manager::remove(const model::ntp& ntp) {
    auto holder = _gate.hold();
    auto found = _ntp_table.find(ntp);

    if (found == _ntp_table.end()) {
        throw std::invalid_argument(fmt_with_ctx(
          ssx::sformat,
          "Can not remove non_replicable partition. NTP {} is not "
          "present in partition manager: ",
          ntp));
    }

    vlog(coproclog.info, "Removing materialized log: {}", ntp);
    auto partition = found->second;
    _ntp_table.erase(found);
    _unmanage_watchers.notify(ntp, ntp.tp.partition);
    co_await partition->stop();
    co_await _storage.log_mgr().remove(ntp);
}

ss::future<>
partition_manager::do_shutdown(ss::lw_shared_ptr<partition> partition) {
    try {
        auto ntp = partition->ntp();
        co_await partition->stop();
        co_await _storage.log_mgr().shutdown(std::move(ntp));
    } catch (...) {
        vassert(
          false,
          "error shutting down non replicable partition {},  "
          "non_replicable partition manager state: {}, error: {} - "
          "terminating redpanda",
          partition->ntp(),
          *this,
          std::current_exception());
    }
}

cluster::notification_id_type partition_manager::register_manage_notification(
  const model::ns& ns, const model::topic& topic, manage_cb_t cb) {
    cluster::ntp_callbacks<manage_cb_t> init;
    init.register_notify(
      ns, topic, [&cb](ss::lw_shared_ptr<partition> p) { cb(p); });
    for (auto& e : _ntp_table) {
        init.notify(e.first, e.second);
    }
    return _manage_watchers.register_notify(ns, topic, std::move(cb));
}

std::ostream& operator<<(std::ostream& os, const partition_manager& nr_pm) {
    fmt::print(os, "{ntp_count: {}}", nr_pm._ntp_table.size());
    return os;
}

} // namespace coproc
