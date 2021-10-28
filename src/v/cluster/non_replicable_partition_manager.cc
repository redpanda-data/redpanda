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

#include "cluster/logger.h"
#include "cluster/non_replicable_partition_manager.h"
#include "storage/api.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

namespace cluster {

non_replicable_partition_manager::non_replicable_partition_manager(
  ss::sharded<storage::api>& storage) noexcept
  : _storage(storage.local()) {}

ss::lw_shared_ptr<non_replicable_partition>
non_replicable_partition_manager::get(const model::ntp& ntp) const {
    if (auto it = _ntp_table.find(ntp); it != _ntp_table.end()) {
        return it->second;
    }
    return nullptr;
}

ss::future<> non_replicable_partition_manager::manage(
  storage::ntp_config ntp_cfg, ss::lw_shared_ptr<partition> src) {
    auto holder = _gate.hold();
    storage::log log = co_await _storage.log_mgr().manage(std::move(ntp_cfg));
    vlog(
      clusterlog.info,
      "Non replicable log created manage completed, ntp: {}, rev: {}, {} "
      "segments, {} bytes",
      log.config().ntp(),
      log.config().get_revision(),
      log.segment_count(),
      log.size_bytes());

    auto nrp = ss::make_lw_shared<non_replicable_partition>(log, src);
    _ntp_table.emplace(log.config().ntp(), nrp);
    co_await nrp->start();
}

ss::future<> non_replicable_partition_manager::stop_partitions() {
    co_await _gate.close();
    auto partitions = std::exchange(_ntp_table, {});
    co_await ss::parallel_for_each(
      partitions, [this](ntp_table_container::value_type& e) {
          return do_shutdown(e.second);
      });
}

ss::future<> non_replicable_partition_manager::remove(const model::ntp& ntp) {
    auto nr_partition = get(ntp);

    if (!nr_partition) {
        throw std::invalid_argument(fmt_with_ctx(
          ssx::sformat,
          "Can not remove non_replicable partition. NTP {} is not "
          "present in partition manager: ",
          ntp));
    }

    _ntp_table.erase(ntp);
    co_await nr_partition->stop();
    co_await _storage.log_mgr().remove(ntp);
}

ss::future<> non_replicable_partition_manager::do_shutdown(
  ss::lw_shared_ptr<non_replicable_partition> nr_partition) {
    try {
        auto ntp = nr_partition->ntp();
        co_await nr_partition->stop();
        co_await _storage.log_mgr().shutdown(nr_partition->ntp());
    } catch (...) {
        vassert(
          false,
          "error shutting down non replicable partition {},  "
          "non_replicable partition manager state: {}, error: {} - "
          "terminating redpanda",
          nr_partition->ntp(),
          *this,
          std::current_exception());
    }
}

std::ostream&
operator<<(std::ostream& o, const non_replicable_partition_manager& nr_pm) {
    return o << "{shard:" << ss::this_shard_id() << ", mngr:{}"
             << nr_pm._storage.log_mgr()
             << ", ntp_table.size:" << nr_pm._ntp_table.size() << "}";
}

} // namespace cluster
