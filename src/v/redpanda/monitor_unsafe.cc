/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "redpanda/monitor_unsafe.h"

#include "cluster/logger.h"
#include "config/configuration.h"
#include "strings/utf8.h"

using cluster::clusterlog;

monitor_unsafe::monitor_unsafe(
  ss::sharded<features::feature_table>& feature_table)
  : _feature_table(feature_table)
  , _legacy_permit_unsafe_log_operation(
      config::shard_local_cfg().legacy_permit_unsafe_log_operation.bind()) {}

void monitor_unsafe::unsafe_log_update() {
    auto flag_val = _legacy_permit_unsafe_log_operation();
    auto original_version = _feature_table.local().get_original_version();
    ssx::spawn_with_gate(_gate, [flag_val, original_version] {
        return ss::smp::invoke_on_all([flag_val, original_version] {
            monitor_unsafe::invoke_unsafe_log_update(
              original_version, flag_val);
        });
    });
}

void monitor_unsafe::invoke_unsafe_log_update(
  cluster::cluster_version original_version, bool flag_value) {
    vlog(
      clusterlog.trace,
      "Updating unsafe log flag: original version: {}, flag: {}",
      original_version,
      flag_value);
    /*
     * How the logic works:
     *
     * +------------------+------------+---------------------+
     * | Original Version | Flag Value | Permit Unsafe Chars |
     * +------------------+------------+---------------------+
     * | >=v23.2.1        | X          | No                  |
     * | <v23.2.1         | T          | Yes                 |
     * | <v23.2.1         | F          | No                  |
     * +------------------+------------+---------------------+
     *
     * If this is a new Redpanda cluster, then the value of the legacy flag
     * is ignored.  If this is an upgraded cluster, then the legacy flag will
     * control whether or not we permit control characters.
     */
    if (original_version >= flag_introduction_version) {
        permit_unsafe_log_operation::set(false);
    } else {
        permit_unsafe_log_operation::set(flag_value);
    }
}

void monitor_unsafe::log_development_feature_warning() {
    if (!config::shard_local_cfg().development_features_enabled()) {
        return;
    }

    vlog(
      clusterlog.warn,
      "WARNING: development features have been enabled which may result in "
      "instability, unrecoverable data loss, or the inability to upgrade.");
}

ss::future<> monitor_unsafe::maybe_log_unsafe_nag() {
    auto nag_check_retry
      = config::shard_local_cfg().legacy_unsafe_log_warning_interval_sec();

    if (
      _feature_table.local().get_original_version() < flag_introduction_version
      && permit_unsafe_log_operation::get()) {
        vlog(
          clusterlog.warn,
          "You have enabled unsafe log operations.  In future versions of "
          "Redpanda, strings containing ASCII control characters will be "
          "rejected.  Try disabling 'legacy_permit_unsafe_log_operation'.  If "
          "you need assistance, please contact Redpanda support");
    }

    log_development_feature_warning();

    try {
        co_await ss::sleep_abortable(nag_check_retry, _as);
    } catch (const ss::sleep_aborted&) {
        // Shutting down - next iteration will drop out
    }
}

ss::future<> monitor_unsafe::start() {
    _legacy_permit_unsafe_log_operation.watch([this] { unsafe_log_update(); });
    unsafe_log_update();
    // Only run the nag on one shard
    ssx::spawn_with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _as.abort_requested(); },
          [this] { return maybe_log_unsafe_nag(); });
    });

    co_return;
}

ss::future<> monitor_unsafe::stop() {
    _as.request_abort();
    co_await _gate.close();
}
