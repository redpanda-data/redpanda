/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/inventory_service.h"

#include "cloud_storage/inventory/inv_consumer.h"
#include "cloud_storage/inventory/report_parser.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/remote.h"
#include "config/node_config.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/file.hh>

#include <algorithm>
#include <exception>
#include <filesystem>
#include <stdexcept>

using namespace std::chrono_literals;
using cloud_storage::cst_log;
namespace csi = cloud_storage::inventory;

namespace {
constexpr auto partition_leaders_retry = 5;
constexpr auto sleep_between_get_leaders = 1s;

// Adapts the remote.cc stream consuemr interface to the gzip/csv report
// consumer interface. The report consumer is passed as reference, it can
// consume multiple report files in it's lifetime maintaining state across
// calls. This is required for certain cloud providers like GCP which split one
// report into multiple files.
struct adapt_remote_stream_to_report_consumer {
    csi::is_gzip_compressed is_stream_compressed;
    csi::inventory_consumer& consumer;
    ss::future<size_t>
    operator()(size_t size, ss::input_stream<char> stream) const {
        co_await consumer.consume(std::move(stream), is_stream_compressed);
        co_return size;
    }
};

/// Helper to remove stale hash files from the hash store path. Note that only
/// the top level directories (which are per NTP) will be recursively deleted.
/// Any other file types (which will not be created by the report consumer) are
/// untouched.
ss::future<> remove_entry(std::filesystem::path parent, ss::directory_entry e) {
    if (!e.type.has_value()) {
        co_return;
    }

    if (e.type.value() != ss::directory_entry_type::directory) {
        co_return;
    }

    parent = parent.lexically_normal();
    auto path = parent / std::string{e.name};
    path = path.lexically_normal();
    auto [mismatch_start, _] = std::ranges::mismatch(parent, path);
    if (mismatch_start != parent.end()) {
        throw std::invalid_argument{fmt_with_ctx(
          fmt::format,
          "Attempt to delete path {} outside of hash store directory {}",
          path.native(),
          parent.native())};
    }
    co_return co_await ss::recursive_remove_directory(path);
}

retry_chain_node make_rtc(ss::abort_source& as) {
    return retry_chain_node{
      as,
      config::shard_local_cfg().cloud_storage_hydration_timeout_ms(),
      config::shard_local_cfg().cloud_storage_initial_backoff_ms()};
}

} // namespace

namespace cluster {

default_remote_provider::default_remote_provider(
  ss::sharded<cloud_storage::remote>& remote)
  : _remote(remote) {}

cloud_storage::cloud_storage_api& default_remote_provider::ref() {
    return _remote.local();
}

default_leaders_provider::default_leaders_provider(
  ss::sharded<partition_leaders_table>& leaders_table)
  : _leaders_table(leaders_table) {}

// On each call returns NTPs "led" by the current node. We should only process
// rows from the inventory report for these NTPs.
ss::future<absl::node_hash_set<model::ntp>>
default_leaders_provider::ntps(ss::abort_source& as) {
    absl::node_hash_set<model::ntp> ntps;
    const auto self_node_id = config::node().node_id();
    if (!self_node_id.has_value()) {
        vlog(cst_log.warn, "node has no id, cannot find leadership");
        co_return ntps;
    }

    for (auto retry : std::ranges::iota_view{0, partition_leaders_retry}) {
        std::exception_ptr ep;
        try {
            co_await _leaders_table.local().for_each_leader(
              [&ntps,
               self_node_id](auto tp_ns, auto pid, auto node_id, auto) mutable {
                  if (node_id == self_node_id) {
                      ntps.insert(model::ntp{tp_ns.ns, tp_ns.tp, pid});
                  }
              });
            break;
        } catch (...) {
            ep = std::current_exception();
            vlog(
              cloud_storage::cst_log.warn,
              "[retry {}] Failed to get current partition leaders: {}",
              retry,
              ep);
        }
        co_await ss::sleep_abortable(sleep_between_get_leaders, as);
    }
    co_return ntps;
}

inventory_service::inventory_service(
  std::filesystem::path hash_store_path,
  std::shared_ptr<leaders_provider> leaders,
  std::shared_ptr<remote_provider> remote,
  csi::inv_ops inv_ops,
  ss::lowres_clock::duration inventory_report_check_interval,
  bool should_create_report_config)
  : _hash_store_path{std::move(hash_store_path)}
  , _leaders{std::move(leaders)}
  , _remote{std::move(remote)}
  , _ops{std::move(inv_ops)}
  , _rtc{_as}
  , _inventory_report_check_interval{inventory_report_check_interval}
  , _should_create_report_config{should_create_report_config} {}

ss::future<> inventory_service::start() {
    if (ss::this_shard_id() != shard_id) {
        co_return;
    }

    vlog(
      cst_log.info,
      "starting inventory download service, should create config: {}, check "
      "interval: {}",
      _should_create_report_config,
      _inventory_report_check_interval);

    const auto config_created = co_await maybe_create_inventory_config();
    if (!config_created && _should_create_report_config) {
        _retry_creating_inv_config = true;
    }

    _report_check_timer.set_callback([this] {
        ssx::spawn_with_gate(_gate, [this] {
            return check_for_current_inventory().handle_exception(
              [](auto eptr) {
                  vlog(
                    cst_log.warn,
                    "failed to check for current inventory: {}",
                    eptr);
              });
        });
    });

    _report_check_timer.arm_periodic(_inventory_report_check_interval);
}

ss::future<bool> inventory_service::maybe_create_inventory_config() {
    bool config_created{false};
    if (_should_create_report_config) {
        vlog(cst_log.info, "Attempting to create inventory configuration");
        auto rtc = make_rtc(_as);
        if (const auto res = co_await _ops.maybe_create_inventory_configuration(
              _remote->ref(), rtc);
            res.has_error()) {
            vlog(
              cst_log.warn,
              "Inventory configuration creation failed, will retry later",
              res.error());
        } else {
            // If another node succeeded in creating the inventory, this is not
            // counted as a failure. The end goal is that the
            // configuration/schedule should exist by the time this call is
            // finished.
            vlog(
              cst_log.info,
              "Inventory configuration creation result: {}",
              res.value());
            config_created = true;
        }
    }
    co_return config_created;
}

ss::future<> inventory_service::check_for_current_inventory() {
    auto h = _gate.hold();
    if (_retry_creating_inv_config) {
        const auto config_created = co_await maybe_create_inventory_config();
        if (config_created) {
            _retry_creating_inv_config = false;
        }

        // We either created the inventory just now, or failed again. In either
        // case the report will not exist for several hours.
        co_return;
    }

    auto rtc = make_rtc(_as);
    auto res = co_await _ops.fetch_latest_report_metadata(_remote->ref(), rtc);
    if (res.has_error()) {
        const auto& error = res.error();
        if (
          error == cloud_storage::inventory::error_outcome::no_reports_found) {
            vlog(cst_log.info, "finished inventory check: {}", res.error());
        } else {
            vlog(
              cst_log.info, "failed to fetch report metadata: {}", res.error());
        }
        co_return;
    }

    const auto& [metadata_path, report_paths, datetime] = res.value();

    // The ISO-8601 datetime format can be lexically compared
    vlog(
      cst_log.info,
      "last fetched: {}, current report: {}",
      _last_fetched,
      datetime);
    if (!_last_fetched.has_value() || datetime > _last_fetched.value()) {
        begin_processing();
        co_await clean_up_hash_directory();
        if (co_await download_and_process_reports(report_paths)) {
            vlog(
              cst_log.info,
              "finished processing inventory for report {}",
              datetime);
            mark_processing_complete();
            _last_fetched = datetime;
        }
    }
}

ss::future<bool>
inventory_service::download_and_process_reports(csi::report_paths paths) {
    auto h = _gate.hold();
    const auto ntps = co_await _leaders->ntps(_as);

    if (ntps.empty()) {
        vlog(cst_log.info, "no partitions led by this node");
        co_return false;
    }

    for (const auto& ntp : ntps) {
        vlog(cst_log.trace, "filtering report for NTP {}", ntp);
    }

    // Report consumer is expected to process multiple reports while keeping
    // state across calls. This is only created once per set of files.
    csi::inventory_consumer c{
      _hash_store_path,
      ntps,
      config::shard_local_cfg()
        .cloud_storage_inventory_max_hash_size_during_parse};

    for (const auto& path : paths) {
        const auto is_path_compressed
          = std::string_view{path().string()}.ends_with(".gz");
        const adapt_remote_stream_to_report_consumer adaptor{
          csi::is_gzip_compressed{is_path_compressed}, c};
        vlog(
          cst_log.info,
          "Processing inventory report: {}, is-compressed: {}",
          path(),
          is_path_compressed);

        auto rtc = make_rtc(_as);
        if (auto res = co_await _remote->ref().download_stream(
              _ops.bucket(),
              cloud_storage::remote_segment_path{path},
              adaptor,
              rtc,
              "inventory-report",
              // TODO add metrics
              {});
            res != cloud_storage::download_result::success) {
            // We may end up here for download failures, failures to parse
            // report and failures to write the parsed data to disk. In all
            // cases it is unsafe to read the results from disk. Exit here and
            // leave the query flag unset so scrubbers will not use data.
            vlog(
              cst_log.warn,
              "Failed to process inventory report: {}, download result {}",
              path(),
              res);
            co_return false;
        }
    }
    co_return true;
}

void inventory_service::begin_processing() { _can_use_inventory_data = false; }

void inventory_service::mark_processing_complete() {
    _can_use_inventory_data = true;
}

ss::future<> inventory_service::clean_up_hash_directory() {
    auto h = _gate.hold();
    if (!co_await ss::file_exists(_hash_store_path.string())) {
        co_return;
    }

    co_await ss::with_file(
      ss::open_directory(_hash_store_path.string()), [this](auto dir) {
          return dir
            .list_directory([this](const ss::directory_entry& e) {
                return remove_entry(_hash_store_path, e)
                  .handle_exception([this, e](std::exception_ptr ex) {
                      vlog(
                        cst_log.warn,
                        "failed to remove entry {}/{}: {}",
                        _hash_store_path.string(),
                        e.name,
                        ex);
                  });
            })
            .done();
      });
}

ss::future<> inventory_service::stop() {
    _report_check_timer.cancel();
    vlog(cst_log.info, "Stopping inventory service");
    co_await _gate.close();
    vlog(cst_log.info, "Stopped inventory service");
}

} // namespace cluster
