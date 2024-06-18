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

#include "base/units.h"
#include "cloud_storage/inventory/inv_consumer.h"
#include "cloud_storage/inventory/report_parser.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/remote.h"

#include <seastar/core/sharded.hh>

using namespace std::chrono_literals;
using cloud_storage::cst_log;
namespace csi = cloud_storage::inventory;

namespace {
constexpr auto max_hash_size_in_memory = 128_KiB;
constexpr auto partition_leaders_retry = 5;
constexpr auto sleep_between_get_leaders = 1s;
constexpr auto marker = "done";

struct stream_consumer {
    csi::is_gzip_compressed is_stream_compressed;
    csi::inventory_consumer& consumer;
    ss::future<size_t>
    operator()(size_t size, ss::input_stream<char> stream) const {
        co_await consumer.consume(std::move(stream), is_stream_compressed);
        co_return size;
    }
};

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

ss::future<absl::node_hash_set<model::ntp>>
default_leaders_provider::ntps(ss::abort_source& as) {
    absl::node_hash_set<model::ntp> ntps;
    for (auto retry : std::ranges::iota_view{0, partition_leaders_retry}) {
        std::exception_ptr ep;
        try {
            co_await _leaders_table.local().for_each_leader(
              [&ntps](auto tp_ns, auto pid, auto, auto) mutable {
                  ntps.insert(model::ntp{tp_ns.ns, tp_ns.tp, pid});
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
  ss::lowres_clock::duration inventory_report_check_interval)
  : _hash_store_path{std::move(hash_store_path)}
  , _leaders{std::move(leaders)}
  , _remote{std::move(remote)}
  , _ops{std::move(inv_ops)}
  , _rtc{_as}
  , _marker{_hash_store_path / marker}
  , _inventory_report_check_interval{inventory_report_check_interval} {}

ss::future<> inventory_service::start() {
    if (ss::this_shard_id() != 0) {
        co_return;
    }

    auto rtc = retry_chain_node{_as, 60s, 1s};

    vlog(cst_log.info, "Attempting to create inventory configuration");
    if (const auto res = co_await _ops.maybe_create_inventory_configuration(
          _remote->ref(), rtc);
        res.has_error()) {
        vlog(
          cst_log.warn,
          "Inventory configuration creation failed, will retry later",
          res.error());
        _try_creating_inv_config = true;
    } else {
        vlog(
          cst_log.info,
          "Inventory configuration creation result: {}",
          res.value());
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

ss::future<> inventory_service::check_for_current_inventory() {
    if (_try_creating_inv_config) {
        auto rtc = retry_chain_node{_as, 60s, 1s};
        auto res = co_await _ops.maybe_create_inventory_configuration(
          _remote->ref(), rtc);

        if (res.has_value()) {
            _try_creating_inv_config = false;
        }

        // We either created the inventory just now, or failed again. In either
        // case the report will not exist for several hours.
        co_return;
    }

    auto rtc = retry_chain_node{_as, 60s, 1s};
    auto res = co_await _ops.fetch_latest_report_metadata(_remote->ref(), rtc);
    if (res.has_error()) {
        co_return;
    }

    const auto& [metadata_path, report_paths, datetime] = res.value();
    if (!_last_fetched.has_value() || datetime > _last_fetched.value()) {
        co_await begin_processing();
        if (co_await download_and_process_reports(report_paths)) {
            vlog(
              cst_log.info,
              "finished processing inventory for report {}",
              datetime);
            co_await mark_processing_complete(datetime);
        }
    }
}

ss::future<bool>
inventory_service::download_and_process_reports(csi::report_paths paths) {
    cloud_storage_clients::bucket_name b{};

    const auto ntps = co_await _leaders->ntps(_as);

    if (ntps.empty()) {
        vlog(cst_log.info, "no partitions led by this node");
        co_return false;
    }

    csi::inventory_consumer c{_hash_store_path, ntps, max_hash_size_in_memory};

    for (const auto& path : paths) {
        auto rtc = retry_chain_node{_as, 60s, 1s};
        const auto is_path_compressed
          = std::string_view{path().string()}.ends_with(".gz");
        const stream_consumer sc{
          csi::is_gzip_compressed{is_path_compressed}, c};
        vlog(
          cst_log.info,
          "Processing inventory report: {}, is-compressed: {}",
          path(),
          is_path_compressed);

        if (auto res = co_await _remote->ref().download_stream(
              b,
              cloud_storage::remote_segment_path{path},
              sc,
              rtc,
              "inventory-report",
              {});
            res != cloud_storage::download_result::success) {
            // We may end up here for download failures, failures to parse
            // report and failures to write the parsed data to disk. In call
            // cases it is unsafe to read the results from disk.
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

ss::future<> inventory_service::begin_processing() const {
    if (co_await ss::file_exists(_marker.string())) {
        co_await ss::remove_file(_marker.string());
    }
}

ss::future<> inventory_service::mark_processing_complete(
  csi::report_datetime content) const {
    if (co_await ss::file_exists(_marker.string())) {
        throw std::runtime_error{fmt::format(
          "Invalid state. Marker {} present but should have been removed "
          "before processing inventory",
          _marker.string())};
    }
    co_return co_await ss::with_file(
      ss::open_file_dma(_marker.string(), ss::open_flags::wo),
      [content](auto f) {
          return ss::make_file_output_stream(f).then(
            [content](ss::output_stream<char> stream) {
                return stream.write(content());
            });
      });
}

ss::future<> inventory_service::stop() {
    _report_check_timer.cancel();
    vlog(cst_log.info, "Stopping inventory service");
    co_await _gate.close();
    vlog(cst_log.info, "Stopped inventory service");
}

} // namespace cluster
