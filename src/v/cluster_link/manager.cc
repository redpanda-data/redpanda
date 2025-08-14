/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/manager.h"

#include "cluster_link/logger.h"

#include <seastar/coroutine/as_future.hh>

#include <utility>

using namespace std::chrono_literals;

using kafka::data::rpc::partition_leader_cache;
using kafka::data::rpc::partition_manager;
using kafka::data::rpc::topic_metadata_cache;

namespace cluster_link {
manager::manager(
  ::model::node_id self,
  std::unique_ptr<kafka::data::rpc::partition_leader_cache>
    partition_leader_cache,
  std::unique_ptr<kafka::data::rpc::partition_manager> partition_manager,
  std::unique_ptr<kafka::data::rpc::topic_metadata_cache> topic_metadata_cache,
  std::unique_ptr<link_registry> registry,
  std::unique_ptr<link_factory> link_factory,
  std::unique_ptr<cluster_factory> cluster_factory,
  ss::lowres_clock::duration task_reconciler_interval)
  : _self(self)
  , _partition_leader_cache(std::move(partition_leader_cache))
  , _partition_manager(std::move(partition_manager))
  , _topic_metadata_cache(std::move(topic_metadata_cache))
  , _registry(std::move(registry))
  , _link_factory(std::move(link_factory))
  , _cluster_factory(std::move(cluster_factory))
  , _queue(
      [](const std::exception_ptr& ex) {
          vlog(cllog.warn, "unexpected cluster link manager error: {}", ex);
      },
      ssx::work_queue::is_paused_t::yes)
  , _task_reconciler_interval(task_reconciler_interval) {}

ss::future<> manager::start() {
    vlog(cllog.info, "Starting cluster link manager");
    auto ids = _registry->get_all_link_ids();
    for (auto id : ids) {
        co_await handle_on_link_change(id);
    }
    _link_task_reconciler_timer.set_callback([this] {
        ssx::spawn_with_gate(_g, [this] { return link_task_reconciler(); });
    });
    _link_task_reconciler_timer.arm_periodic(_task_reconciler_interval);
    _queue.resume();
}

ss::future<> manager::stop() {
    vlog(cllog.info, "Stopping cluster link manager");

    co_await _queue.shutdown();
    _link_task_reconciler_timer.cancel();
    _as.request_abort();
    co_await _g.close();
    for (auto& [_, link] : _links) {
        co_await link->stop();
    }

    vlog(cllog.info, "Cluster link manager stopped");
}

void manager::on_link_change(model::id_t id) {
    vlog(cllog.trace, "Cluster link with id={} has changed", id);
    _queue.submit([this, id] { return handle_on_link_change(id); });
}

void manager::handle_partition_state_change(
  ::model::ntp ntp, ntp_leader is_ntp_leader) {
    vlog(cllog.trace, "NTP={} leadership changed to {}", ntp, is_ntp_leader);
    _queue.submit([this, ntp{std::move(ntp)}, is_ntp_leader]() mutable {
        return handle_on_leadership_change(std::move(ntp), is_ntp_leader);
    });
}

ss::future<> manager::handle_on_link_change(model::id_t id) {
    static constexpr auto retry_delay = 10s;

    vlog(cllog.trace, "Handling cluster link change for id={}", id);
    auto link_opt = _registry->find_link_by_id(id);
    if (!link_opt) {
        vlog(cllog.debug, "Detected cluster link id={} has been removed", id);
        auto it = _links.find(id);
        if (it != _links.end()) {
            // Stop and remove the link
            try {
                vlog(cllog.debug, "Stopping cluster link with id={}", id);
                co_await it->second->stop();
                _links.erase(it);
            } catch (const std::exception& e) {
                vlog(
                  cllog.warn,
                  "Failed to stop link {}: \"{}\".  Re-attempting link stop "
                  "within {} seconds",
                  id,
                  e,
                  retry_delay.count());
                _queue.submit_delayed(retry_delay, [this, id] {
                    return handle_on_link_change(id);
                });
            }
        } else {
            vlog(cllog.trace, "No link found for id={}", id);
        }
        co_return;
    }

    const auto& link_metadata = link_opt->get();
    auto it = _links.find(id);
    if (it != _links.end()) {
        // Link already exists, update its configur
        vlog(
          cllog.debug,
          "Updating cluster link id={} with new config: {}",
          id,
          link_metadata);
        it->second->update_config(link_metadata.copy());
    } else {
        // Create a new link
        vlog(
          cllog.debug,
          "Creating new link with id={}, config: {}",
          id,
          link_metadata);
        try {
            auto units = co_await _link_task_reconciler_mutex.get_units(_as);
            auto new_link = _link_factory->create_link(
              _self,
              id,
              this,
              link_metadata.copy(),
              _cluster_factory->create_cluster(link_metadata));
            vassert(
              new_link, "Link factory returned a null link for id={}", id);
            // Register tasks for the link
            for (auto& task_factory : _task_factories) {
                try {
                    auto ec = co_await new_link->register_task(
                      task_factory.get());
                    if (!ec) {
                        vlog(
                          cllog.warn,
                          "Failed to register task '{}': {}",
                          task_factory->created_task_name(),
                          ec.assume_error().message());
                    }
                } catch (const std::exception& e) {
                    vlog(
                      cllog.warn,
                      "Failed to register task {} for link {}: \"{}\". "
                      "Continuing with link creation",
                      task_factory->created_task_name(),
                      id,
                      e);
                }
            }
            co_await new_link->start();
            _links.emplace(id, std::move(new_link));
        } catch (const ss::semaphore_aborted&) {
            vlog(cllog.debug, "Semaphore aborted, stopping link creation");
            co_return;
        } catch (const std::exception& e) {
            vlog(
              cllog.warn,
              "Failed to create link {}: \"{}\".  Re-attempting link creation "
              "in {} seconds",
              id,
              e,
              retry_delay.count());
            _queue.submit_delayed(
              retry_delay, [this, id] { return handle_on_link_change(id); });
        }
    }
}

topic_metadata_cache& manager::topic_metadata_cache() noexcept {
    return *_topic_metadata_cache;
}

partition_leader_cache& manager::partition_leader_cache() noexcept {
    return *_partition_leader_cache;
}

const partition_leader_cache& manager::partition_leader_cache() const noexcept {
    return *_partition_leader_cache;
}

partition_manager& manager::partition_manager() noexcept {
    return *_partition_manager;
}

const partition_manager& manager::partition_manager() const noexcept {
    return *_partition_manager;
}

ss::future<> manager::link_task_reconciler() {
    vlog(cllog.trace, "Reconciling tasks for all cluster links");

    auto fut = co_await ss::coroutine::as_future(
      _link_task_reconciler_mutex.get_units(_as));
    if (fut.failed()) {
        // abort source triggered, exit early
        co_return;
    }
    auto units = std::move(fut).get();

    for (const auto& [_, link] : _links) {
        vlog(
          cllog.trace,
          "Reconciling tasks for cluster link {} ({})",
          link->config().name,
          link->config().uuid);
        for (const auto& task_factory : _task_factories) {
            auto task_name = task_factory->created_task_name();
            if (!link->task_is_registered(task_name)) {
                vlog(
                  cllog.debug,
                  "Registering task {} for cluster link {} ({})",
                  task_name,
                  link->config().name,
                  link->config().uuid);
                auto ec = co_await link->register_task(task_factory.get());
                if (!ec) {
                    vlog(
                      cllog.error,
                      "Error occurred while registering the task: {}",
                      ec.assume_error().message());
                }
            }
        }
    }
}

ss::future<> manager::handle_on_leadership_change(
  ::model::ntp ntp, ntp_leader is_ntp_leader) {
    vlog(
      cllog.trace,
      "Handling leadership change for NTP={}, is_ntp_leader={}",
      ntp,
      is_ntp_leader);

    co_await ss::parallel_for_each(_links, [ntp, is_ntp_leader](auto& pair) {
        return pair.second->handle_on_leadership_change(ntp, is_ntp_leader);
    });
}

ss::future<::cluster::cluster_link::errc> manager::add_mirror_topic(
  model::id_t link_id, model::add_mirror_topic_cmd cmd) {
    static constexpr auto mirror_topic_timeout = 5s;
    return _registry->add_mirror_topic(
      link_id,
      std::move(cmd),
      ::model::timeout_clock::now() + mirror_topic_timeout);
}

ss::future<::cluster::cluster_link::errc> manager::update_mirror_topic_state(
  model::id_t link_id, model::update_mirror_topic_state_cmd cmd) {
    static constexpr auto mirror_topic_timeout = 5s;
    return _registry->update_mirror_topic_state(
      link_id,
      std::move(cmd),
      ::model::timeout_clock::now() + mirror_topic_timeout);
}

ss::future<::cluster::cluster_link::errc>
manager::update_mirror_topic_properties(
  model::id_t link_id, model::update_mirror_topic_properties_cmd cmd) {
    static constexpr auto mirror_topic_timeout = 5s;
    return _registry->update_mirror_topic_properties(
      link_id,
      std::move(cmd),
      ::model::timeout_clock::now() + mirror_topic_timeout);
}

std::optional<chunked_hash_map<::model::topic, model::mirror_topic_metadata>>
manager::get_mirror_topics_for_link(model::id_t id) const {
    return _registry->get_mirror_topics_for_link(id);
}

model::cluster_link_task_status_report manager::get_task_status_report() const {
    model::cluster_link_task_status_report report;
    report.link_reports.reserve(_links.size());
    for (const auto& [_, link] : _links) {
        auto link_report = link->get_task_status_report();
        auto name = link_report.link_name;
        report.link_reports.emplace(std::move(name), std::move(link_report));
    }

    return report;
}
} // namespace cluster_link
