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
#include "kafka/data/rpc/deps.h"
#include "model/namespace.h"

#include <seastar/coroutine/as_future.hh>

#include <utility>

using namespace std::chrono_literals;

using kafka::data::rpc::partition_leader_cache;
using kafka::data::rpc::partition_manager;
using kafka::data::rpc::topic_creator;
using kafka::data::rpc::topic_metadata_cache;

namespace cluster_link {
namespace {

errc map_cluster_errc(::cluster::cluster_link::errc ec) {
    switch (ec) {
    case cluster::cluster_link::errc::success:
        return errc::success;
    case cluster::cluster_link::errc::does_not_exist:
        return errc::link_id_not_found;
    case cluster::cluster_link::errc::invalid_create:
    case cluster::cluster_link::errc::invalid_update:
    case cluster::cluster_link::errc::bootstrap_servers_empty:
    case cluster::cluster_link::errc::tls_configuration_invalid:
    case cluster::cluster_link::errc::link_name_invalid:
    case cluster::cluster_link::errc::topic_filter_invalid:
    case cluster::cluster_link::errc::topic_property_excluded_from_mirroring:
    case cluster::cluster_link::errc::mirror_topic_name_invalid:
    case cluster::cluster_link::errc::uuid_conflict:
    case cluster::cluster_link::errc::scram_configuration_invalid:
        return errc::invalid_configuration;
    case cluster::cluster_link::errc::limit_exceeded:
        return errc::link_limit_reached;
    case cluster::cluster_link::errc::service_error:
    case cluster::cluster_link::errc::timeout:
    case cluster::cluster_link::errc::not_leader_controller:
    case cluster::cluster_link::errc::replication_error:
    case cluster::cluster_link::errc::rpc_error:
    case cluster::cluster_link::errc::throttling_quota_exceeded:
        return errc::rpc_error;
    case cluster::cluster_link::errc::feature_disabled:
        return errc::cluster_link_disabled;
    case cluster::cluster_link::errc::topic_already_being_mirrored:
        return errc::topic_already_mirrored;
    case cluster::cluster_link::errc::topic_being_mirrored_by_other_link:
        return errc::topic_mirrored_by_other_link;
    case cluster::cluster_link::errc::topic_not_being_mirrored:
        return errc::topic_not_being_mirrored;
    }
    __builtin_unreachable();
}

constexpr auto topic_reconciler_interval = 30s;
} // namespace

manager::manager(
  ::model::node_id self,
  std::unique_ptr<kafka::data::rpc::partition_leader_cache>
    partition_leader_cache,
  std::unique_ptr<kafka::data::rpc::partition_manager> partition_manager,
  std::unique_ptr<kafka::data::rpc::topic_metadata_cache> topic_metadata_cache,
  std::unique_ptr<kafka::data::rpc::topic_creator> topic_creator,
  std::unique_ptr<link_registry> registry,
  std::unique_ptr<link_factory> link_factory,
  std::unique_ptr<cluster_factory> cluster_factory,
  std::unique_ptr<consumer_groups_router> group_router,
  std::unique_ptr<partition_metadata_provider> partition_metadata_provider,
  ss::lowres_clock::duration task_reconciler_interval,
  config::binding<int16_t> default_topic_replication)
  : _self(self)
  , _partition_leader_cache(std::move(partition_leader_cache))
  , _partition_manager(std::move(partition_manager))
  , _topic_metadata_cache(std::move(topic_metadata_cache))
  , _topic_creator(std::move(topic_creator))
  , _registry(std::move(registry))
  , _link_factory(std::move(link_factory))
  , _cluster_factory(std::move(cluster_factory))
  , _group_router(std::move(group_router))
  , _partition_metadata_provider(std::move(partition_metadata_provider))
  , _queue(
      [](const std::exception_ptr& ex) {
          vlog(cllog.warn, "unexpected cluster link manager error: {}", ex);
      },
      ssx::work_queue::is_paused_t::yes)
  , _task_reconciler_interval(task_reconciler_interval)
  , _default_topic_replication(std::move(default_topic_replication)) {}

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

    co_await stop_topic_reconciler();

    co_await _queue.shutdown();
    _link_task_reconciler_timer.cancel();
    _as.request_abort();
    _link_created_cv.broken();
    co_await _g.close();
    for (auto& [_, link] : _links) {
        co_await link->stop();
    }

    vlog(cllog.info, "Cluster link manager stopped");
}

ss::future<result<model::metadata>>
manager::upsert_cluster_link(model::metadata md) {
    static constexpr auto wait_for_link_creation_timeout = 30s;
    auto hold = _g.hold();
    auto name = md.name;
    vlog(cllog.info, "Attempting to create cluster link named '{}'", md.name);
    vlog(cllog.trace, "Cluster link metadata: {}", md);
    const auto needs_consumer_offsets_topic
      = md.configuration.consumer_groups_mirroring_cfg.is_enabled;
    auto ec = co_await _registry->upsert_link(
      std::move(md), ::model::timeout_clock::now() + 30s);
    auto err = map_cluster_errc(ec);
    if (err != errc::success) {
        co_return err_info(
          err, fmt::format("Failed to create cluster link: {}", ec));
    }

    try {
        if (needs_consumer_offsets_topic) {
            co_await _group_router->assure_topic_exists();
        }
        co_await _link_created_cv.wait(
          wait_for_link_creation_timeout, [this, name] {
              return _registry->find_link_by_name(name).has_value();
          });
    } catch (const ss::condition_variable_timed_out&) {
        co_return err_info(
          errc::link_creation_failed,
          fmt::format(
            "Timed out waiting for cluster link '{}' to be created", name));
    } catch (const ss::broken_condition_variable&) {
        co_return err_info(
          errc::service_shutting_down,
          fmt::format(
            "Aborted waiting for cluster link '{}' to be created", name));
    }

    auto metadata_resp = _registry->find_link_by_name(name);
    if (!metadata_resp) {
        co_return err_info(
          errc::link_creation_failed,
          fmt::format("Failed to find cluster link with name '{}'", name));
    }

    co_return metadata_resp->get().copy();
}

result<model::metadata> manager::get_cluster_link(const model::name_t& name) {
    auto metadata_resp = _registry->find_link_by_name(name);
    if (!metadata_resp) {
        return err_info(
          errc::link_id_not_found,
          fmt::format("Failed to find cluster link with name '{}'", name));
    }
    return metadata_resp->get().copy();
}

result<chunked_vector<model::metadata>> manager::list_cluster_links() {
    auto link_ids = _registry->get_all_link_ids();
    chunked_vector<model::metadata> resp;
    resp.reserve(link_ids.size());

    for (const auto id : link_ids) {
        auto maybe_md = _registry->find_link_by_id(id);
        if (!maybe_md) {
            vlog(cllog.warn, "Failed to find link ID {}", id);
            continue;
        }

        resp.emplace_back(maybe_md.value().get().copy());
    }

    return resp;
}

ss::future<result<model::metadata>> manager::update_cluster_link(
  model::name_t name, model::update_cluster_link_configuration_cmd cmd) {
    static constexpr auto model_timeout = 30s;
    auto hold = _g.hold();
    vlog(cllog.info, "Attempting to update cluster link '{}'", name);
    vlog(cllog.trace, "Update command: {}", cmd);
    const auto needs_consumer_offsets_topic
      = cmd.link_config.consumer_groups_mirroring_cfg.is_enabled;

    const auto id = _registry->find_link_id_by_name(name);
    if (!id.has_value()) {
        co_return err_info{
          errc::link_id_not_found,
          ssx::sformat("Unable to find link by name '{}'", name)};
    }

    auto ec = co_await _registry->update_cluster_link_configuration(
      *id, std::move(cmd), ::model::timeout_clock::now() + model_timeout);
    auto err = map_cluster_errc(ec);
    if (err != errc::success) {
        co_return err_info(
          err, fmt::format("Failed to update cluster link {}: {}", name, ec));
    }

    if (needs_consumer_offsets_topic) {
        co_await _group_router->assure_topic_exists();
    }

    auto metadata_resp = _registry->find_link_by_id(*id);
    if (!metadata_resp) {
        co_return err_info(
          errc::link_id_not_found,
          fmt::format("Failed to find cluster link with name '{}'", name));
    }

    co_return metadata_resp->get().copy();
}

ss::future<result<void>> manager::delete_cluster_link(model::name_t name) {
    vlog(cllog.info, "Attempting to delete cluster link named '{}'", name);
    auto cl_resp = get_cluster_link(name);
    if (cl_resp.has_error()) {
        co_return cl_resp.assume_error();
    }

    const auto is_active = [](const model::mirror_topic_state s) {
        switch (s) {
        case model::mirror_topic_state::active:
        case model::mirror_topic_state::paused:
            return true;
        case model::mirror_topic_state::failed:
        case model::mirror_topic_state::promoted:
            return false;
        }
    };

    const auto mirror_topic_states = cl_resp.assume_value().state.mirror_topics
                                     | std::views::values
                                     | std::views::transform(
                                       &model::mirror_topic_metadata::state);

    if (std::ranges::any_of(mirror_topic_states, is_active)) {
        co_return err_info(
          errc::link_has_active_shadow_topics,
          fmt::format(
            "Failed to delete cluster link with name '{}'. There are active "
            "shadow topics.",
            name));
    }

    auto ec = co_await _registry->delete_link(
      std::move(name), ::model::timeout_clock::now() + 30s);
    auto err = map_cluster_errc(ec);
    if (err != errc::success) {
        co_return err_info(
          err, fmt::format("Failed to delete cluster link: {}", ec));
    }

    co_return outcome::success();
}

void manager::on_link_change(model::id_t id) {
    vlog(cllog.trace, "Cluster link with id={} has changed", id);
    if (_topic_reconciler && _is_controller_leader) {
        _topic_reconciler->trigger(id);
    }
    _queue.submit([this, id] { return handle_on_link_change(id); });
}

void manager::handle_partition_state_change(
  ::model::ntp ntp,
  ntp_leader is_ntp_leader,
  std::optional<::model::term_id> term) {
    vlog(cllog.trace, "NTP={} leadership changed to {}", ntp, is_ntp_leader);
    _queue.submit([this, ntp{std::move(ntp)}, is_ntp_leader, term]() mutable {
        return handle_on_leadership_change(std::move(ntp), is_ntp_leader, term);
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
            } catch (const std::exception& e) {
                // generally not possible since stop() is noexcept
                // but is not enforced for coroutines by the compiler.
                vlog(
                  cllog.warn,
                  "Failed to stop link {}: \"{}, going ahead and removing "
                  "it\".",
                  id,
                  e);
            }
            _links.erase(it);
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

            std::exception_ptr start_eptr = nullptr;
            try {
                co_await new_link->start();
            } catch (...) {
                start_eptr = std::current_exception();
            }
            if (start_eptr) {
                vlog(
                  cllog.warn,
                  "Failed to start link {}: \"{}\"",
                  id,
                  start_eptr);
                try {
                    co_await new_link->stop();
                } catch (...) {
                    vlog(
                      cllog.warn,
                      "Failed to stop link {}: \"{}\", ignoring..",
                      id,
                      std::current_exception());
                }
                std::rethrow_exception(start_eptr);
            }
            _links.emplace(id, std::move(new_link));
            _link_created_cv.broadcast();
        } catch (const ss::semaphore_aborted&) {
            vlog(cllog.debug, "Semaphore aborted, stopping link creation");
            co_return;
        } catch (const std::exception& e) {
            vlog(
              cllog.warn,
              "Failed to create link {}: \"{}\".  Re-attempting link "
              "creation "
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

topic_creator& manager::topic_creator() noexcept { return *_topic_creator; }

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
  ::model::ntp ntp,
  ntp_leader is_ntp_leader,
  std::optional<::model::term_id> term) {
    vlog(
      cllog.trace,
      "Handling leadership change for NTP={}, is_ntp_leader={}",
      ntp,
      is_ntp_leader);

    if (ntp == ::model::controller_ntp) {
        if (is_ntp_leader == ntp_leader::yes && !_is_controller_leader) {
            _is_controller_leader = ntp_leader::yes;
            vlog(cllog.debug, "Starting topic reconciler on controller leader");
            co_await start_topic_reconciler();
        }
        if (is_ntp_leader == ntp_leader::no && _is_controller_leader) {
            _is_controller_leader = ntp_leader::no;
            vlog(
              cllog.debug, "Stopping topic reconciler on controller follower");
            co_await stop_topic_reconciler();
        }
    }

    co_await ss::parallel_for_each(
      _links, [ntp, is_ntp_leader, term](auto& pair) {
          return pair.second->handle_on_leadership_change(
            ntp, is_ntp_leader, term);
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

ss::future<> manager::start_topic_reconciler() {
    if (!_is_controller_leader) {
        co_return;
    }
    vlog(cllog.trace, "Starting topic reconciler");
    if (!_topic_reconciler) {
        _topic_reconciler = std::make_unique<topic_reconciler>(
          _topic_creator.get(),
          _topic_metadata_cache.get(),
          _registry.get(),
          topic_reconciler_interval,
          _default_topic_replication);
    }
    try {
        co_await _topic_reconciler->start();
        vlog(cllog.debug, "Topic reconciler has started");
    } catch (const std::exception& e) {
        vlog(cllog.error, "Failed to start topic reconciler: {}", e);
        // If it fails to start, enqueue a retry to start the reconciler
        _queue.submit_delayed(10s, [this] { return start_topic_reconciler(); });
    }
}

ss::future<> manager::stop_topic_reconciler() {
    if (_topic_reconciler) {
        vlog(cllog.trace, "Stopping topic reconciler");
        try {
            co_await _topic_reconciler->stop();
            _topic_reconciler.reset();
            vlog(cllog.debug, "Topic reconciler has stopped");
        } catch (const std::exception& e) {
            vlog(cllog.error, "Failed to stop topic reconciler: {}", e);
            // If it fails to start, enqueue a retry to start the reconciler
            _queue.submit_delayed(
              10s, [this] { return stop_topic_reconciler(); });
        }
    }
}

consumer_groups_router& manager::get_group_router() noexcept {
    return *_group_router;
}

partition_metadata_provider&
manager::get_partition_metadata_provider() noexcept {
    return *_partition_metadata_provider;
}
} // namespace cluster_link
