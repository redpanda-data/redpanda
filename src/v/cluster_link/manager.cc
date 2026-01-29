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

#include "cluster_link/deps.h"
#include "cluster_link/logger.h"
#include "cluster_link/model/types.h"
#include "kafka/data/rpc/client.h"
#include "kafka/data/rpc/deps.h"
#include "model/namespace.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/switch_to.hh>

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
    case cluster::cluster_link::errc::link_has_active_shadow_topics:
        return errc::link_has_active_shadow_topics;
    case cluster::cluster_link::errc::license_required:
        return errc::license_required;
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
  std::unique_ptr<security_service> security_service,
  std::unique_ptr<link_registry> registry,
  std::unique_ptr<link_factory> link_factory,
  std::unique_ptr<cluster_factory> cluster_factory,
  std::unique_ptr<consumer_groups_router> group_router,
  std::unique_ptr<partition_metadata_provider> partition_metadata_provider,
  std::unique_ptr<kafka_rpc_client_service> kafka_rpc_client_service,
  std::unique_ptr<members_table_provider> members_table_provider,
  ss::lowres_clock::duration task_reconciler_interval,
  config::binding<int16_t> default_topic_replication,
  ss::scheduling_group scheduling_group)
  : _self(self)
  , _partition_leader_cache(std::move(partition_leader_cache))
  , _partition_manager(std::move(partition_manager))
  , _topic_metadata_cache(std::move(topic_metadata_cache))
  , _topic_creator(std::move(topic_creator))
  , _security_service(std::move(security_service))
  , _registry(std::move(registry))
  , _link_factory(std::move(link_factory))
  , _cluster_factory(std::move(cluster_factory))
  , _group_router(std::move(group_router))
  , _partition_metadata_provider(std::move(partition_metadata_provider))
  , _kafka_rpc_client_service(std::move(kafka_rpc_client_service))
  , _members_table_provider(std::move(members_table_provider))
  , _queue(
      scheduling_group,
      [](const std::exception_ptr& ex) {
          vlog(cllog.warn, "unexpected cluster link manager error: {}", ex);
      },
      ssx::work_queue::is_paused_t::yes)
  , _task_reconciler_interval(task_reconciler_interval)
  , _default_topic_replication(std::move(default_topic_replication))
  , _scheduling_group(scheduling_group) {}

ss::future<> manager::start() {
    co_await ss::coroutine::switch_to(_scheduling_group);
    vlog(cllog.info, "Starting cluster link manager");
    auto ids = _registry->get_all_link_ids();
    chunked_hash_map<model::id_t, ::model::revision_id> id_to_revision;
    for (const auto& id : ids) {
        auto rev = _registry->get_last_update_revision(id);
        vassert(rev.has_value(), "Link {} does not have a update revision", id);
        id_to_revision.emplace(id, *rev);
    }
    for (const auto& id : ids) {
        co_await handle_on_link_change(id, id_to_revision.at(id));
    }

    _link_task_reconciler_timer.set_callback([this] {
        ssx::spawn_with_gate(_g, [this] {
            return ss::with_scheduling_group(
              _scheduling_group, [this] { return link_task_reconciler(); });
        });
    });
    _link_task_reconciler_timer.arm_periodic(_task_reconciler_interval);
    _queue.resume();
}

ss::future<> manager::stop() {
    vlog(cllog.info, "Stopping cluster link manager");

    co_await on_controller_stepdown();

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

ss::future<err_info> manager::broker_preflight_check(
  ::model::node_id node_id, kafka::client::cluster& cluster) {
    static constexpr auto min_version_with_topic_id_support
      = kafka::api_version(10);
    static constexpr std::array<
      std::tuple<kafka::api_key, kafka::api_version, kafka::api_version>,
      7>
      apis_to_check{
        {{kafka::offset_fetch_api::key,
          kafka::offset_fetch_api::min_valid,
          // clamped as needed by group_mirroring_task
          kafka::api_version(7)},
         {kafka::list_groups_api::key,
          kafka::list_groups_api::min_valid,
          kafka::list_groups_api::max_valid},
         {kafka::find_coordinator_api::key,
          kafka::find_coordinator_api::min_valid,
          kafka::find_coordinator_api::max_valid},
         {kafka::describe_acls_api::key,
          kafka::describe_acls_api::min_valid,
          kafka::describe_acls_api::max_valid},
         {kafka::list_offsets_api::key,
          kafka::list_offsets_api::min_valid,
          kafka::list_offsets_api::max_valid},
         {kafka::describe_configs_api::key,
          kafka::describe_configs_api::min_valid,
          kafka::describe_configs_api::max_valid},
         {kafka::metadata_api::key,
          min_version_with_topic_id_support,
          kafka::metadata_api::max_valid}}};
    vlog(cllog.trace, "Performing preflight check on broker {}", node_id);
    try {
        chunked_vector<ss::sstring> unsupported_api_errors;
        unsupported_api_errors.reserve(apis_to_check.size());
        for (const auto& [api_key, min_version, max_version] : apis_to_check) {
            auto versions = co_await cluster.supported_api_versions(
              node_id, api_key);
            if (!versions) {
                vlog(
                  cllog.warn,
                  "Broker {} does not support required API {}",
                  node_id,
                  api_key);
                unsupported_api_errors.push_back(
                  fmt::format("{}: unsupported", api_key));
                continue;
            }
            // Check if the broker's supported version range overlaps with the
            // required range
            if (versions->max < min_version || versions->min > max_version) {
                vlog(
                  cllog.warn,
                  "Broker {} does not support required version range for "
                  "API {}: supported [{}, {}], required [{}, {}]",
                  node_id,
                  api_key,
                  versions->min,
                  versions->max,
                  min_version,
                  max_version);
                unsupported_api_errors.push_back(
                  fmt::format(
                    "{}: supported [{}, {}], required [{}, {}]",
                    api_key,
                    versions->min,
                    versions->max,
                    min_version,
                    max_version));
            }
        }
        if (!unsupported_api_errors.empty()) {
            co_return err_info(
              errc::link_unsupported_api_version,
              fmt::format(
                "Broker {} does not support required APIs: [{}]",
                node_id,
                fmt::join(unsupported_api_errors, ", ")));
        }
    } catch (const kafka::client::broker_error& e) {
        vlog(
          cllog.warn,
          "Broker {} preflight check failed - {}",
          node_id,
          e.what());
        co_return err_info(
          errc::link_broker_unreachable,
          fmt::format(
            "Broker {} preflight check failed - {}", node_id, e.what()));
    } catch (...) {
        vlog(
          cllog.warn,
          "Broker {} preflight check failed - {}",
          node_id,
          std::current_exception());
        co_return err_info(
          errc::link_broker_verification_failed,
          fmt::format(
            "Broker {} preflight check failed - {}",
            node_id,
            std::current_exception()));
    }
    vlog(
      cllog.trace,
      "Performed preflight check on broker {} successfully",
      node_id);
    co_return err_info{errc::success};
}

ss::future<err_info> manager::link_preflight_checks(const model::metadata& md) {
    auto cluster = _cluster_factory->create_cluster(md);
    err_info return_error{errc::success};
    auto stop_and_ignore = [&cluster, &md]() {
        return cluster->stop().handle_exception([&md](std::exception_ptr ex) {
            vlog(
              cllog.warn,
              "[Ignoring] Error stopping cluster after preflight check for "
              "link '{}' - {}",
              md.name,
              ex);
        });
    };
    try {
        co_await cluster->start();
        co_await cluster->request_metadata_update();
        if (!cluster->is_connected()) {
            vlog(
              cllog.warn,
              "Cluster link '{}' preflight check failed - unable to connect to "
              "cluster",
              md.name);
            co_await stop_and_ignore();
            co_return err_info(
              errc::link_cluster_unreachable,
              fmt::format(
                "Cluster link '{}' preflight check failed - unable to connect "
                "to cluster",
                md.name));
        }
        // there is at least one broker.
        const auto& brokers = cluster->get_brokers();
        auto reported_brokers = brokers.get_broker_ids();
        auto num_reported_brokers = reported_brokers.size();
        if (num_reported_brokers > _members_table_provider->node_count()) {
            vlog(
              cllog.warn,
              "Cluster link '{}' connecting to source cluster with {} brokers, "
              "which is more than the shadow cluster's {} nodes",
              md.name,
              num_reported_brokers,
              _members_table_provider->node_count());
        }
        chunked_vector<ss::sstring> broker_errors;
        broker_errors.reserve(brokers.size());
        co_await ss::max_concurrent_for_each(
          std::move(reported_brokers),
          10,
          [this, &cluster, &broker_errors](::model::node_id id) mutable {
              return broker_preflight_check(id, *cluster)
                .then([&broker_errors](err_info err) mutable {
                    // Handle any incompatible API versions or unexpected
                    // errors.
                    if (err.code() != errc::success) {
                        broker_errors.push_back(err.message());
                    }
                });
          });
        // Here it is possible that we may be missing some brokers
        // (if they are dead), we just deal with whatever responses
        // we have.
        // We consider it a failure if none of the brokers succeeded
        // in the preflight check. This is a loose check but should be
        // sufficient to catch misconfigurations.
        if (broker_errors.size() == num_reported_brokers) {
            return_error = err_info(
              errc::link_broker_verification_failed,
              fmt::format("[{}]", fmt::join(broker_errors, ", ")));
        }
    } catch (const std::exception& e) {
        vlog(
          cllog.warn,
          "Cluster link '{}' preflight check failed - {}",
          md.name,
          e.what());
        return_error = {
          errc::link_cluster_unreachable,
          fmt::format(
            "Cluster link '{}' unreachable, preflight check "
            "failed - {}",
            md.name,
            e.what())};
    }
    co_await stop_and_ignore();
    co_return return_error;
}

ss::future<cl_result<model::metadata_ptr>>
manager::upsert_cluster_link(model::metadata md) {
    static constexpr auto wait_for_link_creation_timeout = 30s;
    auto hold = _g.hold();
    auto name = md.name;
    vlog(cllog.info, "Attempting to create cluster link named '{}'", md.name);
    vlog(cllog.trace, "Cluster link metadata: {}", md);
    auto preflight_err = co_await link_preflight_checks(md);
    if (preflight_err.code() != errc::success) {
        co_return preflight_err;
    }

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
              return _registry->find_link_by_name(name) != nullptr;
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

    co_return metadata_resp;
}

cl_result<model::metadata_ptr>
manager::get_cluster_link(const model::name_t& name) {
    auto metadata_resp = _registry->find_link_by_name(name);
    if (!metadata_resp) {
        return err_info(
          errc::link_id_not_found,
          fmt::format("Failed to find cluster link with name '{}'", name));
    }
    return metadata_resp;
}

cl_result<chunked_vector<model::metadata_ptr>> manager::list_cluster_links() {
    auto link_ids = _registry->get_all_link_ids();
    chunked_vector<model::metadata_ptr> resp;
    resp.reserve(link_ids.size());

    for (const auto id : link_ids) {
        auto maybe_md = _registry->find_link_by_id(id);
        if (!maybe_md) {
            vlog(cllog.warn, "Failed to find link ID {}", id);
            continue;
        }

        resp.emplace_back(maybe_md);
    }

    return resp;
}

ss::future<cl_result<model::metadata_ptr>> manager::update_cluster_link(
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

    co_return metadata_resp;
}

ss::future<cl_result<model::metadata_ptr>> manager::update_mirror_topic_status(
  model::name_t link_name,
  ::model::topic topic,
  model::mirror_topic_status status,
  bool force_update) {
    static constexpr auto model_timeout = 30s;
    auto hold = _g.hold();
    vlog(
      cllog.info,
      "Attempting to update mirror topic '{}' status on link '{}' to status: "
      "{} (forced: {})",
      topic,
      link_name,
      status,
      force_update);
    const auto link_id = _registry->find_link_id_by_name(link_name);
    if (!link_id.has_value()) {
        co_return err_info{
          errc::link_id_not_found,
          ssx::sformat("Unable to find link by name '{}'", link_name)};
    }
    model::update_mirror_topic_status_cmd cmd;
    cmd.topic = topic;
    cmd.status = status;
    cmd.force_update = model::update_mirror_topic_status_cmd::force_update_t{
      force_update};
    auto ec = co_await _registry->update_mirror_topic_state(
      *link_id, std::move(cmd), ::model::timeout_clock::now() + model_timeout);
    auto err = map_cluster_errc(ec);
    if (err != errc::success) {
        co_return err_info(
          err,
          fmt::format(
            "Failed to update mirror topic '{}' status on link '{}': {}",
            topic,
            *link_id,
            ec));
    }
    auto metadata_resp = _registry->find_link_by_id(*link_id);
    if (!metadata_resp) {
        co_return err_info(
          errc::link_id_not_found,
          fmt::format("Failed to find cluster link with id '{}'", *link_id));
    }
    co_return metadata_resp;
}

ss::future<cl_result<model::metadata_ptr>>
manager::failover_link_topics(model::name_t link_name) {
    static constexpr auto failover_command_timeout = 5s;
    auto hold = _g.hold();
    vlog(
      cllog.info,
      "Attempting to failover all mirror topics on link '{}'",
      link_name);
    const auto link_id = _registry->find_link_id_by_name(link_name);
    if (!link_id.has_value()) {
        co_return err_info{
          errc::link_id_not_found,
          ssx::sformat("Unable to find link by name '{}'", link_name)};
    }
    auto ec = co_await _registry->failover_link_topics(
      *link_id, failover_command_timeout);
    auto err = map_cluster_errc(ec);
    if (err != errc::success) {
        co_return err_info(
          err,
          fmt::format(
            "Failed to failover all mirror topics on link '{}': {}",
            *link_id,
            ec));
    }
    auto metadata_resp = _registry->find_link_by_id(*link_id);
    if (!metadata_resp) {
        co_return err_info(
          errc::link_id_not_found,
          fmt::format("Failed to find cluster link with id '{}'", *link_id));
    }
    co_return metadata_resp;
}

ss::future<cl_result<void>>
manager::delete_cluster_link(model::name_t name, bool force_delete_link) {
    vlog(cllog.info, "Attempting to delete cluster link named '{}'", name);
    auto hold = _g.hold();
    auto cl_resp = get_cluster_link(name);
    if (cl_resp.has_error()) {
        co_return cl_resp.assume_error();
    }

    const auto is_active = [](const model::mirror_topic_status s) {
        switch (s) {
        case model::mirror_topic_status::active:
        case model::mirror_topic_status::paused:
        case model::mirror_topic_status::failing_over:
        case model::mirror_topic_status::promoting:
            return true;
        case model::mirror_topic_status::failed:
        case model::mirror_topic_status::promoted:
        case model::mirror_topic_status::failed_over:
            return false;
        }
    };

    const auto mirror_topic_states = cl_resp.assume_value()->state.mirror_topics
                                     | std::views::values
                                     | std::views::transform(
                                       &model::mirror_topic_metadata::status);

    auto active_shadow_topics = std::ranges::any_of(
      mirror_topic_states, is_active);

    if (active_shadow_topics) {
        if (!force_delete_link) {
            co_return err_info(
              errc::link_has_active_shadow_topics,
              fmt::format(
                "Failed to delete cluster link with name '{}'. There are "
                "active/promoting shadow topics.",
                name));
        }
        vlog(
          cllog.info,
          "Force deleting link \"{}\" with active shadow topics",
          name);
    }

    auto ec = co_await _registry->delete_link(
      std::move(name), force_delete_link, ::model::timeout_clock::now() + 30s);
    auto err = map_cluster_errc(ec);
    if (err != errc::success) {
        co_return err_info(
          err, fmt::format("Failed to delete cluster link: {}", ec));
    }

    co_return outcome::success();
}

ss::future<cl_result<model::metadata_ptr>>
manager::remove_shadow_topic_from_link(
  model::name_t link_name, ::model::topic shadow_topic) {
    vlog(
      cllog.info,
      "Attempting to remove Shadow Topic '{}' from Shadow Link '{}'",
      shadow_topic,
      link_name);
    auto hold = _g.hold();
    auto link_id = _registry->find_link_id_by_name(link_name);
    if (!link_id.has_value()) {
        co_return err_info{
          errc::link_id_not_found,
          ssx::sformat("Unable to find link by name '{}'", link_name)};
    }

    model::delete_mirror_topic_cmd cmd;
    cmd.topic = shadow_topic;

    auto ec = co_await _registry->delete_shadow_topic(
      *link_id, std::move(cmd), ::model::timeout_clock::now() + 30s);

    auto err = map_cluster_errc(ec);
    if (err != errc::success) {
        co_return err_info(
          err,
          fmt::format(
            "Failed to delete shadow topic '{}' from link '{}': {}",
            shadow_topic,
            link_name,
            ec));
    }

    co_return get_cluster_link(link_name);
}

void manager::on_link_change(model::id_t id, ::model::revision_id revision) {
    vlog(cllog.trace, "Cluster link with id={} has changed", id);
    if (_topic_reconciler && _is_controller_leader) {
        _topic_reconciler->trigger(id);
    }
    _queue.submit(
      [this, id, revision] { return handle_on_link_change(id, revision); });
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

ss::future<>
manager::handle_on_link_change(model::id_t id, ::model::revision_id revision) {
    static constexpr auto retry_delay = 10s;

    vlog(
      cllog.trace,
      "Handling cluster link change for id={}, revision={}",
      id,
      revision);
    auto notify_reconciler = ss::defer([this] {
        if (_link_status_reconciler) {
            _link_status_reconciler->reconcile();
        }
    });
    auto link_metadata = _registry->find_link_by_id(id);
    if (!link_metadata) {
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

    auto it = _links.find(id);
    if (it != _links.end()) {
        // Link already exists, update its configuration
        vlog(
          cllog.debug,
          "Updating cluster link id={} with new config: {}",
          id,
          link_metadata);
        it->second->update_config(link_metadata, revision);
        _cfg_change_notifications.notify(id, link_metadata);
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
              link_metadata,
              _cluster_factory->create_cluster(*link_metadata));
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
            _queue.submit_delayed(retry_delay, [this, id, revision] {
                return handle_on_link_change(id, revision);
            });
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

security_service& manager::get_security_service() noexcept {
    return *_security_service;
}

partition_manager& manager::partition_manager() noexcept {
    return *_partition_manager;
}

const partition_manager& manager::partition_manager() const noexcept {
    return *_partition_manager;
}

topic_creator& manager::topic_creator() noexcept { return *_topic_creator; }

cl_result<chunked_hash_map<::model::ntp, replication::partition_offsets_report>>
manager::get_partition_offsets_report_for_link(
  const model::name_t& name) const {
    auto link_id = _registry->find_link_id_by_name(name);
    if (!link_id) {
        return err_info(
          errc::link_id_not_found,
          ssx::sformat("Unable to find link by name '{}'", name));
    }
    return get_partition_offsets_report_for_link(*link_id);
}

cl_result<chunked_hash_map<::model::ntp, replication::partition_offsets_report>>
manager::get_partition_offsets_report_for_link(model::id_t link_id) const {
    auto link_it = _links.find(link_id);
    if (link_it == _links.end()) {
        return err_info(
          errc::link_id_not_found,
          ssx::sformat("Link with id '{}' not found", link_id));
    }
    return link_it->second->get_partition_offsets_report();
}

members_table_provider& manager::get_members_table_provider() noexcept {
    return *_members_table_provider;
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
          link->get_config()->name,
          link->get_config()->uuid);
        for (const auto& task_factory : _task_factories) {
            auto task_name = task_factory->created_task_name();
            if (!link->task_is_registered(task_name)) {
                vlog(
                  cllog.debug,
                  "Registering task {} for cluster link {} ({})",
                  task_name,
                  link->get_config()->name,
                  link->get_config()->uuid);
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
            vassert(term.has_value(), "term must be set when becoming leader");
            _is_controller_leader = ntp_leader::yes;
            co_await on_controller_leadership(term.value());
        }
        if (is_ntp_leader == ntp_leader::no && _is_controller_leader) {
            _is_controller_leader = ntp_leader::no;
            co_await on_controller_stepdown();
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
  model::id_t link_id, model::update_mirror_topic_status_cmd cmd) {
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

cl_result<model::link_task_status_report>
manager::get_task_status_report(model::id_t link_id) const {
    const auto it = _links.find(link_id);
    if (it == _links.end()) {
        return err_info(
          errc::link_id_not_found,
          ssx::sformat("Link with id '{}' not found", link_id));
    }

    return it->second->get_task_status_report();
}

ss::future<> manager::on_controller_leadership(::model::term_id term) {
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
          _default_topic_replication,
          _scheduling_group);
    }
    try {
        co_await _topic_reconciler->start();
        vlog(cllog.debug, "Topic reconciler has started");
    } catch (const std::exception& e) {
        vlog(cllog.error, "Failed to start topic reconciler: {}", e);
        // If it fails to start, enqueue a retry to start the reconciler
        _queue.submit_delayed(
          10s, [this, term] { return on_controller_leadership(term); });
    }
    if (!_link_status_reconciler) {
        _link_status_reconciler = std::make_unique<link_status_reconciler>(
          _registry.get(), term);
        try {
            co_await _link_status_reconciler->start();
        } catch (const std::exception& e) {
            vlog(cllog.error, "Failed to start link status reconciler: {}", e);
            // If it fails to start, enqueue a retry to start the reconciler
            _queue.submit_delayed(
              10s, [this, term] { return on_controller_leadership(term); });
        }
    }
}

ss::future<> manager::on_controller_stepdown() {
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
              10s, [this] { return on_controller_stepdown(); });
        }
    }
    if (_link_status_reconciler) {
        vlog(cllog.trace, "Stopping link status reconciler");
        try {
            co_await _link_status_reconciler->stop();
            _link_status_reconciler.reset();
            vlog(cllog.debug, "Link status reconciler has stopped");
        } catch (const std::exception& e) {
            vlog(cllog.error, "Failed to stop link status reconciler: {}", e);
            // If it fails to start, enqueue a retry to start the reconciler
            _queue.submit_delayed(
              10s, [this] { return on_controller_stepdown(); });
        }
    }
}

manager::notification_id manager::register_link_config_changes_callback(
  link_cfg_change_notification_cb cb) {
    return _cfg_change_notifications.register_cb(std::move(cb));
}

void manager::unregister_link_config_changes_callback(notification_id id) {
    _cfg_change_notifications.unregister_cb(id);
}

consumer_groups_router& manager::get_group_router() noexcept {
    return *_group_router;
}

partition_metadata_provider&
manager::get_partition_metadata_provider() noexcept {
    return *_partition_metadata_provider;
}

kafka_rpc_client_service& manager::get_kafka_rpc_client_service() noexcept {
    return *_kafka_rpc_client_service;
}
} // namespace cluster_link
