

#include "cluster_link/group_mirroring_task.h"

#include "cluster_link/link.h"
#include "cluster_link/model/filter_utils.h"
#include "kafka/protocol/errors.h"
#include "ssx/async_algorithm.h"
#include "ssx/future-util.h"

namespace cluster_link {

group_mirroring_task::group_mirroring_task(
  link* link, const model::metadata& link_metadata)
  : task(
      link,
      link_metadata.configuration.consumer_groups_mirroring_cfg
        .get_task_interval(),
      group_mirroring_task::task_name)

  , _config(link_metadata.configuration.consumer_groups_mirroring_cfg.copy()) {}

void group_mirroring_task::update_config(const model::metadata& link_metadata) {
    _config = link_metadata.configuration.consumer_groups_mirroring_cfg.copy();
    set_run_interval(_config.get_task_interval());
}

model::enabled_t group_mirroring_task::is_enabled() const {
    return _config.is_enabled;
}

namespace {

chunked_hash_set<::model::partition_id>
coordinators_on_current_shard(link& link, ss::shard_id, ::model::node_id) {
    auto topic_cfg = link.topic_metadata_cache().find_topic_cfg(
      ::model::kafka_consumer_offsets_nt);
    chunked_hash_set<::model::partition_id> ret;
    if (!topic_cfg.has_value()) {
        return ret;
    }
    for (::model::partition_id p_id{0};
         p_id < ::model::partition_id(topic_cfg->partition_count);
         ++p_id) {
        ::model::ktp ktp(::model::kafka_consumer_offsets_topic, p_id);
        const auto is_leader = link.partition_manager().is_current_shard_leader(
          ktp);
        if (is_leader) {
            ret.insert(p_id);
        }
    }

    return ret;
}
} // namespace

ss::future<task::state_transition>
group_mirroring_task::run_impl(ss::abort_source& as) {
    auto result = co_await list_consumer_groups();

    if (!result.has_value()) {
        co_return make_unavailable(result.error());
    }

    as.check();

    if (_needs_coordinator_update) {
        auto result = co_await update_group_coordinators();

        if (!result.has_value()) {
            co_return make_unavailable(result.error());
        }
    }

    as.check();

    result = co_await synchronize_consumer_groups_offsets();

    if (!result.has_value()) {
        co_return make_unavailable(result.error());
    }

    co_return make_active();
};

bool group_mirroring_task::should_start_impl(
  ss::shard_id current_shard, ::model::node_id node_id) const {
    return !coordinators_on_current_shard(*get_link(), current_shard, node_id)
              .empty();
}

bool group_mirroring_task::should_stop_impl(
  ss::shard_id current_shard, ::model::node_id node_id) const {
    return coordinators_on_current_shard(*get_link(), current_shard, node_id)
      .empty();
}

ss::future<group_mirroring_task::result_t>
group_mirroring_task::list_consumer_groups() {
    vlog(logger().trace, "Refreshing consumer group list");
    try {
        auto broker_ids = get_cluster_connection().get_broker_ids();
        if (broker_ids.empty()) {
            vlog(
              logger().info,
              "No brokers available for listing consumer groups");
            co_await get_cluster_connection().request_metadata_update();
            co_return std::unexpected{error("No brokers available")};
        }

        size_t error_cnt = 0;
        auto all_groups = co_await ssx::parallel_transform(
          broker_ids, [this, &error_cnt](::model::node_id id) {
              return list_groups_from_broker(id).then(
                [this, id, &error_cnt](
                  std::expected<chunked_vector<kafka::group_id>, error>
                    result) {
                    if (result.has_value()) {
                        return std::move(result.value());
                    }
                    vlog(
                      logger().warn,
                      "Failed to list groups from broker {}: {}",
                      id,
                      result.error());
                    // even if one broker is unavailable the mirroring should
                    // still continue
                    error_cnt++;
                    return chunked_vector<kafka::group_id>{};
                });
          });
        const auto current_shard_coordinators = coordinators_on_current_shard(
          *get_link(), ss::this_shard_id(), get_link()->self());
        chunked_hash_set<kafka::group_id> discovered_groups;
        std::ranges::for_each(
          all_groups | std::views::join, [&](const kafka::group_id& group) {
              discovered_groups.insert(group);
          });
        // we only want to remove groups if there were no errors when listing
        // groups
        if (error_cnt == 0) {
            // erase all groups that are no longer discovered
            std::erase_if(
              _groups_to_mirror, [this, &discovered_groups](const auto& entry) {
                  auto remove = !discovered_groups.contains(entry.first);
                  if (remove) {
                      vlog(
                        logger().debug,
                        "Group {} is no longer present on the remote cluster, "
                        "removing it from mirroring",
                        entry.first,
                        entry.second.coordinator_id.has_value());
                  }
                  return remove;
              });
        }

        if (error_cnt == broker_ids.size()) {
            vlog(
              logger().info,
              "All brokers returned an error when listing groups");
            co_return std::unexpected{
              error("All brokers returned an error when listing groups")};
        }

        co_await ssx::async_for_each(
          std::move(discovered_groups),
          [this, &current_shard_coordinators](kafka::group_id group_id) {
              if (!should_group_be_mirrored(
                    group_id, current_shard_coordinators)) {
                  vlog(
                    logger().trace,
                    "Group {} is not eligible for mirroring, current shard "
                    "coordinators: {}",
                    group_id,
                    fmt::join(current_shard_coordinators, ", "));
                  _groups_to_mirror.erase(group_id);
              } else {
                  auto [_, inserted] = _groups_to_mirror.try_emplace(
                    group_id, group_metadata{});
                  _needs_coordinator_update |= inserted;
                  vlog(
                    logger().trace,
                    "Group {} is eligible for mirroring, (newly discovered: "
                    "{})",
                    group_id,
                    inserted);
              }
          });
        co_return result_t{};
    } catch (...) {
        co_return std::unexpected{error(
          ssx::sformat(
            "Exception thrown while listing groups - {}",
            std::current_exception()))};
    }
}

bool group_mirroring_task::should_group_be_mirrored(
  const kafka::group_id& group_id,
  const chunked_hash_set<::model::partition_id>& current_shard_coordinators) {
    if (!model::select_group(group_id, _config.filters)) {
        return false;
    }
    auto maybe_partition = get_link()->get_group_router().partition_for(
      group_id);

    if (!maybe_partition.has_value()) {
        return false;
    }
    return current_shard_coordinators.contains(*maybe_partition);
}

task::state_transition
group_mirroring_task::make_unavailable(const error& err) {
    vlog(logger().warn, "Group mirroring task failed: {}", err);

    return state_transition{
      .desired_state = model::task_state::link_unavailable,
      .reason = fmt::format("{}", err)};
}

task::state_transition group_mirroring_task::make_active() {
    vlog(logger().trace, "Group mirroring task finished successfully");

    return state_transition{
      .desired_state = model::task_state::active,
      .reason = "Group mirroring task finished successfully"};
}

ss::future<group_mirroring_task::result_t>
group_mirroring_task::synchronize_consumer_groups_offsets() {
    // group by coordinator id
    chunked_hash_map<::model::node_id, chunked_vector<kafka::group_id>>
      groups_by_coordinator;
    for (auto& [g_id, metadata] : _groups_to_mirror) {
        if (!metadata.coordinator_id.has_value()) {
            continue;
        }
        groups_by_coordinator[*metadata.coordinator_id].push_back(g_id);
    }
    // no groups, do nothing
    if (groups_by_coordinator.empty()) {
        co_return result_t{};
    }

    auto total_requests = groups_by_coordinator.size();
    chunked_vector<error> errors;
    co_await ss::max_concurrent_for_each(
      std::move(groups_by_coordinator),
      concurrent_requests_limit,
      [this, &errors](auto& pair) {
          return fetch_and_commit_offsets(std::move(pair.second), pair.first)
            .then([&errors](result_t result) {
                if (!result) {
                    errors.push_back(std::move(result.error()));
                }
            });
      });
    if (errors.size() == total_requests) {
        co_return std::unexpected{error(
          ssx::sformat(
            "all requests to fetch and commit offsets failed: {}",
            fmt::join(errors, ", ")))};
    }
    co_return result_t{};
}

ss::future<group_mirroring_task::result_t>
group_mirroring_task::fetch_and_commit_offsets(
  chunked_vector<kafka::group_id> groups, ::model::node_id coordinator_id) {
    vlog(
      logger().trace,
      "Fetching offsets for {} groups from: {}",
      groups.size(),
      coordinator_id);
    auto result = co_await fetch_offsets(std::move(groups), coordinator_id);
    if (!result) {
        co_return std::unexpected{std::move(result.error())};
    }
    auto trimmed_offsets = co_await trim_to_partition_highwatermark(
      std::move(result.value()));

    co_await commit_group_offsets(std::move(trimmed_offsets));
    co_return result_t{};
}

namespace {
ss::future<kafka::offset_commit_request>
build_offset_commit_request(group_mirroring_task::group_offsets g_offsets) {
    ssx::async_counter cnt;
    kafka::offset_commit_request req;
    req.data.group_id = std::move(g_offsets.group_id);
    req.data.topics.reserve(g_offsets.topic_offsets.size());

    for (auto& topic_offsets : g_offsets.topic_offsets) {
        kafka::offset_commit_request_topic t;
        t.name = std::move(topic_offsets.topic);
        t.partitions.reserve(topic_offsets.partition_offsets.size());
        co_await ssx::async_for_each_counter(
          cnt,
          std::move(topic_offsets.partition_offsets),
          [&t](auto& partition_offset) {
              kafka::offset_commit_request_partition p;
              p.partition_index = partition_offset.partition;
              p.committed_offset = kafka::offset_cast(
                partition_offset.committed_offset);
              // do not pass leader epoch information to target cluster as the
              // epochs are not preserved
              p.committed_leader_epoch = kafka::leader_epoch(-1);
              t.partitions.push_back(std::move(p));
          });
        req.data.topics.push_back(std::move(t));
    }

    co_return req;
}
template<typename ApiT>
kafka::api_version
get_max_supported(kafka::client::api_version_range remote_supported) {
    constexpr auto client_supported_max = [] {
        if constexpr (std::same_as<ApiT, kafka::offset_fetch_api>) {
            return kafka::api_version{7};
        } else {
            return ApiT::max_valid;
        }
    }();
    return std::min(remote_supported.max, client_supported_max);
}

kafka::list_groups_request make_list_groups_request() {
    kafka::list_groups_request req;
    req.data.states_filter.reserve(4);
    // we want all groups, no filtering
    req.data.states_filter.emplace_back(kafka::group_state_name_empty);
    req.data.states_filter.emplace_back(
      kafka::group_state_name_preparing_rebalance);
    req.data.states_filter.emplace_back(kafka::group_state_name_stable);
    req.data.states_filter.emplace_back(
      kafka::group_state_name_completing_rebalance);

    return req;
}
} // namespace

ss::future<> group_mirroring_task::commit_group_offsets(
  chunked_vector<group_offsets> offsets) {
    co_await ss::max_concurrent_for_each(
      std::move(offsets), concurrent_requests_limit, [this](group_offsets& go) {
          return build_offset_commit_request(std::move(go))
            .then([this](kafka::offset_commit_request req) {
                auto group = req.data.group_id;
                vlog(logger().trace, "Committing {} group offsets", group);
                return get_link()
                  ->get_group_router()
                  .offset_commit(std::move(req))
                  .then([this, group = std::move(group)](
                          kafka::offset_commit_response resp) mutable {
                      handle_offset_commit_response(
                        std::move(group), std::move(resp));
                  });
            });
      });
}

void group_mirroring_task::handle_offset_commit_response(
  kafka::group_id group, kafka::offset_commit_response resp) {
    for (const auto& tp : resp.data.topics) {
        for (const auto& p : tp.partitions) {
            if (p.errored()) [[unlikely]] {
                vlog(
                  logger().warn,
                  "Failed to commit offset for group {}, "
                  "partition {}/{}, error: {}",
                  group,
                  tp.name,
                  p.partition_index,
                  p.error_code);
            }
        }
    }
}

ss::future<chunked_vector<group_mirroring_task::group_offsets>>
group_mirroring_task::trim_to_partition_highwatermark(
  chunked_vector<group_offsets> offsets) {
    chunked_vector<group_offsets> trimmed_offsets;
    trimmed_offsets.reserve(offsets.size());

    for (auto& g_offsets : offsets) {
        group_offsets trimmed_g_offsets;
        trimmed_g_offsets.group_id = std::move(g_offsets.group_id);
        trimmed_g_offsets.topic_offsets.reserve(g_offsets.topic_offsets.size());
        for (auto& t_offsets : g_offsets.topic_offsets) {
            topic_offsets trimmed_t_offsets;
            trimmed_t_offsets.topic = std::move(t_offsets.topic);
            trimmed_t_offsets.partition_offsets.reserve(
              t_offsets.partition_offsets.size());
            for (auto& po : t_offsets.partition_offsets) {
                auto maybe_hw = co_await get_partition_high_watermark(
                  trimmed_t_offsets.topic, po.partition);
                vlog(
                  logger().trace,
                  "Offset trimming for group {}, partition: {}/{}, committed "
                  "offset: {}, partition hwm: {}",
                  trimmed_g_offsets.group_id,
                  trimmed_t_offsets.topic,
                  po.partition,
                  po.committed_offset,
                  maybe_hw);
                if (!maybe_hw.has_value()) {
                    vlog(
                      logger().debug,
                      "Group: {}, no high watermark for partition {}/{}, "
                      "skipping offset commit",
                      trimmed_g_offsets.group_id,
                      trimmed_t_offsets.topic,
                      po.partition);
                    continue;
                }
                trimmed_t_offsets.partition_offsets.push_back(
                  partition_committed_offset{
                    .partition = po.partition,
                    .committed_offset = std::min(
                      *maybe_hw, po.committed_offset),
                  });
            }
            if (!trimmed_t_offsets.partition_offsets.empty()) {
                trimmed_g_offsets.topic_offsets.push_back(
                  std::move(trimmed_t_offsets));
            }
        }
        if (!trimmed_g_offsets.topic_offsets.empty()) {
            trimmed_offsets.push_back(std::move(trimmed_g_offsets));
        }
    }

    co_return std::move(trimmed_offsets);
}
ss::future<std::optional<kafka::offset>>
group_mirroring_task::get_partition_high_watermark(
  const ::model::topic& topic, ::model::partition_id p_id) {
    auto& metadata_provider = get_link()->get_partition_metadata_provider();
    auto hr_hwm = co_await metadata_provider.get_partition_high_watermark(
      ::model::topic_partition_view(topic, p_id));

    /**
     * Check if the replica for partition is present on current node, if it is
     * then query for its high watermark as it is most likely more up to date
     * than the one from the health report.
     */

    auto& partition_manager = get_link()->partition_manager();
    ::model::ktp ktp(topic, p_id);
    auto owning_shard = partition_manager.shard_owner(
      ktp); // ensure metadata is loaded
    if (!owning_shard.has_value()) {
        co_return hr_hwm;
    }

    if (owning_shard) {
        vlog(
          logger().trace,
          "Fetching high watermark for partition {}/{} from replica on shard "
          "{}",
          topic,
          p_id,
          *owning_shard);
        auto hw = co_await partition_manager.invoke_on_shard(
          *owning_shard,
          ktp,
          [](kafka::partition_proxy* pp) {
              return ssx::now<::result<::model::offset, cluster::errc>>(
                pp->high_watermark());
          },
          kafka::data::rpc::require_leader::no);
        if (hw) {
            auto replica_hwm = ::model::offset_cast(hw.value());
            if (hr_hwm) {
                co_return std::max(replica_hwm, hr_hwm.value());
            }
            co_return replica_hwm;
        }
        vlog(
          logger().info,
          "Failed to get high watermark for partition {}/{} from replica - "
          "{}",
          topic,
          p_id,
          hw.error());
    }
    co_return hr_hwm;
}
ss::future<std::expected<
  chunked_vector<group_mirroring_task::group_offsets>,
  group_mirroring_task::error>>
group_mirroring_task::fetch_offsets(
  chunked_vector<kafka::group_id> groups, ::model::node_id coordinator) {
    auto& c_connection = get_cluster_connection();

    auto versions = co_await c_connection.supported_api_versions(
      coordinator, kafka::offset_fetch_api::key);
    chunked_vector<group_offsets> ret;
    if (!versions) {
        co_return std::unexpected<error>{ssx::sformat(
          "Broker {} does not support offset fetch API", coordinator)};
    }

    ret.reserve(groups.size());
    auto version = get_max_supported<kafka::offset_fetch_api>(*versions);
    // TODO: handle new version where we can fetch offsets for multiple groups
    // at once.
    const auto group_count = groups.size();
    size_t error_cnt = 0;
    co_await ss::max_concurrent_for_each(
      std::move(groups),
      concurrent_requests_limit,
      [this, &ret, version, coordinator, &c_connection, &error_cnt](
        kafka::group_id& group_id) {
          kafka::offset_fetch_request req;
          req.data.group_id = group_id;
          req.data.topics = std::nullopt; // no topics filtering
          return c_connection.dispatch_to(coordinator, std::move(req), version)
            .then([this, &ret, group_id, &error_cnt](
                    kafka::offset_fetch_response resp) mutable {
                return process_single_group_fetch_offsets_response(
                         group_id, std::move(resp))
                  .then([&ret, &error_cnt, group_id](
                          std::expected<chunked_vector<topic_offsets>, error>
                            offsets) mutable {
                      if (!offsets) {
                          error_cnt++;
                      }
                      ret.push_back(
                        group_offsets{
                          .group_id = std::move(group_id),
                          .topic_offsets = std::move(offsets.value())});
                  });
            })
            .handle_exception(
              [this, group_id, &error_cnt](const std::exception_ptr& e) {
                  vlog(
                    logger().warn,
                    "Failed to fetch offsets for group {} - {}",
                    group_id,
                    e);
                  error_cnt++;
              });
      });

    if (error_cnt == group_count) {
        co_return std::unexpected<error>{
          ssx::sformat("All requests to fetch offsets failed")};
    }

    co_return ret;
}

ss::future<std::expected<
  chunked_vector<group_mirroring_task::topic_offsets>,
  group_mirroring_task::error>>
group_mirroring_task::process_single_group_fetch_offsets_response(
  const kafka::group_id& group_id, kafka::offset_fetch_response resp) {
    chunked_vector<topic_offsets> ret;
    if (resp.data.error_code != kafka::error_code::none) {
        if (kafka::is_retriable(resp.data.error_code)) {
            _needs_coordinator_update = true;
            vlog(
              logger().info,
              "Failed to fetch offsets for group {}, error_code: {}",
              group_id,
              resp.data.error_code);
        } else {
            co_return std::unexpected<error>{ssx::sformat(
              "Failed to fetch offsets for group {}, error_code: {}",
              group_id,
              resp.data.error_code)};
        }

        co_return ret;
    }
    ret.reserve(resp.data.topics.size());
    ssx::async_counter counter;
    for (auto& t : resp.data.topics) {
        topic_offsets t_offsets;
        t_offsets.topic = std::move(t.name);
        t_offsets.partition_offsets.reserve(t.partitions.size());
        co_await ssx::async_for_each_counter(
          counter,
          std::move(t.partitions),
          [&t_offsets, this, &group_id](
            kafka::offset_fetch_response_partition& p) {
              if (p.error_code != kafka::error_code::none) {
                  if (kafka::is_retriable(p.error_code)) {
                      _needs_coordinator_update = true;
                  }
                  vlogl(
                    logger(),
                    kafka::is_retriable(p.error_code) ? ss::log_level::info
                                                      : ss::log_level::warn,
                    "Failed to fetch offsets for group {}, partition {}/{}"
                    "error_code: {}",
                    group_id,
                    t_offsets.topic,
                    p.partition_index,
                    p.error_code);
                  return;
              }
              t_offsets.partition_offsets.push_back(
                partition_committed_offset{
                  .partition = p.partition_index,
                  .committed_offset = ::model::offset_cast(p.committed_offset),
                });
          });
        ret.push_back(std::move(t_offsets));
    }

    co_return ret;
}

ss::future<
  std::expected<chunked_vector<kafka::group_id>, group_mirroring_task::error>>
group_mirroring_task::list_groups_from_broker(::model::node_id broker_id) {
    kafka::list_groups_request req;
    auto& c_connection = get_cluster_connection();

    auto versions = co_await c_connection.supported_api_versions(
      broker_id, kafka::list_groups_api::key);
    if (!versions) {
        vlog(
          logger().warn,
          "Broker {} does not support list groups API",
          broker_id);
        co_return std::unexpected<error>(ssx::sformat(
          "Broker {} does not support list groups API", broker_id));
    }
    try {
        auto reply = co_await c_connection.dispatch_to(
          broker_id,
          make_list_groups_request(),
          get_max_supported<kafka::list_groups_api>(*versions));

        if (reply.data.error_code != kafka::error_code::none) {
            vlog(
              logger().warn,
              "Failed to list groups from broker {}: kafka api error: {}",
              broker_id,
              reply.data.error_code);
            co_return std::unexpected<error>(ssx::sformat(
              "Failed to list groups from {}", reply.data.error_code));
        }
        chunked_vector<kafka::group_id> groups;
        groups.reserve(reply.data.groups.size());
        for (const auto& group : reply.data.groups) {
            groups.push_back(group.group_id);
        }
        co_return std::move(groups);
    } catch (...) {
        auto lvl = ssx::is_shutdown_exception(std::current_exception())
                     ? ss::log_level::trace
                     : ss::log_level::warn;
        vlogl(
          logger(),
          lvl,
          "Exception thrown while listing groups from broker {} - {}",
          broker_id,
          std::current_exception());

        co_return std::unexpected<error>(ssx::sformat(
          "Exception thrown while listing groups - {}",
          std::current_exception()));
    }
}

ss::future<group_mirroring_task::result_t>
group_mirroring_task::update_group_coordinators() {
    static constexpr kafka::api_version batched_find_coordinator_v{4};
    auto& c_connection = get_cluster_connection();

    auto versions = co_await c_connection.supported_api_versions(
      kafka::find_coordinator_api::key);
    if (!versions) {
        co_return std::unexpected<error>(
          "Remote cluster does not support find coordinator API");
    }
    bool errored = false;
    const auto max_supported_v = get_max_supported<kafka::find_coordinator_api>(
      *versions);
    if (max_supported_v >= batched_find_coordinator_v) {
        chunked_vector<ss::sstring> group_ids;
        group_ids.reserve(_groups_to_mirror.size());
        std::ranges::transform(
          _groups_to_mirror,
          std::back_inserter(group_ids),
          [](const auto& entry) { return entry.first(); });
        auto coordinators = co_await do_find_coordinator_batched(
          std::move(group_ids), max_supported_v);
        if (!coordinators.has_value()) {
            co_return std::unexpected{std::move(coordinators.error())};
        }

        for (auto& coord : coordinators.value()) {
            if (coord.error_code != kafka::error_code::none) {
                vlog(
                  logger().warn,
                  "Error finding coordinator for {} - {}",
                  coord.key,
                  coord.error_code);
                errored = true;
                continue;
            }
            auto it = _groups_to_mirror.find(kafka::group_id(coord.key));
            if (it != _groups_to_mirror.end()) {
                it->second.coordinator_id = coord.node_id;
                vlog(
                  logger().trace,
                  "Group {} coordinator discovered: {}",
                  coord.key,
                  coord.node_id);
            }
        }

    } else {
        co_await ss::max_concurrent_for_each(
          _groups_to_mirror.begin(),
          _groups_to_mirror.end(),
          concurrent_requests_limit,
          [this, &errored, max_supported_v](auto& entry) {
              return do_find_coordinator(entry.first, max_supported_v)
                .then(
                  [this, &entry, &errored](
                    std::expected<::model::node_id, group_mirroring_task::error>
                      result) {
                      if (result.has_value()) {
                          vlog(
                            logger().trace,
                            "Group {} coordinator discovered: {}",
                            entry.first,
                            *result);
                          entry.second.coordinator_id = *result;
                      } else {
                          vlog(
                            logger().warn,
                            "Failed to find coordinator for group {} - {}",
                            entry.first,
                            result.error());
                          errored = true;
                      }
                  });
          });
    }
    _needs_coordinator_update = errored;
    co_return result_t{};
}

kafka::client::cluster& group_mirroring_task::get_cluster_connection() {
    return get_link()->get_cluster_connection();
}

ss::future<std::expected<::model::node_id, group_mirroring_task::error>>
group_mirroring_task::do_find_coordinator(
  const kafka::group_id& group_id, kafka::api_version max_version) {
    vlog(logger().trace, "Requesting find coordinator for group: {}", group_id);
    kafka::find_coordinator_request req;
    req.data.key_type = kafka::coordinator_type::group;
    req.data.key = group_id;
    // limit version to 3 as the next one use batched api
    try {
        auto resp = co_await get_cluster_connection().dispatch_to_any(
          std::move(req),
          std::min(max_version, std::min(max_version, kafka::api_version{3})));

        if (resp.data.error_code != kafka::error_code::none) {
            vlog(
              logger().warn,
              "Error finding coordinator for {} - {}",
              group_id,
              resp.data.error_code);
            co_return std::unexpected(error(
              ssx::sformat("Error finding coordinator for {}", group_id),
              resp.data.error_code));
        }
        co_return resp.data.node_id;
    } catch (...) {
        auto lvl = ssx::is_shutdown_exception(std::current_exception())
                     ? ss::log_level::trace
                     : ss::log_level::warn;
        vlogl(
          logger(),
          lvl,
          "Exception thrown while finding coordinator for group {} - {}",
          group_id,
          std::current_exception());
        co_return std::unexpected<error>(ssx::sformat(
          "Exception thrown while finding coordinator for group {} - {}",
          group_id,
          std::current_exception()));
    }
}

ss::future<std::expected<
  chunked_vector<kafka::coordinator>,
  group_mirroring_task::error>>
group_mirroring_task::do_find_coordinator_batched(
  chunked_vector<ss::sstring> group_ids, kafka::api_version max_version) {
    vlog(
      logger().trace,
      "Requesting find coordinator for {} groups",
      group_ids.size());
    kafka::find_coordinator_request req;
    req.data.key_type = kafka::coordinator_type::group;
    const auto group_cnt = group_ids.size();
    req.data.coordinator_keys = std::move(group_ids);
    try {
        auto resp = co_await get_cluster_connection().dispatch_to_any(
          std::move(req), std::min(max_version, max_version));

        if (resp.data.error_code != kafka::error_code::none) {
            vlog(
              logger().warn,
              "Error finding coordinator for groups - {}",
              resp.data.error_code);
            co_return std::unexpected(error(
              ssx::sformat("Error finding coordinator for groups"),
              resp.data.error_code));
        }

        co_return std::move(resp.data.coordinators);
    } catch (...) {
        auto lvl = ssx::is_shutdown_exception(std::current_exception())
                     ? ss::log_level::trace
                     : ss::log_level::warn;
        vlogl(
          logger(),
          lvl,
          "Exception thrown while finding coordinators for {} groups - {}",
          group_cnt,
          std::current_exception());
        co_return std::unexpected<error>(ssx::sformat(
          "Exception thrown while finding coordinators for {} groups - {}",
          group_cnt,
          std::current_exception()));
    }
}

std::string_view
group_mirroring_task_factory::created_task_name() const noexcept {
    return group_mirroring_task::task_name;
}

std::unique_ptr<task> group_mirroring_task_factory::create_task(link* link) {
    return std::make_unique<group_mirroring_task>(link, *(link->get_config()));
}

fmt::iterator group_mirroring_task::error::format_to(fmt::iterator it) const {
    if (errc != kafka::error_code::none) {
        return fmt::format_to(it, "{{message: {}, errc: {}}}", message, errc);
    }
    return fmt::format_to(it, "{{message: {}}}", message);
}

fmt::iterator group_mirroring_task::partition_committed_offset::format_to(
  fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{partition: {}, committed_offset: {}}}",
      partition,
      committed_offset);
}

fmt::iterator
group_mirroring_task::topic_offsets::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{topic: {}, partition_offsets: {}}}",
      topic,
      fmt::join(partition_offsets, ", "));
}

fmt::iterator
group_mirroring_task::group_offsets::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{group_id: {}, topic_offsets: {}}}",
      group_id,
      fmt::join(topic_offsets, ", "));
}

} // namespace cluster_link
