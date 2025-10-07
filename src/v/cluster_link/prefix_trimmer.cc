/**
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/dev/licenses/rcl.md
 *
 */

#include "cluster_link/prefix_trimmer.h"

#include "cluster_link/link.h"
#include "kafka/protocol/types.h"

namespace cluster_link {

namespace {

chunked_vector<kafka::data::rpc::topic_partitions>
build_get_offsets_request(const absl::flat_hash_set<::model::ntp>& ntps) {
    chunked_vector<kafka::data::rpc::topic_partitions> tp_vec;
    chunked_hash_map<::model::topic, chunked_vector<::model::partition_id>>
      topic_partitions_map;
    std::ranges::for_each(
      ntps, [&topic_partitions_map](const ::model::ntp& ntp) {
          topic_partitions_map[ntp.tp.topic].push_back(ntp.tp.partition);
      });
    tp_vec.reserve(topic_partitions_map.size());
    for (auto& [tp, parts] : topic_partitions_map) {
        kafka::data::rpc::topic_partitions topic_partitions;
        topic_partitions.topic = std::move(tp);
        topic_partitions.partitions = std::move(parts);
        tp_vec.push_back(std::move(topic_partitions));
    }

    return tp_vec;
}
} // namespace

prefix_trimmer::prefix_trimmer(link* link, const model::metadata& config)
  : task(
      link,
      config.configuration.partition_prefix_trimming_cfg.get_task_interval(),
      prefix_trimmer::task_name)
  , _config(config.configuration.partition_prefix_trimming_cfg) {}

void prefix_trimmer::update_config(const model::metadata& config) {
    _config = config.configuration.partition_prefix_trimming_cfg;
    set_run_interval(
      config.configuration.partition_prefix_trimming_cfg.get_task_interval());
}

void prefix_trimmer::handle_partition_leadership_change(
  ::model::ntp ntp, ntp_leader is_ntp_leader, std::optional<::model::term_id>) {
    if (is_ntp_leader == ntp_leader::yes) {
        add_tracked_partition(std::move(ntp));
    } else {
        remove_tracked_partition(std::move(ntp));
    }
}

ss::future<> prefix_trimmer::run_impl() {
    vlog(logger().trace, "Running partition prefix trimmer task");
    if (_tracked_partitions.empty()) {
        vlog(
          logger().debug,
          "No tracked partitions for prefix trimmer, skipping run");
        co_return;
    }

    // The prefix trimmer task is responsible for keeping the log start offsets
    // of shadow partitions in sync with the source partitions.  To accomplish
    // this this task will periodically:
    // 1. Gather the partition offsets from the replicators
    // 2. for every ntp in that list that is tracked by this task, compare the
    // source start offset with the log start offset of the mirror partition
    // 3. If the source is ahead of the mirror start offset, issue a prefix trim

    auto offsets = get_link()->get_partition_offsets_report();

    auto offset_req = build_get_offsets_request(_tracked_partitions);

    vlog(logger().trace, "offset_req: {}", offset_req);

    auto& rpc_client = get_link()->get_kafka_rpc_client_service();
    auto offset_reply = co_await rpc_client.get_partition_offsets(
      std::move(offset_req));

    if (!offset_reply.has_value()) {
        vlog(
          logger().warn,
          "Failed to get partition offsets: {}",
          offset_reply.assume_error());
        co_return;
    }

    const auto shadow_partition_offsets
      = std::move(offset_reply).assume_value();

    // Now build the trim requests
    kafka::data::rpc::delete_records_cmd_map to_trim;

    for (const auto& ntp : _tracked_partitions) {
        auto offset_it = offsets.find(ntp);
        if (offset_it == offsets.end()) {
            vlog(
              logger().debug,
              "No offsets reported for tracked partition {}, skipping",
              ntp);
            continue;
        }

        const auto& offset_report = offset_it->second;
        if (offset_report.update_time == ss::lowres_clock::time_point{}) {
            vlog(
              logger().debug,
              "No offsets reported for tracked partition {}, skipping",
              ntp);
            continue;
        }

        const auto& topic_it = shadow_partition_offsets.find(ntp.tp.topic);
        if (topic_it == shadow_partition_offsets.end()) {
            vlog(
              logger().debug,
              "Did not receive local partition offsets for topic {}",
              ntp.tp.topic);
            continue;
        }
        const auto& partition_it = topic_it->second.find(ntp.tp.partition);
        if (partition_it == topic_it->second.end()) {
            vlog(
              logger().debug,
              "Did not receive local partition offsets for {}/{}",
              ntp.tp.topic,
              ntp.tp.partition);
            continue;
        }

        const auto& partition_offset_result = partition_it->second;
        if (partition_offset_result.err != cluster::errc::success) {
            vlog(
              logger().debug,
              "Did not receive offset for {}/{}: {}",
              ntp.tp.topic,
              ntp.tp.partition,
              partition_offset_result.err);
            continue;
        }

        auto shadow_hwm = partition_offset_result.offsets.high_watermark;
        auto shadow_start = partition_offset_result.offsets.log_start_offset;

        auto source_start = offset_report.source_start_offset;

        if (source_start <= shadow_start) {
            // no trim needed
            vlog(
              logger().trace,
              "Not trimming {}/{} to {} <= shadow start {}",
              ntp.tp.topic,
              ntp.tp.partition,
              source_start,
              shadow_start);
            continue;
        }

        if (source_start > shadow_hwm) {
            // cannot trim past the HWM
            vlog(
              logger().debug,
              "Unable to trim {}/{} to {} > shadow HWM {}",
              ntp.tp.topic,
              ntp.tp.partition,
              source_start,
              shadow_hwm);
            continue;
        }

        vlog(
          logger().debug,
          "Trimming {}/{} to {}",
          ntp.tp.topic,
          ntp.tp.partition,
          source_start);
        to_trim[ntp.tp.topic][ntp.tp.partition]
          = kafka::data::rpc::delete_records_cmd{.offset = source_start};
    }

    if (to_trim.empty()) {
        vlog(logger().debug, "No partitions need trimming");
        if (get_state() != model::task_state::active) {
            std::ignore = change_state(
              model::task_state::active, "Partition prefix trimmer ran");
        }
        co_return;
    }

    // Issue the trim requests
    auto trim_res = co_await rpc_client.delete_records(std::move(to_trim));
    if (!trim_res.has_value()) {
        vlog(
          logger().warn,
          "Failed to issue trim requests: {}",
          trim_res.assume_error());
    } else {
        vlog(
          logger().debug,
          "Successfully issued trim requests: {}",
          trim_res.assume_value());
    }

    if (get_state() != model::task_state::active) {
        std::ignore = change_state(
          model::task_state::active, "Partition prefix trimmer ran");
    }
}

bool prefix_trimmer::should_start_impl(ss::shard_id, ::model::node_id) const {
    return !_tracked_partitions.empty();
}

bool prefix_trimmer::should_stop_impl(ss::shard_id, ::model::node_id) const {
    return _tracked_partitions.empty();
}

void prefix_trimmer::add_tracked_partition(::model::ntp ntp) {
    vlog(logger().trace, "Adding tracked partition {}", ntp);
    _tracked_partitions.insert(std::move(ntp));
}

void prefix_trimmer::remove_tracked_partition(::model::ntp ntp) {
    vlog(logger().trace, "Removing tracked partition {}", ntp);
    _tracked_partitions.erase(ntp);
}

std::string_view prefix_trimmer_factory::created_task_name() const noexcept {
    return prefix_trimmer::task_name;
}

std::unique_ptr<task> prefix_trimmer_factory::create_task(link* link) {
    return std::make_unique<prefix_trimmer>(link, link->get_config());
}
} // namespace cluster_link
