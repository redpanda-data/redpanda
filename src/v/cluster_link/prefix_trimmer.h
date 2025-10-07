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

#pragma once

#include "cluster_link/model/types.h"
#include "cluster_link/task.h"
#include "kafka/protocol/list_offset.h"

namespace cluster_link {
/**
 * @brief This task is responsible for trimming the partition prefix based on
 * source partition start offset.
 *
 * This task will run on every shard that is a leader for any shadow partition.
 * It will be alerted to new partitions to track based on leadership changes. If
 * there are no shadow partitions on this shard, the task will stop.  Otherwise,
 * this task will periodically:
 *
 * 1. Gather the offsets tracked by the replicator
 * 2. Compare those offsets with the local log start offsets
 * 3. Issue prefix trim commands against partitions that are behind the source
 *
 */
class prefix_trimmer : public task {
public:
    static constexpr auto task_name = "Partition Prefix Trimmer";

    prefix_trimmer(link* link, const model::metadata& config);
    prefix_trimmer(const prefix_trimmer&) = delete;
    prefix_trimmer(prefix_trimmer&&) = delete;
    prefix_trimmer& operator=(const prefix_trimmer&) = delete;
    prefix_trimmer& operator=(prefix_trimmer&&) = delete;
    ~prefix_trimmer() override = default;

    void update_config(const model::metadata& config) final;

    void handle_partition_leadership_change(
      ::model::ntp ntp,
      ntp_leader is_ntp_leader,
      std::optional<::model::term_id> term) final;

protected:
    ss::future<> run_impl() final;

    bool should_start_impl(ss::shard_id, ::model::node_id) const final;

    bool should_stop_impl(ss::shard_id, ::model::node_id) const final;

private:
    /**
     * @brief Called when a shard becomes a leader for a partition
     */
    void add_tracked_partition(::model::ntp ntp);
    /**
     * @brief Called when a shard loses leadership for a partition
     */
    void remove_tracked_partition(::model::ntp ntp);

private:
    model::partition_prefix_trimming_config _config;
    absl::flat_hash_set<::model::ntp> _tracked_partitions;
};

/**
 * @brief Factory for the prefix_trimmer task
 *
 */
class prefix_trimmer_factory : public task_factory {
public:
    std::string_view created_task_name() const noexcept final;
    std::unique_ptr<task> create_task(link* link) final;
};
} // namespace cluster_link
