/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster/topic_table.h"

#include <seastar/core/sharded.hh>

namespace cluster {
struct partition_replicas {
    model::ntp partition;
    std::vector<model::broker_shard> replicas;
};

class topic_table_partition_generator_exception : public std::runtime_error {
public:
    explicit topic_table_partition_generator_exception(const std::string& m);
};

/*
 * This is a utility class that walks the topic table and returns batches
 * of partitions and their replicas. For this operation to make sense,
 * topic table stability is required. The generator will throw if the topic
 * table has been updated between batches.
 */
class topic_table_partition_generator {
public:
    using generator_type_t = std::vector<partition_replicas>;

    static constexpr size_t default_batch_size = 256;

    explicit topic_table_partition_generator(
      ss::sharded<topic_table>& topic_table,
      size_t batch_size = default_batch_size);

    ss::future<std::optional<generator_type_t>> next_batch();

private:
    void next();

    const assignments_set& current_assignment_set() const;

    ss::sharded<topic_table>& _topic_table;
    model::revision_id _stable_revision_id;
    size_t _batch_size;
    bool _exhausted{false};

    topic_table::underlying_t::const_iterator _topic_iterator;
    assignments_set::const_iterator _partition_iterator;
};
} // namespace cluster
