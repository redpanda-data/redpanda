/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/topic_table_partition_generator.h"

namespace cluster {

topic_table_partition_generator_exception::
  topic_table_partition_generator_exception(const std::string& m)
  : std::runtime_error(m) {}

topic_table_partition_generator::topic_table_partition_generator(
  ss::sharded<topic_table>& topic_table, size_t batch_size)
  : _topic_table(topic_table)
  , _stable_revision_id(_topic_table.local().last_applied_revision())
  , _batch_size(batch_size) {
    if (_topic_table.local()._topics.empty()) {
        _topic_iterator = _topic_table.local()._topics.end();
        _exhausted = true;
    } else {
        _topic_iterator = _topic_table.local()._topics.begin();
        _partition_iterator = current_assignment_set().begin();
    }
}

ss::future<std::optional<topic_table_partition_generator::generator_type_t>>
topic_table_partition_generator::next_batch() {
    const auto current_revision_id
      = _topic_table.local().last_applied_revision();
    if (current_revision_id != _stable_revision_id) {
        throw topic_table_partition_generator_exception(fmt::format(
          "Last applied revision id moved from {} to {} whilst "
          "the generator was active",
          _stable_revision_id,
          current_revision_id));
    }

    if (_exhausted) {
        co_return std::nullopt;
    }

    generator_type_t batch;
    batch.reserve(_batch_size);

    while (!_exhausted && batch.size() < _batch_size) {
        model::topic_namespace tn = _topic_iterator->first;
        model::partition_id pid = _partition_iterator->id;
        std::vector<model::broker_shard> replicas
          = _partition_iterator->replicas;
        partition_replicas entry{
          .partition = model::ntp{tn.ns, tn.tp, pid},
          .replicas = std::move(replicas)};

        batch.push_back(std::move(entry));

        next();
    }

    co_return batch;
}

void topic_table_partition_generator::next() {
    if (++_partition_iterator == current_assignment_set().end()) {
        if (++_topic_iterator == _topic_table.local()._topics.end()) {
            _exhausted = true;
            return;
        }

        _partition_iterator = current_assignment_set().begin();
    }
}

const assignments_set&
topic_table_partition_generator::current_assignment_set() const {
    return _topic_iterator->second.get_assignments();
}

} // namespace cluster
