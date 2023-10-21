/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "model/transform.h"

#include "model/fundamental.h"

namespace model {

std::ostream& operator<<(std::ostream& os, const transform_metadata& meta) {
    fmt::print(
      os,
      "{{name: \"{}\", input: {}, outputs: {}, "
      "env: <redacted>, uuid: {}, source_ptr: {} }}",
      meta.name,
      meta.input_topic,
      meta.output_topics,
      // skip env becuase of pii
      meta.uuid,
      meta.source_ptr);
    return os;
}

std::ostream& operator<<(std::ostream& os, const transform_offsets_key& key) {
    fmt::print(
      os, "{{ transform id: {}, partition: {} }}", key.id, key.partition);
    return os;
}

std::ostream&
operator<<(std::ostream& os, const transform_offsets_value& value) {
    fmt::print(os, "{{ offset: {} }}", value.offset);
    return os;
}

std::ostream&
operator<<(std::ostream& os, const transform_report::processor& p) {
    fmt::print(os, "{{id: {}, status: {}, node: {}}}", p.id, p.status, p.node);
    return os;
}

transform_report::transform_report(transform_metadata meta)
  : metadata(std::move(meta))
  , processors() {}

transform_report::transform_report(
  transform_metadata meta, absl::btree_map<model::partition_id, processor> map)
  : metadata(std::move(meta))
  , processors(std::move(map)){};

void transform_report::add(processor processor) {
    processors.insert_or_assign(processor.id, processor);
}

void cluster_transform_report::add(
  transform_id id,
  const transform_metadata& meta,
  transform_report::processor processor) {
    auto [it, _] = transforms.try_emplace(id, meta);
    it->second.add(processor);
}

void cluster_transform_report::merge(const cluster_transform_report& other) {
    for (const auto& [tid, treport] : other.transforms) {
        for (const auto& [pid, preport] : treport.processors) {
            add(tid, treport.metadata, preport);
        }
    }
}

std::ostream&
operator<<(std::ostream& os, transform_report::processor::state s) {
    return os << processor_state_to_string(s);
}
std::string_view
processor_state_to_string(transform_report::processor::state state) {
    switch (state) {
    case transform_report::processor::state::inactive:
        return "inactive";
    case transform_report::processor::state::running:
        return "running";
    case transform_report::processor::state::errored:
        return "errored";
    case transform_report::processor::state::unknown:
        break;
    }
    return "unknown";
}
} // namespace model
