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

} // namespace model
