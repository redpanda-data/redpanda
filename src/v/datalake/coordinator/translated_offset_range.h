/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/format_to.h"
#include "container/chunked_vector.h"
#include "datalake/coordinator/data_file.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

#include <fmt/core.h>

namespace datalake::coordinator {

// Represents a translated contiguous range of Kafka offsets.
struct translated_offset_range
  : serde::envelope<
      translated_offset_range,
      serde::version<2>,
      serde::compat_version<0>> {
    auto serde_fields() {
        return std::tie(
          start_offset, last_offset, files, dlq_files, kafka_bytes_processed);
    }
    // First Kafka offset (inclusive) represented in this range.
    kafka::offset start_offset;
    // Last Kafka offset (inclusive) represented in this range.
    kafka::offset last_offset;
    // Files for main table.
    chunked_vector<data_file> files;
    // Files for dead-letter queue table.
    chunked_vector<data_file> dlq_files;

    // Total number of raw Kafka bytes processed to generate this
    // translated range.
    uint64_t kafka_bytes_processed{0};

    translated_offset_range copy() const {
        translated_offset_range range;
        range.start_offset = start_offset;
        range.last_offset = last_offset;
        range.kafka_bytes_processed = kafka_bytes_processed;
        range.files.reserve(files.size());
        for (const auto& f : files) {
            range.files.push_back(f.copy());
        }
        range.dlq_files.reserve(dlq_files.size());
        for (const auto& f : dlq_files) {
            range.dlq_files.push_back(f.copy());
        }
        return range;
    }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{start_offset: {}, last_offset: {}, files: {}, dlq_files: {}, "
          "kafka_bytes_processed: {}}}",
          start_offset,
          last_offset,
          files,
          dlq_files,
          kafka_bytes_processed);
    }
};

inline size_t estimated_memory_bytes(const translated_offset_range& r) {
    size_t bytes = sizeof(translated_offset_range);
    for (const auto& f : r.files) {
        bytes += estimated_memory_bytes(f);
    }
    for (const auto& f : r.dlq_files) {
        bytes += estimated_memory_bytes(f);
    }
    return bytes;
}

} // namespace datalake::coordinator
