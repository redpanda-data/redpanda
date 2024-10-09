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

#include "container/fragmented_vector.h"
#include "datalake/coordinator/data_file.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

#include <fmt/core.h>

namespace datalake::coordinator {

// Represents a translated contiguous range of Kafka offsets.
struct translated_offset_range
  : serde::envelope<
      translated_offset_range,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(start_offset, last_offset, files); }
    // First Kafka offset (inclusive) represented in this range.
    kafka::offset start_offset;
    // Last Kafka offset (inclusive) represented in this range.
    kafka::offset last_offset;
    chunked_vector<data_file> files;
};

std::ostream& operator<<(std::ostream& o, const translated_offset_range& r);

} // namespace datalake::coordinator
