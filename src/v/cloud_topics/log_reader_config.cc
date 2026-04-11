/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/log_reader_config.h"

#include "base/format_to.h"
#include "utils/to_string.h"

namespace cloud_topics {

fmt::iterator cloud_topic_log_reader_config::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "start_offset:{}, max_offset:{}, min_bytes:{}, max_bytes:{}, "
      "strict_max_bytes:{}, type_filter: {}, first_timestamp:{}, "
      "skip_cache:{}, allow_mat_failure:{}, abortable:{}, "
      "client_address:{}",
      start_offset,
      max_offset,
      min_bytes,
      max_bytes,
      strict_max_bytes,
      type_filter,
      first_timestamp,
      skip_cache,
      allow_mat_failure,
      abort_source.has_value(),
      client_address);
}

}; // namespace cloud_topics
