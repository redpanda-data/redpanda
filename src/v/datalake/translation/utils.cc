/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/translation/utils.h"

namespace datalake::translation {

model::offset highest_log_offset_below_next(
  ss::shared_ptr<storage::log> log, kafka::offset kafka_lto) {
    /**
     * If translated offset is smaller than the start offset of the log it means
     * that the last translated offset is stored in cloud storage. We need to
     * clamp max collectible offset. This can happen when fast partition
     * movement.
     */
    auto log_start_offset = log->offsets().start_offset;
    if (log_start_offset >= model::offset{0}) {
        auto log_start_kafka_offset = model::offset_cast(
          log->from_log_offset(log_start_offset));

        if (log_start_kafka_offset >= kafka_lto) {
            return log_start_offset;
        }
    }

    auto next_kafka_offset = kafka::next_offset(kafka_lto);

    auto log_offset_for_next_kafka_offset = log->to_log_offset(
      kafka::offset_cast(next_kafka_offset));
    auto translated_log_offset = model::prev_offset(
      log_offset_for_next_kafka_offset);
    return translated_log_offset;
}

} // namespace datalake::translation
