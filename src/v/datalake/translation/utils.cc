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

model::offset
get_translated_log_offset(ss::shared_ptr<storage::log> log, kafka::offset o) {
    if (o == kafka::offset{}) {
        return kafka::offset_cast(o);
    }

    auto next_kafka_offset = kafka::next_offset(o);
    auto log_offset_for_next_kafka_offset = log->to_log_offset(
      kafka::offset_cast(next_kafka_offset));
    auto translated_log_offset = model::prev_offset(
      log_offset_for_next_kafka_offset);
    return translated_log_offset;
}

} // namespace datalake::translation
