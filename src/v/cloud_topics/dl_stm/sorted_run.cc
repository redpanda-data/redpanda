/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/dl_stm/sorted_run.h"

#include "cloud_topics/dl_stm/commands.h"
#include "model/fundamental.h"

namespace experimental::cloud_topics {

sorted_run_t::sorted_run_t(const dl_overlay& o)
  : base(o.base_offset)
  , last(o.last_offset)
  , ts_base(o.base_ts)
  , ts_last(o.last_ts) {
    values.push_back(o);
}

bool sorted_run_t::maybe_append(const dl_overlay& o) {
    if (values.empty()) {
        values.push_back(o);
        base = o.base_offset;
        last = o.last_offset;
        ts_base = o.base_ts;
        ts_last = o.last_ts;
        return true;
    }
    // check invariant
    if (o.base_offset < kafka::next_offset(last)) {
        return false;
    }
    values.push_back(o);
    last = o.last_offset;
    ts_base = std::min(o.base_ts, ts_base);
    ts_last = std::max(o.last_ts, ts_last);
    return true;
}

} // namespace experimental::cloud_topics
