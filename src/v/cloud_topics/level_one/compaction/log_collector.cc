/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/log_collector.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics::l1 {

log_collector::log_collector(compaction_scheduler* scheduler)
  : _scheduler(scheduler) {}

ss::future<> log_collector::start() { co_await start_collecting_logs(); }

ss::future<> log_collector::stop() { co_await stop_collecting_logs(); }

} // namespace cloud_topics::l1
