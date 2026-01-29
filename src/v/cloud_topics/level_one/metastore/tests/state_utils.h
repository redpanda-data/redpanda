/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/level_one/metastore/state.h"

namespace lsm {
class database;
} // namespace lsm

namespace cloud_topics::l1 {

state snapshot_to_state(lsm::database&);

} // namespace cloud_topics::l1
