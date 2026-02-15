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

#include "base/seastarx.h"
#include "container/intrusive_list_helpers.h"

#include <seastar/core/future.hh>

namespace cloud_topics {

struct inflight_write_token {
    ss::promise<> done;
    intrusive_list_hook _hook;
};

using inflight_write_list
  = intrusive_list<inflight_write_token, &inflight_write_token::_hook>;

} // namespace cloud_topics
