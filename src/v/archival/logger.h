/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "seastarx.h"

#include <seastar/util/log.hh>

namespace archival {
inline ss::logger archival_log("archival");
inline ss::logger upload_ctrl_log("archival-ctrl");
} // namespace archival
