// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/logger.h"

#include "units.h"

namespace kafka {
static constexpr size_t max_log_line_bytes = 128_KiB;
ss::logger klog("kafka");
truncating_logger kwire(klog, max_log_line_bytes);
} // namespace kafka
