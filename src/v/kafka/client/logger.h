/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "utils/truncating_logger.h"

#include <seastar/util/log.hh>

namespace kafka::client {
inline ss::logger kclog("kafka/client");
inline truncating_logger kcwire(kclog, 1048576);
} // namespace kafka::client
