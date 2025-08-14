/*
 * Copyright 2025 Redpanda Data, Inc.
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

#include <seastar/util/bool_class.hh>

namespace cluster_link {
/// Indicates if the current node is the leader for a given NTP
using ntp_leader = ss::bool_class<struct is_ntp_leader_tag>;
} // namespace cluster_link
