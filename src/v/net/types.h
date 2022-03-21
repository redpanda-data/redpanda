/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include <seastar/core/lowres_clock.hh>
#include <seastar/util/bool_class.hh>

namespace net {

using metrics_disabled = seastar::bool_class<struct metrics_disabled_tag>;
using clock_type = seastar::lowres_clock;

} // namespace net
