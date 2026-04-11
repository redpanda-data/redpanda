/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "ssx/minimum_interval_timer.h"

namespace ssx {

template class minimum_interval_timer<
  minimum_interval_timer_type::tail,
  ss::lowres_clock>;

} // namespace ssx
