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

#include "ssx/time.h"

namespace ssx {

fmt::format_context::iterator
duration::format_to(fmt::format_context::iterator out) const {
    return fmt::format_to(out, "{}", absl::FormatDuration(_dur));
}

fmt::format_context::iterator
instant::format_to(fmt::format_context::iterator out) const {
    return fmt::format_to(out, "{{duration={}}}", *this - instant());
}

fmt::format_context::iterator
wall_time::format_to(fmt::format_context::iterator out) const {
    return fmt::format_to(out, "{}", absl::FormatTime(_time));
}

} // namespace ssx
