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

#include "model/fundamental.h"
#include "serde/envelope.h"

namespace datalake::coordinator {

struct partition_state_override
  : public serde::envelope<
      partition_state_override,
      serde::version<0>,
      serde::compat_version<0>> {
    std::optional<kafka::offset> last_committed;

    auto serde_fields() { return std::tie(last_committed); }

    friend std::ostream&
    operator<<(std::ostream&, const partition_state_override&);
};

} // namespace datalake::coordinator
