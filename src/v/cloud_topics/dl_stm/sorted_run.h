/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/dl_stm/commands.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/envelope.h"

namespace experimental::cloud_topics {

struct sorted_run_t
  : public serde::
      envelope<sorted_run_t, serde::version<0>, serde::compat_version<0>> {
    explicit sorted_run_t(const dl_overlay& o);

    sorted_run_t() = default;

    bool maybe_append(const dl_overlay& o);

    auto serde_fields() {
        return std::tie(values, base, last, ts_base, ts_last);
    }

    bool operator==(const sorted_run_t& other) const noexcept = default;

    std::deque<dl_overlay> values;
    kafka::offset base;
    kafka::offset last;
    model::timestamp ts_base;
    model::timestamp ts_last;
};

} // namespace experimental::cloud_topics
