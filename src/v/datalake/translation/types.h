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

#include "model/fundamental.h"
#include "serde/envelope.h"

namespace datalake::translation {

struct translation_state
  : serde::
      envelope<translation_state, serde::version<0>, serde::compat_version<0>> {
    // highest offset that has been successfully translated (inclusive)
    kafka::offset highest_translated_offset;

    auto serde_fields() { return std::tie(highest_translated_offset); }
};

}; // namespace datalake::translation
