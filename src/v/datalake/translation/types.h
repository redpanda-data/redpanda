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
    // The translation process encompasses datalake format conversion, upload
    // to a cloud bucket and a subsequent notification to the datalake
    // coordinator of it's availablity.
    kafka::offset highest_translated_offset;

    auto serde_fields() { return std::tie(highest_translated_offset); }
};

}; // namespace datalake::translation
