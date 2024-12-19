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
#include "storage/log.h"

#include <seastar/core/shared_ptr.hh>

namespace datalake::translation {

// Returns the translated offset, while taking into account translator batch
// types, for a given kafka offset.
//
// For example, in the following situation:
// Kaf offsets: [TO]  .   .    .     [NKO]
// Log offsets: [K]  [C] [C] [C/TLO] [NKO]
// where TO is the translated offset, K is the last kafka record, C is a
// translator (Config) batch, TLO is the translated log offset, and NKO is the
// next expected kafka record, we should expect TLO to be equal to the offset of
// the last configuration batch before the next kafka record.
model::offset
get_translated_log_offset(ss::shared_ptr<storage::log> log, kafka::offset o);

} // namespace datalake::translation
