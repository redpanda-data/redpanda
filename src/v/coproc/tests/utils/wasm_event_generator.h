/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "bytes/bytes.h"
#include "coproc/wasm_event.h"
#include "model/record.h"
#include "model/record_batch_reader.h"

#include <seastar/core/sstring.hh>

#include <optional>

namespace coproc {

/// Convienent struct for bundling together data necessary to serialized a
/// wasm_event into a model::record. Any field set to std::nullopt will be
/// skipped during serialization
struct wasm_event {
    using opt_action_t = std::optional<wasm_event_action>;
    using opt_str_t = std::optional<ss::sstring>;
    using opt_bytes_t = std::optional<bytes>;
    opt_str_t name;
    opt_str_t desc;
    opt_str_t script;
    opt_bytes_t checksum;
    opt_action_t action;
};

/// \brief Generates an event that models the 'wasm_event' struct passed in. Any
/// optional fields aren't serialized into the resultant record, useful for
/// testing failure cases against malformatted record events
model::record create_wasm_record(const wasm_event&);

/// \brief Makes model::record_batch of a predefined deterministic records list
model::record_batch
make_wasm_batch(model::offset o, std::vector<model::record>);

/// \brief A record batch reader that generates random valid wasm_events
/// @param offset offset to start at
/// @param batch_size number of record_batches in each batch
/// @param n_batches num of batches of batches to generate before stream ends
model::record_batch_reader
make_wasm_event_record_batch_reader(model::offset, int, int);

} // namespace coproc
