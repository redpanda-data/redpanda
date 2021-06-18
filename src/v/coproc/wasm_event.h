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
#include "bytes/iobuf.h"
#include "coproc/errc.h"
#include "coproc/types.h"
#include "model/record.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/optimized_optional.hh>

#include <absl/container/btree_map.h>

#include <optional>
#include <variant>

namespace coproc::wasm {

/// Enumerations for all possible values for keys within the record headers
enum class event_header { action, name, description, checksum };

/// Enumerations for all possible values for the 'action' key within the headers
enum class event_action { deploy, remove };

struct deploy_attributes {
    ss::sstring description;
    ss::sstring name;
};
struct log_event {
    deploy_attributes attrs;
    iobuf source_code;
};

/// \brief returns the id of the event, performs a byte-copy
std::optional<script_id> get_event_id(const model::record&);

/// \brief returns an iterator to the value in the records header keyed by
/// the 'wasm_event_header' parameter
std::vector<model::record_header>::const_iterator
get_value_for_event_header(const model::record&, event_header);

/// \brief wasm::errc if record is malformed, otherwise returns
/// wasm_event_action::deploy or wasm_event_action::remove, representing a
/// command to change state within the wasm engine
std::variant<wasm::errc, event_action> get_event_action(const model::record&);

/// \brief errc::none if the record is valid AND the checksum passes validation
wasm::errc verify_event_checksum(const model::record&);

/// \brief performs the strictest form of validation, 'none' if all checks pass
wasm::errc validate_event(const model::record&);

/// \brief Returns the newest events and their data blobs if the event has
/// passed all validators
absl::btree_map<script_id, log_event>
  reconcile_events(std::vector<model::record_batch>);

} // namespace coproc::wasm
