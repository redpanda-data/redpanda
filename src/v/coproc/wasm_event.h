/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "bytes/iobuf.h"
#include "coproc/errc.h"
#include "coproc/types.h"
#include "model/record.h"
#include "outcome.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/optimized_optional.hh>

#include <absl/container/btree_map.h>

#include <optional>
#include <variant>

namespace coproc::wasm {

/// Enumerations for all possible values for keys within the record headers
enum class event_header { action, description, checksum, type };

/// Enumerations for all possible coproc_type
/// TODO: think about naming!!!
enum class event_type { async, data_policy };

std::string_view coproc_type_as_string_view(event_type header);

/// Enumerations for all possible values for the 'action' key within the headers
enum class event_action { deploy, remove };

/// \brief returns the id of the event, performs a byte-copy
std::optional<script_id> get_event_id(const model::record&);

/// \brief returns an iterator to the value in the records header keyed by
/// the 'wasm_event_header' parameter
std::vector<model::record_header>::const_iterator
get_value_for_event_header(const model::record&, event_header);

/// \brief wasm::errc if record is malformed, otherwise returns
/// wasm_event_type
result<event_type, wasm::errc> get_coproc_type(const model::record& r);

/// \brief wasm::errc if record is malformed, otherwise returns
/// wasm_event_action::deploy or wasm_event_action::remove, representing a
/// command to change state within the wasm engine
result<event_action, wasm::errc> get_event_action(const model::record&);

/// \brief errc::none if the record is valid AND the checksum passes validation
wasm::errc verify_event_checksum(const model::record&);

/// \brief Structure for parsed event from model::record. It contains raw data
/// of the event and header with action and coproc_type
struct parsed_event {
    struct event_header {
        event_action action;
        event_type type;
    };

    event_header header;
    iobuf data;
};

/// \brief performs the strictest form of validation and fill header for event,
/// 'none' if all checks pass
result<parsed_event::event_header, wasm::errc>
validate_event(const model::record&);

/// \brief Returns the newest parsed events with their data blobs and headers if
/// the event has passed all validators
absl::btree_map<script_id, parsed_event>
  reconcile_events(std::vector<model::record_batch>);

/// \brief Returns the newest parsed events grouped by type
using event_batch
  = absl::btree_map<event_type, absl::btree_map<script_id, parsed_event>>;
event_batch reconcile_events_by_type(std::vector<model::record_batch>);

} // namespace coproc::wasm
