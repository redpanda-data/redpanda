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
#include "coproc/errc.h"
#include "model/record.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/optimized_optional.hh>

#include <optional>
#include <variant>

namespace coproc {

/// Enumerations for all possible values for keys within the record headers
enum class wasm_event_header { action, description, checksum };

/// Enumerations for all possible values for the 'action' key within the headers
enum class wasm_event_action { deploy, remove };

/// \brief returns the id of the event, performs a byte-copy
std::optional<ss::sstring> wasm_event_get_name(const model::record&);

/// \brief returns an iterator to the value in the records header keyed by
/// the 'wasm_event_header' parameter
std::vector<model::record_header>::const_iterator
wasm_event_get_value_for_header(const model::record&, wasm_event_header);

/// \brief wasm_event_errc if record is malformed, otherwise returns
/// wasm_event_action::deploy or wasm_event_action::remove, representing a
/// command to change state within the wasm engine
std::variant<wasm_event_errc, wasm_event_action>
wasm_event_get_action(const model::record&);

/// \brief errc::none if the record is valid AND the checksum passes validation
wasm_event_errc wasm_event_verify_checksum(const model::record&);

/// \brief performs the strictest form of validation, 'none' if all checks pass
wasm_event_errc wasm_event_validate(const model::record&);

} // namespace coproc
