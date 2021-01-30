/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/wasm_event.h"

#include "bytes/iobuf_parser.h"
#include "coproc/logger.h"
#include "hashing/secure.h"

namespace coproc {

std::string_view action_as_string_view(wasm_event_action action) {
    switch (action) {
    case wasm_event_action::deploy:
        return "deploy";
    case wasm_event_action::remove:
        return "remove";
    }
    __builtin_unreachable();
}

std::string_view header_as_string_view(wasm_event_header header) {
    switch (header) {
    case wasm_event_header::action:
        return "action";
    case wasm_event_header::description:
        return "description";
    case wasm_event_header::checksum:
        return "sha256";
    }
    __builtin_unreachable();
}

std::optional<ss::sstring> wasm_event_get_name(const model::record& r) {
    /// Performs a copy
    auto id = iobuf_const_parser(r.key()).read_string(r.key_size());
    if (id.empty()) {
        return std::nullopt;
    }
    return id;
}

std::vector<model::record_header>::const_iterator
wasm_event_get_value_for_header(
  const model::record& r, wasm_event_header header) {
    std::string_view match = header_as_string_view(header);
    return std::find_if(
      r.headers().cbegin(),
      r.headers().cend(),
      [&match](const model::record_header& rh) { return rh.key() == match; });
}

std::variant<wasm_event_errc, wasm_event_action>
wasm_event_get_action(const model::record& r) {
    auto itr = wasm_event_get_value_for_header(r, wasm_event_header::action);
    if (itr == r.headers().cend()) {
        return wasm_event_errc::missing_header_key;
    }
    const auto& action = itr->value();
    if (action == action_as_string_view(wasm_event_action::deploy)) {
        return wasm_event_action::deploy;
    } else if (action == action_as_string_view(wasm_event_action::remove)) {
        return wasm_event_action::remove;
    }
    return wasm_event_errc::unexpected_action_type;
}

wasm_event_errc wasm_event_verify_checksum(const model::record& r) {
    auto itr = wasm_event_get_value_for_header(r, wasm_event_header::checksum);
    if (itr == r.headers().cend()) {
        return wasm_event_errc::missing_header_key;
    }
    /// Verify the checksum against the actual wasm script
    const auto checksum
      = iobuf_const_parser(itr->value()).read_bytes(itr->value_size());
    /// Performs a copy
    auto script = iobuf_const_parser(r.value()).read_string(r.value_size());
    if (script.empty()) {
        return wasm_event_errc::empty_mandatory_field;
    }
    hash_sha256 h;
    h.update(script);
    const auto calculated = h.reset();
    return to_bytes_view(calculated) == checksum
             ? wasm_event_errc::none
             : wasm_event_errc::mismatched_checksum;
}

wasm_event_errc wasm_event_validate(const model::record& r) {
    /// A key field is mandatory for all types of expected wasm_events
    if (r.key_size() == 0) {
        return wasm_event_errc::empty_mandatory_field;
    }
    /// No copy performed here, optional will be false in the case the 'action'
    /// key is missing, or the value provided is unsupported i.e. not 'remove'
    /// or 'deploy'
    auto action = coproc::wasm_event_get_action(r);
    if (auto errc = std::get_if<wasm_event_errc>(&action)) {
        return *errc;
    }
    /// Technically all a 'remove' event needs to be valid, is a non-empty
    /// key field
    auto wasm_action = std::get<wasm_event_action>(action);
    if (wasm_action == coproc::wasm_event_action::deploy) {
        if (r.value_size() == 0) {
            return wasm_event_errc::empty_mandatory_field;
        }
        return coproc::wasm_event_verify_checksum(r);
    } else {
        if (r.value_size() > 0) {
            /// A 'remove' should have an empty body
            return coproc::wasm_event_errc::unexpected_value;
        }
    }
    /// A description isn't neccessary for either wasm_event type
    return wasm_event_errc::none;
}

} // namespace coproc
