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
#include "vlog.h"

#include <type_traits>

namespace coproc::wasm {

std::string_view action_as_string_view(event_action action) {
    switch (action) {
    case event_action::deploy:
        return "deploy";
    case event_action::remove:
        return "remove";
    }
    __builtin_unreachable();
}

std::string_view header_as_string_view(event_header header) {
    switch (header) {
    case event_header::action:
        return "action";
    case event_header::description:
        return "description";
    case event_header::checksum:
        return "sha256";
    case event_header::type:
        return "type";
    }
    __builtin_unreachable();
}

std::string_view coproc_type_as_string_view(event_type header) {
    switch (header) {
    case event_type::async:
        return "async";
    case event_type::data_policy:
        return "data-policy";
    }
    __builtin_unreachable();
}

std::optional<script_id> get_event_id(const model::record& r) {
    if (r.key_size() != sizeof(script_id::type)) {
        return std::nullopt;
    }
    /// Can be sure the parser won't throw, as we previously checked that the
    /// at least the correct number of bytes exists in the buffer
    return script_id(iobuf_const_parser(r.key()).consume_type<uint64_t>());
}

std::vector<model::record_header>::const_iterator
get_value_for_event_header(const model::record& r, event_header header) {
    std::string_view match = header_as_string_view(header);
    return std::find_if(
      r.headers().cbegin(),
      r.headers().cend(),
      [&match](const model::record_header& rh) { return rh.key() == match; });
}

std::variant<wasm::errc, event_type> get_coproc_type(const model::record& r) {
    auto itr = get_value_for_event_header(r, event_header::type);
    if (itr == r.headers().cend()) {
        return event_type::async; // support old msgs from topic
    }

    const auto& type_raw = itr->value();
    auto type = iobuf_const_parser(type_raw).read_bytes(type_raw.size_bytes());
    if (type == "async") {
        return event_type::async;
    }
    if (type == "data-policy") {
        return event_type::data_policy;
    }
    return wasm::errc::unexpected_coproc_type;
}

std::variant<wasm::errc, event_action>
get_event_action(const model::record& r) {
    auto itr = get_value_for_event_header(r, event_header::action);
    if (itr == r.headers().cend()) {
        return wasm::errc::missing_header_key;
    }
    const auto& action = itr->value();
    if (action == action_as_string_view(event_action::deploy)) {
        return event_action::deploy;
    } else if (action == action_as_string_view(event_action::remove)) {
        return event_action::remove;
    }
    return wasm::errc::unexpected_action_type;
}

wasm::errc verify_event_checksum(const model::record& r) {
    auto itr = get_value_for_event_header(r, event_header::checksum);
    if (itr == r.headers().cend()) {
        return wasm::errc::missing_header_key;
    }
    /// Verify the checksum against the actual wasm script
    const auto checksum
      = iobuf_const_parser(itr->value()).read_bytes(itr->value_size());
    /// Performs a copy
    auto script = iobuf_const_parser(r.value()).read_bytes(r.value_size());
    if (script.empty()) {
        return wasm::errc::empty_mandatory_field;
    }
    hash_sha256 h;
    h.update(script);
    const auto calculated = h.reset();
    return to_bytes_view(calculated) == checksum
             ? wasm::errc::none
             : wasm::errc::mismatched_checksum;
}

wasm::errc
validate_event(const model::record& r, parsed_event::event_header& header) {
    /// A key field is mandatory for all types of expected events
    if (!get_event_id(r)) {
        return wasm::errc::empty_mandatory_field;
    }
    /// No copy performed here, optional will be false in the case the 'action'
    /// key is missing, or the value provided is unsupported i.e. not 'remove'
    /// or 'deploy'
    auto action = get_event_action(r);
    if (auto errc = std::get_if<wasm::errc>(&action)) {
        return *errc;
    }

    auto get_type_res = get_coproc_type(r);
    if (auto err = std::get_if<wasm::errc>(&get_type_res)) {
        return wasm::errc::unexpected_coproc_type;
    }
    header.type = std::get<event_type>(get_type_res);

    /// Technically all a 'remove' event needs to be valid, is a non-empty
    /// key field
    auto wasm_action = std::get<event_action>(action);
    if (wasm_action == event_action::deploy) {
        if (r.value_size() == 0) {
            return wasm::errc::empty_mandatory_field;
        }
        header.action = event_action::deploy;

        return verify_event_checksum(r);
    } else {
        if (r.value_size() > 0) {
            /// A 'remove' should have an empty body
            return wasm::errc::unexpected_value;
        }

        header.action = event_action::remove;
    }
    /// A description isn't neccessary for either event type
    return wasm::errc::none;
}

absl::btree_map<script_id, parsed_event>
reconcile_events(std::vector<model::record_batch> events) {
    absl::btree_map<script_id, parsed_event> script_id_to_event;
    for (auto& record_batch : events) {
        record_batch.for_each_record([&script_id_to_event](model::record r) {
            parsed_event new_event;
            auto mb_error = wasm::validate_event(r, new_event.header);
            if (mb_error != wasm::errc::none) {
                vlog(
                  coproclog.error,
                  "Erranous coproc record detected, issue: {}",
                  mb_error);
                return;
            }
            auto id = wasm::get_event_id(r).value();
            new_event.data = r.share_value();
            /// Update or insert, preferring the newest
            script_id_to_event.insert_or_assign(id, std::move(new_event));
        });
    }
    return script_id_to_event;
}

event_batch reconcile_events_by_type(std::vector<model::record_batch> events) {
    auto parsed_events = reconcile_events(std::move(events));
    event_batch wsas;
    for (auto& [id, event] : parsed_events) {
        wsas[event.header.type].emplace(id, std::move(event));
    }
    return wsas;
}

} // namespace coproc::wasm
