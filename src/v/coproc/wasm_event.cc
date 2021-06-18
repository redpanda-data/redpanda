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
    case event_header::name:
        return "name";
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

wasm::errc parse_event(const model::record& r, log_event& le) {
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
    /// Technically all a 'remove' event needs to be valid, is a non-empty
    /// key field
    auto wasm_action = std::get<event_action>(action);
    if (wasm_action == event_action::deploy) {
        if (r.value_size() == 0) {
            return wasm::errc::empty_mandatory_field;
        }
        auto desc_itr = get_value_for_event_header(
          r, event_header::description);
        auto name_itr = get_value_for_event_header(r, event_header::name);
        if (
          (desc_itr == r.headers().cend())
          || (name_itr == r.headers().cend())) {
            return wasm::errc::empty_mandatory_field;
        }
        auto checksum_errc = verify_event_checksum(r);
        if (checksum_errc != wasm::errc::none) {
            return checksum_errc;
        }
        le.attrs.description = iobuf_const_parser(desc_itr->value())
                                 .read_string(desc_itr->value_size());
        le.attrs.name = iobuf_const_parser(name_itr->value())
                          .read_string(name_itr->value_size());
    } else {
        if (r.value_size() > 0) {
            /// A 'remove' should have an empty body
            return wasm::errc::unexpected_value;
        }
    }
    return wasm::errc::none;
}

wasm::errc validate_event(const model::record& r) {
    log_event le;
    return parse_event(r, le);
}

absl::btree_map<script_id, log_event>
reconcile_events(std::vector<model::record_batch> events) {
    absl::btree_map<script_id, log_event> wsas;
    for (auto& record_batch : events) {
        record_batch.for_each_record([&wsas](model::record r) {
            log_event le;
            auto mb_error = wasm::parse_event(r, le);
            if (mb_error != wasm::errc::none) {
                vlog(
                  coproclog.error,
                  "Erranous coproc record detected, issue: {}",
                  mb_error);
                return;
            }
            auto id = wasm::get_event_id(r);
            le.source_code = r.share_value();
            wsas.insert_or_assign(*id, std::move(le));
        });
    }
    return wsas;
}

} // namespace coproc::wasm
