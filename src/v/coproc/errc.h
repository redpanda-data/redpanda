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
#include <ostream>
#include <system_error>
namespace coproc {

namespace wasm {

enum class errc {
    none = 0,
    mismatched_checksum,
    empty_mandatory_field,
    missing_header_key,
    unexpected_action_type,
    unexpected_value,
    unexpected_coproc_type
};

struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "coproc::wasm::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::none:
            return "No error";
        case errc::mismatched_checksum:
            return "wasm::events checksum doesn't match expected value";
        case errc::empty_mandatory_field:
            return "wasm::event missing a mandatory field";
        case errc::missing_header_key:
            return "wasm::event missing a mandatory header key";
        case errc::unexpected_action_type:
            return "wasm::event contains an unexpected action type";
        case errc::unexpected_value:
            return "wasm::event contains an unexpected value() payload";
        default:
            return "Undefined wasm::errc encountered";
        }
    }
};
inline const std::error_category& error_category() noexcept {
    static errc_category e;
    return e;
}
inline std::error_code make_error_code(errc e) noexcept {
    return std::error_code(static_cast<int>(e), error_category());
}

} // namespace wasm

enum class errc {
    success = 0,
    internal_error,
    topic_does_not_exist,
    invalid_topic,
    materialized_topic,
    script_id_does_not_exist,
    partition_not_exists,
    partition_already_exists
};

struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "coproc::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "Success";
        case errc::internal_error:
            return "Internal error";
        case errc::topic_does_not_exist:
            return "Topic does not exist yet";
        case errc::invalid_topic:
            return "Topic name is invalid";
        case errc::materialized_topic:
            return "Topic is already a materialized topic";
        case errc::script_id_does_not_exist:
            return "Could not find coprocessor with matching script_id";
        case errc::partition_not_exists:
            return "Partition missing from coproc::partition_manager";
        case errc::partition_already_exists:
            return "Partition alreday exists in coproc::partition_manager";
        default:
            return "Undefined coprocessor error encountered";
        }
    }
};
inline const std::error_category& error_category() noexcept {
    static errc_category e;
    return e;
}
inline std::error_code make_error_code(errc e) noexcept {
    return std::error_code(static_cast<int>(e), error_category());
}
} // namespace coproc
namespace std {
template<>
struct is_error_code_enum<coproc::errc> : true_type {};
template<>
struct is_error_code_enum<coproc::wasm::errc> : true_type {};
} // namespace std
