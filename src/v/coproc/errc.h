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
    unexpected_value
};

inline std::ostream& operator<<(std::ostream& os, errc err) {
    switch (err) {
    case errc::none:
        os << "none";
        break;
    case errc::mismatched_checksum:
        os << "mismatched_checksum";
        break;
    case errc::empty_mandatory_field:
        os << "empty_mandatory_field";
        break;
    case errc::missing_header_key:
        os << "missing_header_key";
        break;
    case errc::unexpected_action_type:
        os << "unexpected_action_type";
        break;
    case errc::unexpected_value:
        os << "unexpected_value";
        break;
    default:
        os << "missing error type";
    }
    return os;
}

} // namespace wasm

enum class errc {
    success = 0,
    internal_error,
    invalid_ingestion_policy,
    script_id_already_exists,
    topic_does_not_exist,
    invalid_topic,
    materialized_topic,
    script_id_does_not_exist
};

struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "coproc::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "Success";
        case errc::invalid_ingestion_policy:
            return "Ingestion policy not yet supported";
        case errc::script_id_already_exists:
            return "Attempted double registration encountered";
        case errc::script_id_does_not_exist:
            return "Could not find coprocessor with matching script_id";
        case errc::topic_does_not_exist:
            return "Topic does not exist yet";
        case errc::invalid_topic:
            return "Topic name is invalid";
        case errc::materialized_topic:
            return "Topic is already a materialized topic";
        default:
            return "coproc::errc::internal_error check the logs";
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
} // namespace std
