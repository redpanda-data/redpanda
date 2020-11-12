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
#include <system_error>

namespace coproc {

enum class errc {
    success = 0,
    invalid_ingestion_policy,
    script_id_already_exists,
    script_id_does_not_exist,
    topic_already_enabled,
    topic_does_not_exist,
    topic_never_enabled,
    invalid_topic,
    materialized_topic,
    internal_error
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
        case errc::topic_already_enabled:
            return "Topic already enabled on behalf of a coprocessor";
        case errc::topic_does_not_exist:
            return "Topic does not exist yet";
        case errc::topic_never_enabled:
            return "Topic never enabled on behalf of a coprocessor";
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
