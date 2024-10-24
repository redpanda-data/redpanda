/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/recovery_errors.h"

#include "base/outcome.h"

namespace cloud_storage {

const char* recovery_error_category::name() const noexcept {
    return "recovery_error_code";
}

std::string recovery_error_category::message(int c) const {
    switch (static_cast<recovery_error_code>(c)) {
    case recovery_error_code::success:
        return "recovery successful";
    case recovery_error_code::invalid_client_config:
        return "An unsupported cloud storage client config was specified";
    case recovery_error_code::error_listing_items:
        return "Failed to list items on bucket";
    case recovery_error_code::recovery_already_running:
        return "An instance of recovery is already running";
    case recovery_error_code::error_downloading_manifest:
        return "Failed to download manifest";
    case recovery_error_code::error_creating_topics:
        return "Failed to create topics";
    default:
        return "unknown";
    }
}

const std::error_category& error_category() noexcept {
    static const recovery_error_category e;
    return e;
}

std::error_code make_error_code(recovery_error_code e) noexcept {
    return {static_cast<int>(e), error_category()};
}

recovery_error_ctx
recovery_error_ctx::make(ss::sstring context, recovery_error_code error_code) {
    return {.error_code = error_code, .context = std::move(context)};
}

std::error_code make_error_code(const recovery_error_ctx& r) noexcept {
    return make_error_code(r.error_code);
}

void outcome_throw_as_system_error_with_payload(recovery_error_ctx r) {
    outcome::try_throw_std_exception_from_error(make_error_code(r));
    throw std::runtime_error(r.context);
}

} // namespace cloud_storage
