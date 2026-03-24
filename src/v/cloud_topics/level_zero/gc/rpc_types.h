/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/types.h"
#include "serde/envelope.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/optional.h"

namespace cloud_topics::l0::gc::rpc {

struct barrier_request
  : serde::
      envelope<barrier_request, serde::version<0>, serde::compat_version<0>> {
    using resp_t = struct barrier_response;

    cluster_epoch candidate;
    std::optional<cluster_epoch> safe_epoch;

    auto serde_fields() { return std::tie(candidate, safe_epoch); }
};

struct barrier_response
  : serde::
      envelope<barrier_response, serde::version<0>, serde::compat_version<0>> {
    enum class status : uint8_t {
        ready,
        pending,
        error,
    };

    status s{status::error};

    auto serde_fields() { return std::tie(s); }
};

} // namespace cloud_topics::l0::gc::rpc

template<>
struct fmt::formatter<cloud_topics::l0::gc::rpc::barrier_response::status> final
  : fmt::formatter<std::string_view> {
    using status = cloud_topics::l0::gc::rpc::barrier_response::status;
    template<typename FormatContext>
    auto format(const status& s, FormatContext& ctx) const {
        switch (s) {
        case status::ready:
            return fmt::format_to(ctx.out(), "barrier::ready");
        case status::pending:
            return fmt::format_to(ctx.out(), "barrier::pending");
        case status::error:
            return fmt::format_to(ctx.out(), "barrier::error");
        }
        return fmt::format_to(
          ctx.out(), "barrier::unknown({})", static_cast<int>(s));
    }
};
