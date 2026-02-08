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
#include "rpc/errc.h"
#include "serde/envelope.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"

namespace cloud_topics::l0::gc::rpc {

enum class errc : int16_t {
    ok = 0,
    not_ready,
    shutting_down,
};

struct invalidate_epoch_request
  : serde::envelope<
      invalidate_epoch_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = struct invalidate_epoch_response;

    cluster_epoch candidate;

    auto serde_fields() { return std::tie(candidate); }
};

struct invalidate_epoch_response
  : serde::envelope<
      invalidate_epoch_response,
      serde::version<0>,
      serde::compat_version<0>> {
    ::rpc::errc ec{::rpc::errc::success};

    auto serde_fields() { return std::tie(ec); }
};

struct poll_drain_request
  : serde::envelope<
      poll_drain_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = struct poll_drain_response;

    cluster_epoch candidate;

    auto serde_fields() { return std::tie(candidate); }
};

struct poll_drain_response
  : serde::envelope<
      poll_drain_response,
      serde::version<0>,
      serde::compat_version<0>> {
    bool drained{false};
    ::rpc::errc ec{::rpc::errc::success};

    auto serde_fields() { return std::tie(drained, ec); }
};

struct publish_safe_epoch_request
  : serde::envelope<
      publish_safe_epoch_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = struct publish_safe_epoch_response;

    cluster_epoch safe_epoch;

    auto serde_fields() { return std::tie(safe_epoch); }
};

struct publish_safe_epoch_response
  : serde::envelope<
      publish_safe_epoch_response,
      serde::version<0>,
      serde::compat_version<0>> {
    ::rpc::errc ec{::rpc::errc::success};

    auto serde_fields() { return std::tie(ec); }
};

} // namespace cloud_topics::l0::gc::rpc

template<>
struct fmt::formatter<cloud_topics::l0::gc::rpc::errc> final
  : fmt::formatter<std::string_view> {
    using errc = cloud_topics::l0::gc::rpc::errc;
    template<typename FormatContext>
    auto format(const errc& ec, FormatContext& ctx) const {
        switch (ec) {
        case errc::ok:
            return fmt::format_to(ctx.out(), "rpc::errc::ok");
        case errc::not_ready:
            return fmt::format_to(ctx.out(), "rpc::errc::not_ready");
        case errc::shutting_down:
            return fmt::format_to(ctx.out(), "rpc::errc::shutting_down");
        }
        return fmt::format_to(
          ctx.out(), "rpc::errc::unknown({})", static_cast<int>(ec));
    }
};
