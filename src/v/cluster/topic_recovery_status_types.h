/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/topic_recovery_service.h"
#include "serde/envelope.h"

#include <type_traits>

namespace cluster {

struct status_request
  : serde::
      envelope<status_request, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    auto serde_fields() { return std::tie(); }
};

struct topic_downloads
  : serde::
      envelope<topic_downloads, serde::version<0>, serde::compat_version<0>> {
    model::topic_namespace tp_ns;
    int pending_downloads;
    int successful_downloads;
    int failed_downloads;

    auto serde_fields() {
        return std::tie(
          tp_ns, pending_downloads, successful_downloads, failed_downloads);
    }

    friend bool operator==(const topic_downloads&, const topic_downloads&)
      = default;
};

struct recovery_request_params
  : serde::envelope<
      recovery_request_params,
      serde::version<0>,
      serde::compat_version<0>> {
    std::optional<ss::sstring> topic_names_pattern;
    std::optional<size_t> retention_bytes;
    std::optional<std::chrono::milliseconds> retention_ms;

    void populate(const std::optional<cloud_storage::recovery_request>& r);

    auto serde_fields() {
        return std::tie(topic_names_pattern, retention_bytes, retention_ms);
    }

    friend bool
    operator==(const recovery_request_params&, const recovery_request_params&)
      = default;
};

std::ostream& operator<<(std::ostream&, const recovery_request_params&);

struct single_status
  : serde::
      envelope<single_status, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    cloud_storage::topic_recovery_service::state state;
    std::vector<topic_downloads> download_counts;
    recovery_request_params request;
    auto serde_fields() { return std::tie(state, download_counts, request); }

    bool is_active() const;

    friend bool operator==(const single_status&, const single_status&)
      = default;
};

struct status_response
  : serde::
      envelope<status_response, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    std::vector<single_status> status_log;
    auto serde_fields() { return std::tie(status_log); }

    bool is_active() const;

    friend bool operator==(const status_response&, const status_response&)
      = default;
};

} // namespace cluster
