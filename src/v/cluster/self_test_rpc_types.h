/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/errc.h"
#include "json/document.h"
#include "model/metadata.h"
#include "serde/serde.h"
#include "utils/uuid.h"

namespace cluster {

enum class self_test_status : int8_t { idle = 0, running, unreachable };

inline ss::sstring self_test_status_as_string(self_test_status sts) {
    switch (sts) {
    case self_test_status::idle:
        return "idle";
    case self_test_status::running:
        return "running";
    case self_test_status::unreachable:
        return "unreachable";
    default:
        __builtin_unreachable();
    }
}

inline std::ostream& operator<<(std::ostream& o, self_test_status sts) {
    fmt::print(o, "{}", self_test_status_as_string(sts));
    return o;
}

/// TODO: Replace fields with actual fields from real self-tests
struct diskcheck_opts
  : serde::
      envelope<diskcheck_opts, serde::version<0>, serde::compat_version<0>> {
    ss::lowres_clock::duration duration;

    static diskcheck_opts from_json(const json::Document& doc) {
        static const auto default_duration = 5;
        return cluster::diskcheck_opts{
          .duration = std::chrono::seconds(
            doc.HasMember("disk_test_execution_time")
              ? doc["disk_test_execution_time"].GetInt()
              : default_duration)};
    }

    friend std::ostream&
    operator<<(std::ostream& o, const diskcheck_opts& opts) {
        fmt::print(o, "{{duration: {}}}", opts.duration.count());
        return o;
    }
};

/// TODO: Replace fields with actual fields from real self-tests
struct netcheck_opts
  : serde::
      envelope<netcheck_opts, serde::version<0>, serde::compat_version<0>> {
    ss::lowres_clock::duration duration;

    static netcheck_opts from_json(const json::Document& doc) {
        static const auto default_duration = 5;
        return cluster::netcheck_opts{
          .duration = std::chrono::seconds(
            doc.HasMember("network_test_execution_time")
              ? doc["network_test_execution_time"].GetInt()
              : default_duration)};
    }

    friend std::ostream&
    operator<<(std::ostream& o, const netcheck_opts& opts) {
        fmt::print(o, "{{test_duration: {}}}", opts.duration.count());
        return o;
    }
};

struct self_test_result
  : serde::
      envelope<self_test_result, serde::version<0>, serde::compat_version<0>> {
    double p50;
    double p90;
    double p99;
    double p999;
    double max;
    uint64_t rps;
    uint64_t bps;
    uuid_t test_id;
    ss::sstring name;
    ss::sstring info;
    ss::lowres_clock::duration duration;
    std::optional<ss::sstring> warning;
    std::optional<ss::sstring> error;

    friend std::ostream&
    operator<<(std::ostream& o, const self_test_result& r) {
        fmt::print(
          o,
          "{{p50: {} p90: {} p99: {} p999: {} max: {} rps: {} bps: {} test_id: "
          "{} name: {} info: {} duration: {}ms warning: {} error: {}}}",
          r.p50,
          r.p90,
          r.p99,
          r.p999,
          r.max,
          r.rps,
          r.bps,
          r.test_id,
          r.name,
          r.info,
          std::chrono::duration_cast<std::chrono::milliseconds>(r.duration)
            .count(),
          r.warning ? *r.warning : "<no_value>",
          r.error ? *r.error : "<no_value>");
        return o;
    }
};

struct empty_request
  : serde::
      envelope<empty_request, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
};

struct start_test_request
  : serde::envelope<
      start_test_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    uuid_t id;
    std::optional<diskcheck_opts> dto;
    std::optional<netcheck_opts> nto;

    friend std::ostream&
    operator<<(std::ostream& o, const start_test_request& r) {
        std::stringstream ss;
        if (r.dto) {
            fmt::print(ss, "diskcheck_opts: {}", *r.dto);
        } else {
            fmt::print(ss, "diskcheck_opts: <no_value>");
        }
        if (r.nto) {
            fmt::print(ss, "netcheck_opts: {}", *r.nto);
        } else {
            fmt::print(ss, "netcheck_opts: <no_value>");
        }
        fmt::print(o, "{{id: {} {}}}", r.id, ss.str());
        return o;
    }
};

struct get_status_response
  : serde::envelope<
      get_status_response,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    uuid_t id{};
    self_test_status status{};
    std::vector<self_test_result> results;

    friend std::ostream&
    operator<<(std::ostream& o, const get_status_response& r) {
        fmt::print(
          o,
          "{{id: {} status:{} test_results: {}}}",
          r.id,
          r.status,
          r.results);
        return o;
    }
};

} // namespace cluster
