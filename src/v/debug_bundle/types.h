/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "container/fragmented_vector.h"
#include "model/metadata.h"
#include "strings/string_switch.h"
#include "utils/uuid.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/btree_set.h>
#include <fmt/format.h>

#include <chrono>
#include <optional>
#include <ostream>
#include <variant>
namespace debug_bundle {

/// Special date strings used by journalctl
enum class special_date { yesterday, today, now, tomorrow };

constexpr std::string_view to_string_view(special_date d) {
    switch (d) {
    case special_date::yesterday:
        return "yesterday";
    case special_date::today:
        return "today";
    case special_date::now:
        return "now";
    case special_date::tomorrow:
        return "tomorrow";
    }
}

inline std::ostream& operator<<(std::ostream& o, const special_date& d) {
    return o << to_string_view(d);
}

inline std::istream& operator>>(std::istream& i, special_date& d) {
    ss::sstring s;
    i >> s;
    d = string_switch<special_date>(s)
          .match(
            to_string_view(special_date::yesterday), special_date::yesterday)
          .match(to_string_view(special_date::today), special_date::today)
          .match(to_string_view(special_date::now), special_date::now)
          .match(
            to_string_view(special_date::tomorrow), special_date::tomorrow);
    return i;
}

/// Defines which clock the debug bundle will use
using debug_bundle_clock = ss::lowres_system_clock;
/// A timepoint
using debug_bundle_timepoint = std::chrono::time_point<debug_bundle_clock>;
/// When provided a time, may either be a time point or a special_date
using time_variant = std::variant<debug_bundle_timepoint, special_date>;

/// SCRAM credentials for authn
struct scram_creds {
    ss::sstring username;
    ss::sstring password;
    ss::sstring mechanism;
};
/// Variant so it can be expanded as new authn methods are added to rpk
using debug_bundle_authn_options = std::variant<scram_creds>;

/// Used to collect topics and partitions for the "--partitions" option for "rpk
/// debug_bundle"
using partition_selection
  = std::pair<model::topic_namespace, absl::btree_set<model::partition_id>>;

/// Parameters used to spawn rpk debug bundle
struct debug_bundle_parameters {
    std::optional<debug_bundle_authn_options> authn_options;
    std::optional<uint64_t> controller_logs_size_limit_bytes;
    std::optional<std::chrono::seconds> cpu_profiler_wait_seconds;
    std::optional<time_variant> logs_since;
    std::optional<uint64_t> logs_size_limit_bytes;
    std::optional<time_variant> logs_until;
    std::optional<std::chrono::seconds> metrics_interval_seconds;
    std::optional<std::vector<partition_selection>> partition;
};

/// The state of the debug bundle process
enum class debug_bundle_status { running, success, error };

constexpr std::string_view to_string_view(debug_bundle_status s) {
    switch (s) {
    case debug_bundle_status::running:
        return "running";
    case debug_bundle_status::success:
        return "success";
    case debug_bundle_status::error:
        return "error";
    }
}

inline std::ostream& operator<<(std::ostream& o, const debug_bundle_status& s) {
    return o << to_string_view(s);
}

/// Status of the debug bundle process
struct debug_bundle_status_data {
    uuid_t job_id;
    debug_bundle_status status;
    int64_t created_timestamp;
    ss::sstring file_name;
    chunked_vector<ss::sstring> stdout;
    chunked_vector<ss::sstring> stderr;
};
} // namespace debug_bundle

template<>
struct fmt::formatter<debug_bundle::special_date>
  : formatter<std::string_view> {
    auto format(debug_bundle::special_date d, format_context& ctx) const
      -> format_context::iterator;
};

template<>
struct fmt::formatter<debug_bundle::time_variant>
  : formatter<std::string_view> {
    fmt::format_context::iterator
    format(const debug_bundle::time_variant&, format_context& ctx) const;
};

template<>
struct fmt::formatter<debug_bundle::partition_selection>
  : formatter<std::string_view> {
    fmt::format_context::iterator
    format(const debug_bundle::partition_selection&, format_context& ctx) const;
};
