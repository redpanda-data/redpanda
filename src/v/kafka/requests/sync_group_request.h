/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "kafka/errors.h"
#include "kafka/requests/response.h"
#include "kafka/requests/schemata/sync_group_request.h"
#include "kafka/requests/schemata/sync_group_response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

#include <utility>

namespace kafka {

struct sync_group_api final {
    static constexpr const char* name = "sync group";
    static constexpr api_key key = api_key(14);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(3);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct sync_group_request final {
    sync_group_request_data data;

    // set during request processing after mapping group to ntp
    model::ntp ntp;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }

    assignments_type member_assignments() && {
        assignments_type res;
        res.reserve(data.assignments.size());
        std::for_each(
          std::begin(data.assignments),
          std::end(data.assignments),
          [&res](sync_group_request_assignment& a) mutable {
              res.emplace(std::move(a.member_id), std::move(a.assignment));
          });
        return res;
    }
};

inline std::ostream& operator<<(std::ostream& os, const sync_group_request& r) {
    return os << r.data;
}

struct sync_group_response final {
    using api_type = sync_group_api;

    sync_group_response_data data;

    sync_group_response(error_code error, bytes assignment)
      : data({
        .throttle_time_ms = std::chrono::milliseconds(0),
        .error_code = error,
        .assignment = std::move(assignment),
      }) {}

    explicit sync_group_response(error_code error)
      : sync_group_response(error, bytes()) {}

    sync_group_response(const sync_group_request&, error_code error)
      : sync_group_response(error) {}

    void encode(const request_context&, response&);
};

static inline ss::future<sync_group_response>
make_sync_error(error_code error) {
    return ss::make_ready_future<sync_group_response>(error);
}

inline std::ostream&
operator<<(std::ostream& os, const sync_group_response& r) {
    return os << r.data;
}

} // namespace kafka
