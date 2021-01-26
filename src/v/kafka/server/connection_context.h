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
#include "kafka/server/protocol.h"
#include "kafka/server/response.h"
#include "rpc/server.h"
#include "seastarx.h"
#include "utils/hdr_hist.h"
#include "utils/named_type.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_map.h>

#include <memory>

namespace kafka {

struct request_header;
class request_context;

class connection_context final
  : public ss::enable_lw_shared_from_this<connection_context> {
public:
    connection_context(protocol& p, rpc::server::resources&& r) noexcept
      : _proto(p)
      , _rs(std::move(r)) {}
    ~connection_context() noexcept = default;
    connection_context(const connection_context&) = delete;
    connection_context(connection_context&&) = delete;
    connection_context& operator=(const connection_context&) = delete;
    connection_context& operator=(connection_context&&) = delete;

    protocol& server() { return _proto; }
    const ss::sstring& listener() const { return _rs.conn->name(); }

    ss::future<> process_one_request();
    bool is_finished_parsing() const;

private:
    // used to pass around some internal state
    struct session_resources {
        ss::lowres_clock::duration backpressure_delay;
        ss::semaphore_units<> memlocks;
        std::unique_ptr<hdr_hist::measurement> method_latency;
    };

    /// called by throttle_request
    ss::future<ss::semaphore_units<>> reserve_request_units(size_t size);

    /// apply correct backpressure sequence
    ss::future<session_resources>
    throttle_request(std::optional<std::string_view>, size_t sz);

    ss::future<> dispatch_method_once(request_header, size_t sz);
    ss::future<> process_next_response();
    ss::future<> do_process(request_context);

private:
    using sequence_id = named_type<uint64_t, struct kafka_protocol_sequence>;
    using map_t = absl::flat_hash_map<sequence_id, response_ptr>;

    protocol& _proto;
    rpc::server::resources _rs;
    sequence_id _next_response;
    sequence_id _seq_idx;
    map_t _responses;
};

} // namespace kafka
