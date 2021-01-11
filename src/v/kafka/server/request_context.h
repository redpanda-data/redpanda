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
#include "bytes/iobuf.h"
#include "kafka/protocol/request_reader.h"
#include "kafka/server/connection_context.h"
#include "kafka/server/fetch_session_cache.h"
#include "kafka/server/logger.h"
#include "kafka/server/protocol.h"
#include "kafka/server/response.h"
#include "kafka/types.h"
#include "seastarx.h"
#include "vlog.h"

#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/unaligned.hh>
#include <seastar/util/log.hh>

#include <memory>

namespace kafka {

// Fields may not be byte-aligned since we work
// with the underlying network buffer.
struct [[gnu::packed]] raw_request_header {
    ss::unaligned<int16_t> api_key;
    ss::unaligned<int16_t> api_version;
    ss::unaligned<correlation_id::type> correlation;
    ss::unaligned<int16_t> client_id_size;
};

struct [[gnu::packed]] raw_response_header {
    ss::unaligned<int32_t> size;
    ss::unaligned<correlation_id::type> correlation;
};

struct request_header {
    api_key key;
    api_version version;
    correlation_id correlation;
    ss::temporary_buffer<char> client_id_buffer;
    std::optional<std::string_view> client_id;
};

std::ostream& operator<<(std::ostream&, const request_header&);

class request_context {
public:
    request_context(
      ss::lw_shared_ptr<connection_context> conn,
      request_header&& header,
      iobuf&& request,
      ss::lowres_clock::duration throttle_delay) noexcept
      : _conn(std::move(conn))
      , _header(std::move(header))
      , _reader(std::move(request))
      , _throttle_delay(throttle_delay) {
        // XXX: don't forget to extend the move ctor
    }
    ~request_context() noexcept = default;
    request_context(request_context&& o) noexcept
      : _conn(std::move(o._conn))
      , _header(std::move(o._header))
      , _reader(std::move(o._reader))
      , _throttle_delay(o._throttle_delay) {}
    request_context& operator=(request_context&& o) noexcept {
        if (this != &o) {
            this->~request_context();
            new (this) request_context(std::move(o));
        }
        return *this;
    }
    request_context(const request_context&) = delete;
    request_context& operator=(const request_context&) = delete;

    const request_header& header() const { return _header; }

    request_reader& reader() { return _reader; }

    const cluster::metadata_cache& metadata_cache() const {
        return _conn->server().metadata_cache();
    }

    cluster::metadata_cache& metadata_cache() {
        return _conn->server().metadata_cache();
    }

    cluster::topics_frontend& topics_frontend() const {
        return _conn->server().topics_frontend();
    }

    cluster::id_allocator_frontend& id_allocator_frontend() const {
        return _conn->server().id_allocator_frontend();
    }

    bool is_idempotence_enabled() {
        return _conn->server().is_idempotence_enabled();
    }

    int32_t throttle_delay_ms() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                 _throttle_delay)
          .count();
    }

    kafka::group_router& groups() { return _conn->server().group_router(); }

    cluster::shard_table& shards() { return _conn->server().shard_table(); }

    ss::sharded<cluster::partition_manager>& partition_manager() {
        return _conn->server().partition_manager();
    }

    fetch_session_cache& fetch_sessions() {
        return _conn->server().fetch_sessions_cache();
    }

    // clang-format off
    template<typename ResponseType>
    CONCEPT(requires requires (
            ResponseType r, const request_context& ctx, response& resp) {
        { r.encode(ctx, resp) } -> std::same_as<void>;
    })
    // clang-format on
    ss::future<response_ptr> respond(ResponseType r) {
        vlog(
          klog.trace,
          "sending {}:{} response {}",
          ResponseType::api_type::key,
          ResponseType::api_type::name,
          r);
        auto resp = std::make_unique<response>();
        r.encode(*this, *resp.get());
        return ss::make_ready_future<response_ptr>(std::move(resp));
    }

    coordinator_ntp_mapper& coordinator_mapper() {
        return _conn->server().coordinator_mapper();
    }

    const ss::sstring& listener() const { return _conn->listener(); }

private:
    ss::lw_shared_ptr<connection_context> _conn;
    request_header _header;
    request_reader _reader;
    ss::lowres_clock::duration _throttle_delay;
};

// Executes the API call identified by the specified request_context.
ss::future<response_ptr>
process_request(request_context&&, ss::smp_service_group);

} // namespace kafka
