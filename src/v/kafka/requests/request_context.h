#pragma once
#include "bytes/iobuf.h"
#include "kafka/logger.h"
#include "kafka/requests/request_reader.h"
#include "kafka/types.h"
#include "seastarx.h"
#include "vlog.h"

#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/unaligned.hh>
#include <seastar/util/log.hh>

#include <memory>

namespace cluster {
class metadata_cache;
class partition_manager;
class shard_table;
class topics_frontend;
} // namespace cluster

namespace kafka {
class coordinator_ntp_mapper;

template<typename T>
class group_router;

class group_manager;

using group_router_type = kafka::group_router<kafka::group_manager>;

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

class response;
using response_ptr = ss::foreign_ptr<std::unique_ptr<response>>;

class request_context {
public:
    request_context(
      ss::sharded<cluster::metadata_cache>& metadata_cache,
      cluster::topics_frontend& topics_frontend,
      request_header&& header,
      iobuf&& request,
      ss::lowres_clock::duration throttle_delay,
      kafka::group_router_type& group_router,
      cluster::shard_table& shard_table,
      ss::sharded<cluster::partition_manager>& partition_manager,
      ss::sharded<coordinator_ntp_mapper>& coordinator_mapper) noexcept
      : _metadata_cache(&metadata_cache)
      , _topics_frontend(&topics_frontend)
      , _header(std::move(header))
      , _reader(std::move(request))
      , _throttle_delay(throttle_delay)
      , _group_router(&group_router)
      , _shard_table(&shard_table)
      , _partition_manager(&partition_manager)
      , _coordinator_mapper(&coordinator_mapper) {
        // XXX: don't forget to extend the move ctor
    }
    ~request_context() noexcept = default;
    request_context(request_context&& o) noexcept
      : _metadata_cache(o._metadata_cache)
      , _topics_frontend(o._topics_frontend)
      , _header(std::move(o._header))
      , _reader(std::move(o._reader))
      , _throttle_delay(o._throttle_delay)
      , _group_router(o._group_router)
      , _shard_table(o._shard_table)
      , _partition_manager(o._partition_manager)
      , _coordinator_mapper(o._coordinator_mapper) {}
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
        return _metadata_cache->local();
    }

    cluster::metadata_cache& metadata_cache() {
        return _metadata_cache->local();
    }

    cluster::topics_frontend& topics_frontend() const {
        return *_topics_frontend;
    }

    int32_t throttle_delay_ms() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                 _throttle_delay)
          .count();
    }

    kafka::group_router_type& groups() { return *_group_router; }

    cluster::shard_table& shards() { return *_shard_table; }

    ss::sharded<cluster::partition_manager>& partition_manager() {
        return *_partition_manager;
    }

    // clang-format off
    template<typename ResponseType>
    CONCEPT(requires requires (
            ResponseType r, const request_context& ctx, response& resp) {
        { r.encode(ctx, resp) } -> void;
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

    ss::sharded<kafka::coordinator_ntp_mapper>& coordinator_mapper() {
        return *_coordinator_mapper;
    }

private:
    ss::sharded<cluster::metadata_cache>* _metadata_cache;
    cluster::topics_frontend* _topics_frontend;
    request_header _header;
    request_reader _reader;
    ss::lowres_clock::duration _throttle_delay;
    kafka::group_router_type* _group_router;
    cluster::shard_table* _shard_table;
    ss::sharded<cluster::partition_manager>* _partition_manager;
    ss::sharded<kafka::coordinator_ntp_mapper>* _coordinator_mapper;
};

// Executes the API call identified by the specified request_context.
ss::future<response_ptr>
process_request(request_context&&, ss::smp_service_group);

} // namespace kafka
