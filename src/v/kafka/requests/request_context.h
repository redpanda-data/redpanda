#pragma once

#include "bytes/iobuf.h"
#include "cluster/controller.h"
#include "kafka/groups/group_manager.h"
#include "kafka/groups/group_router.h"
#include "kafka/groups/group_shard_mapper.h"
#include "kafka/requests/request_reader.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/unaligned.hh>
#include <seastar/util/log.hh>

#include <memory>

namespace cluster {
class metadata_cache;
}

namespace kafka {
using group_router_type = kafka::group_router<
  kafka::group_manager,
  kafka::group_shard_mapper<cluster::shard_table>>;

class controller_dispatcher;

extern ss::logger kreq_log;
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
      ss::sharded<cluster::metadata_cache>& metadata_cache,
      controller_dispatcher& cntrl_dispatcher,
      request_header&& header,
      iobuf&& request,
      ss::lowres_clock::duration throttle_delay,
      kafka::group_router_type& group_router,
      cluster::shard_table& shard_table,
      ss::sharded<cluster::partition_manager>& partition_manager) noexcept
      : _metadata_cache(metadata_cache)
      , _cntrl_dispatcher(cntrl_dispatcher)
      , _header(std::move(header))
      , _reader(std::move(request))
      , _throttle_delay(throttle_delay)
      , _group_router(group_router)
      , _shard_table(shard_table)
      , _partition_manager(partition_manager) {}

    request_context(request_context&&) noexcept = default;

    const request_header& header() const { return _header; }

    request_reader& reader() { return _reader; }

    const cluster::metadata_cache& metadata_cache() const {
        return _metadata_cache.local();
    }

    controller_dispatcher& cntrl_dispatcher() const {
        return _cntrl_dispatcher;
    }

    int32_t throttle_delay_ms() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                 _throttle_delay)
          .count();
    }

    kafka::group_router_type& groups() { return _group_router; }

    cluster::shard_table& shards() { return _shard_table; }

    ss::sharded<cluster::partition_manager>& partition_manager() {
        return _partition_manager;
    }

private:
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    controller_dispatcher& _cntrl_dispatcher;
    request_header _header;
    request_reader _reader;
    ss::lowres_clock::duration _throttle_delay;
    kafka::group_router_type& _group_router;
    cluster::shard_table& _shard_table;
    ss::sharded<cluster::partition_manager>& _partition_manager;
};

class response;
using response_ptr = ss::foreign_ptr<std::unique_ptr<response>>;

// Executes the API call identified by the specified request_context.
ss::future<response_ptr>
process_request(request_context&&, ss::smp_service_group);

} // namespace kafka
