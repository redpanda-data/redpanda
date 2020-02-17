#pragma once

#include "bytes/iobuf.h"
#include "kafka/probe.h"
#include "kafka/quota_manager.h"
#include "kafka/requests/request_context.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/execution_stage.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/unaligned.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/tls.hh>

#include <boost/intrusive/list.hpp>

#include <cstdint>
#include <optional>
#include <vector>

namespace cluster {
class metadata_cache;
}

namespace kafka {
struct request_header;
}

namespace kafka {
class controller_dispatcher;
}

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

struct kafka_server_config {
    size_t max_request_size;
    ss::smp_service_group smp_group;
    std::optional<ss::tls::credentials_builder> credentials;
};

class kafka_server {
public:
    kafka_server(
      probe,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<controller_dispatcher>&,
      kafka_server_config,
      ss::sharded<quota_manager>& quota_mgr,
      ss::sharded<kafka::group_router_type>& group_router,
      ss::sharded<cluster::shard_table>& shard_table,
      ss::sharded<cluster::partition_manager>& partition_manager) noexcept;
    ss::future<> listen(ss::socket_address server_addr, bool keepalive);
    ss::future<> do_accepts(int which, ss::net::inet_address server_addr);
    ss::future<> stop();

    class connection : public boost::intrusive::list_base_hook<> {
    public:
        connection(
          kafka_server& server,
          ss::connected_socket&& fd,
          ss::socket_address addr);
        ~connection();
        void shutdown();
        ss::future<> process();

        static ss::future<request_header>
        read_header(ss::input_stream<char>& src);

        static size_t process_size(
          const ss::input_stream<char>& src, ss::temporary_buffer<char>&&);

    private:
        ss::future<> process_request();
        void do_process(request_context&&, ss::semaphore_units<>&&);
        ss::future<> write_response(response_ptr&&, correlation_id);

    private:
        kafka_server& _server;
        ss::connected_socket _fd;
        ss::socket_address _addr;
        ss::input_stream<char> _read_buf;
        ss::output_stream<char> _write_buf;
        ss::future<> _ready_to_respond = ss::make_ready_future<>();
    };

private:
    ss::future<> do_accepts(int which, bool keepalive);

    probe _probe;
    ss::sharded<controller_dispatcher>& _cntrl_dispatcher;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    size_t _max_request_size;
    ss::semaphore _memory_available;
    ss::smp_service_group _smp_group;
    std::vector<ss::server_socket> _listeners;
    boost::intrusive::list<connection> _connections;
    ss::abort_source _as;
    ss::gate _listeners_and_connections;
    ss::sharded<quota_manager>& _quota_mgr;
    ss::shared_ptr<ss::tls::server_credentials> _creds;
    ss::metrics::metric_groups _metrics;
    ss::sharded<kafka::group_router_type>& _group_router;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<cluster::partition_manager>& _partition_manager;
};

} // namespace kafka
