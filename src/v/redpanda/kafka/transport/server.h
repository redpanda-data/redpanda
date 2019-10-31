#pragma once

#include "redpanda/kafka/requests/request_context.h"
#include "redpanda/kafka/transport/probe.h"
#include "redpanda/kafka/transport/quota_manager.h"
#include "seastarx.h"
#include "utils/fragbuf.h"

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

using size_type = int32_t;

// Fields may not be byte-aligned since we work
// with the underlying network buffer.
struct [[gnu::packed]] raw_request_header {
    unaligned<int16_t> api_key;
    unaligned<int16_t> api_version;
    unaligned<correlation_type> correlation_id;
    unaligned<int16_t> client_id_size;
};

struct [[gnu::packed]] raw_response_header {
    unaligned<size_type> size;
    unaligned<correlation_type> correlation_id;
};

struct kafka_server_config {
    size_t max_request_size;
    smp_service_group smp_group;
    std::optional<tls::credentials_builder> credentials;
};

class kafka_server {
public:
    kafka_server(
      probe,
      sharded<cluster::metadata_cache>&,
      sharded<controller_dispatcher>&,
      kafka_server_config,
      sharded<quota_manager>& quota_mgr,
      sharded<kafka::group_router_type>& group_router) noexcept;
    future<> listen(socket_address server_addr, bool keepalive);
    future<> do_accepts(int which, net::inet_address server_addr);
    future<> stop();

    class connection : public boost::intrusive::list_base_hook<> {
    public:
        connection(
          kafka_server& server, connected_socket&& fd, socket_address addr);
        ~connection();
        void shutdown();
        future<> process();

        static future<request_header> read_header(input_stream<char>& src);

        static size_t
        process_size(const input_stream<char>& src, temporary_buffer<char>&&);

    private:
        future<> process_request();
        void do_process(request_context&&, semaphore_units<>&&);
        future<> write_response(response_ptr&&, correlation_type);

    private:
        kafka_server& _server;
        connected_socket _fd;
        socket_address _addr;
        input_stream<char> _read_buf;
        output_stream<char> _write_buf;
        fragbuf::reader _buffer_reader;
        future<> _ready_to_respond = make_ready_future<>();
    };

private:
    future<> do_accepts(int which, bool keepalive);

    probe _probe;
    sharded<controller_dispatcher>& _cntrl_dispatcher;
    sharded<cluster::metadata_cache>& _metadata_cache;
    size_t _max_request_size;
    semaphore _memory_available;
    smp_service_group _smp_group;
    std::vector<server_socket> _listeners;
    boost::intrusive::list<connection> _connections;
    abort_source _as;
    gate _listeners_and_connections;
    sharded<quota_manager>& _quota_mgr;
    shared_ptr<tls::server_credentials> _creds;
    metrics::metric_groups _metrics;
    sharded<kafka::group_router_type>& _group_router;
};

} // namespace kafka
