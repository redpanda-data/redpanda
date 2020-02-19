#pragma once

#include "rpc/connection.h"
#include "rpc/types.h"
#include "utils/hdr_hist.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/semaphore.hh>

#include <boost/intrusive/list.hpp>

#include <list>
#include <type_traits>
#include <vector>

namespace rpc {

class server {
public:
    // always guaranteed non-null
    class resources final {
    public:
        resources(server* s, ss::lw_shared_ptr<connection> c)
          : conn(std::move(c))
          , _s(s) {}

        // NOLINTNEXTLINE
        ss::lw_shared_ptr<connection> conn;

        server_probe& probe() { return _s->_probe; }
        ss::semaphore& memory() { return _s->_memory; }
        hdr_hist& hist() { return _s->_hist; }
        ss::gate& conn_gate() { return _s->_conn_gate; }
        bool abort_requested() const { return _s->_as.abort_requested(); }

    private:
        server* _s;
    };
    struct protocol {
        protocol() noexcept = default;
        protocol(protocol&&) noexcept = default;
        protocol& operator=(protocol&&) noexcept = default;
        protocol(const protocol&) = delete;
        protocol& operator=(const protocol&) = delete;

        virtual ~protocol() noexcept = default;
        virtual const char* name() const = 0;
        // the lifetime of all references here are guaranteed to live
        // until the end of the server (container/parent)
        virtual ss::future<> apply(server::resources) = 0;
    };

    explicit server(server_configuration);
    server(server&&) noexcept = default;
    server& operator=(server&&) noexcept = delete;
    server(const server&) = delete;
    server& operator=(const server&) = delete;
    ~server();

    void set_protocol(std::unique_ptr<protocol> proto) {
        _proto = std::move(proto);
    }
    void start();
    ss::future<> stop();

    const server_configuration cfg; // NOLINT
    const hdr_hist& histogram() const { return _hist; }

private:
    friend resources;
    ss::future<> accept(ss::server_socket&);
    void setup_metrics();

    std::unique_ptr<protocol> _proto;
    ss::semaphore _memory;
    std::vector<std::unique_ptr<ss::server_socket>> _listeners;
    boost::intrusive::list<connection> _connections;
    ss::abort_source _as;
    ss::gate _conn_gate;
    hdr_hist _hist;
    server_probe _probe;
    ss::metrics::metric_groups _metrics;
    ss::shared_ptr<ss::tls::server_credentials> _creds;
};

} // namespace rpc
