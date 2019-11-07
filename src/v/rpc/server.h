#pragma once

#include "rpc/connection.h"
#include "rpc/netbuf.h"
#include "rpc/service.h"
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
class server_context_impl;

class server {
public:
    explicit server(server_configuration c);
    server(const server&) = delete;
    ~server();

    void start();
    future<> stop();

    template<typename T, typename... Args>
    void register_service(Args&&... args) {
        static_assert(std::is_base_of_v<service, T>, "must extend service.h");
        _services.push_back(std::make_unique<T>(std::forward<Args>(args)...));
    }

    const server_configuration cfg;
    const hdr_hist& histogram() const {
        return _hist;
    }

private:
    friend server_context_impl;

    future<> accept(server_socket&);
    future<> continous_method_dispath(lw_shared_ptr<connection>);
    future<> dispatch_method_once(header, lw_shared_ptr<connection>);
    future<> handle_connection(lw_shared_ptr<connection>);
    void setup_metrics();

    semaphore _memory;
    std::vector<std::unique_ptr<service>> _services;
    std::vector<server_socket> _listeners;
    boost::intrusive::list<connection> _connections;
    abort_source _as;
    gate _conn_gate;
    hdr_hist _hist;
    server_probe _probe;
    metrics::metric_groups _metrics;
    shared_ptr<tls::server_credentials> _creds;
};

} // namespace rpc
