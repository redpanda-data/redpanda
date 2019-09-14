#pragma once

#include "cluster/metadata_cache.h"
#include "redpanda/kafka/requests/headers.h"
#include "redpanda/kafka/requests/request_reader.h"
#include "seastarx.h"
#include "utils/fragbuf.h"

#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/log.hh>

#include <memory>

namespace kafka::requests {

extern logger kreq_log;

class request_context {
public:
    request_context(
      sharded<cluster::metadata_cache>& metadata_cache,
      request_header&& header,
      fragbuf&& request) noexcept
      : _metadata_cache(metadata_cache)
      , _header(std::move(header))
      , _request(std::move(request))
      , _reader(_request.get_istream()) {
    }

    const request_header& header() const {
        return _header;
    }

    request_reader& reader() {
        return _reader;
    }

    const cluster::metadata_cache& metadata_cache() const {
        return _metadata_cache.local();
    }

private:
    sharded<cluster::metadata_cache>& _metadata_cache;
    request_header _header;
    fragbuf _request;
    request_reader _reader;
};

class response;
using response_ptr = foreign_ptr<std::unique_ptr<response>>;

// Executes the API call identified by the specified request_context.
future<response_ptr> process_request(request_context&, smp_service_group);

} // namespace kafka::requests
