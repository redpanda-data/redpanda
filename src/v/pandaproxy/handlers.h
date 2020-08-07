#include "pandaproxy/server.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace pandaproxy {

ss::future<server::reply_t>
get_topics_names(server::request_t rq, server::reply_t rp);

} // namespace pandaproxy
