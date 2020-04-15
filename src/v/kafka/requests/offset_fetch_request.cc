#include "kafka/requests/offset_fetch_request.h"

#include "kafka/errors.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "model/metadata.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

static std::ostream&
operator<<(std::ostream& o, const offset_fetch_request_topic& t) {
    return ss::fmt_print(
      o, "topic={} partitions={}", t.name, t.partition_indexes);
}

std::ostream& operator<<(std::ostream& o, const offset_fetch_request& r) {
    return ss::fmt_print(
      o, "group={} topics={}", r.data.group_id, r.data.topics);
}

static std::ostream&
operator<<(std::ostream& os, const offset_fetch_response_partition& p) {
    fmt::print(
      os,
      "id {} offset {} metadata {} error {}",
      p.partition_index,
      p.committed_offset,
      p.metadata,
      p.error_code);
    return os;
}

static std::ostream&
operator<<(std::ostream& os, const offset_fetch_response_topic& t) {
    fmt::print(os, "name {} partitions {}", t.name, t.partitions);
    return os;
}

std::ostream& operator<<(std::ostream& os, const offset_fetch_response& r) {
    fmt::print(os, "topics {}", r.data.topics);
    return os;
}

struct offset_fetch_ctx {
    request_context rctx;
    offset_fetch_request request;
    ss::smp_service_group ssg;

    offset_fetch_ctx(
      request_context&& rctx,
      offset_fetch_request&& request,
      ss::smp_service_group ssg)
      : rctx(std::move(rctx))
      , request(std::move(request))
      , ssg(ssg) {}
};

ss::future<response_ptr>
offset_fetch_api::process(request_context&& ctx, ss::smp_service_group ssg) {
    offset_fetch_request request;
    request.decode(ctx.reader(), ctx.header().version);
    klog.trace("Handling request {}", request);
    return ss::do_with(
      offset_fetch_ctx(std::move(ctx), std::move(request), ssg),
      [](offset_fetch_ctx& octx) {
          return octx.rctx.groups()
            .offset_fetch(std::move(octx.request))
            .then([&octx](offset_fetch_response resp) {
                return octx.rctx.respond(std::move(resp));
            });
      });
}

} // namespace kafka
