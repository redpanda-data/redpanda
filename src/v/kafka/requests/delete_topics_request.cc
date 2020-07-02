#include "kafka/requests/delete_topics_request.h"

#include "kafka/errors.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

struct delete_topics_ctx {
    request_context rctx;
    delete_topics_request request;
    ss::smp_service_group ssg;

    delete_topics_ctx(
      request_context&& rctx,
      delete_topics_request&& request,
      ss::smp_service_group ssg)
      : rctx(std::move(rctx))
      , request(std::move(request))
      , ssg(ssg) {}
};

std::vector<model::topic_namespace>
create_topic_namespaces(std::vector<model::topic> topic_names) {
    std::vector<model::topic_namespace> ret;
    ret.reserve(topic_names.size());
    std::transform(
      std::begin(topic_names),
      std::end(topic_names),
      std::back_inserter(ret),
      [](model::topic& tp) {
          return model::topic_namespace(
            cluster::kafka_namespace, std::move(tp));
      });
    return ret;
}

delete_topics_response create_response(std::vector<cluster::topic_result> res) {
    delete_topics_response resp;
    resp.data.responses.reserve(res.size());
    std::transform(
      res.begin(),
      res.end(),
      std::back_inserter(resp.data.responses),
      [](cluster::topic_result tr) {
          return deletable_topic_result{
            .name = std::move(tr.tp_ns.tp),
            .error_code = map_topic_error_code(tr.ec)};
      });
    return resp;
}

ss::future<response_ptr>
delete_topics_api::process(request_context&& ctx, ss::smp_service_group ssg) {
    delete_topics_request request;
    request.decode(ctx.reader(), ctx.header().version);
    vlog(klog.trace, "Handling request {}", request);

    return ss::do_with(
      delete_topics_ctx(std::move(ctx), std::move(request), ssg),
      [](delete_topics_ctx& octx) {
          auto req = std::move(octx.request.data);
          auto tout = req.timeout_ms + model::timeout_clock::now();
          return octx.rctx.topics_frontend()
            .delete_topics(
              create_topic_namespaces(std::move(req.topic_names)), tout)
            .then([&octx](std::vector<cluster::topic_result> res) {
                auto resp = create_response(std::move(res));
                resp.data.throttle_time_ms = std::chrono::milliseconds(
                  octx.rctx.throttle_delay_ms());
                return octx.rctx.respond(std::move(resp));
            });
      });
}

} // namespace kafka
