#include "cluster/service.h"

#include "cluster/controller.h"
#include "cluster/metadata_cache.h"
#include "cluster/types.h"

namespace cluster {
service::service(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<controller>& c,
  ss::sharded<metadata_cache>& cache)
  : controller_service(sg, ssg)
  , _controller(c)
  , _md_cache(cache) {}

ss::future<join_reply>
service::join(join_request&& req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, broker = std::move(req.node)]() mutable {
          return _controller
            .invoke_on(
              controller::shard,
              get_smp_service_group(),
              [this, broker = std::move(broker)](controller& c) mutable {
                  return c.process_join_request(std::move(broker));
              })
            .then([] { return join_reply{true}; });
      });
}

ss::future<create_topics_reply>
service::create_topics(create_topics_request&& r, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, r = std::move(r)]() mutable {
          return _controller
            .invoke_on(
              controller::shard,
              get_smp_service_group(),
              [this, r = std::move(r)](controller& c) mutable {
                  return c.create_topics(
                    std::move(r.topics),
                    model::timeout_clock::now() + r.timeout);
              })
            .then([this](std::vector<topic_result> res) {
                // Fetch metadata from succesfully created topics
                auto md = fetch_metadata(res);
                return create_topics_reply{std::move(res), std::move(md)};
            });
      });
}

std::vector<model::topic_metadata>
service::fetch_metadata(const std::vector<topic_result>& res) {
    std::vector<model::topic_metadata> md;
    md.reserve(res.size());
    for (const auto& r : res) {
        if (r.ec == errc::success) {
            auto topic_md = _md_cache.local().get_topic_metadata(r.topic);
            if (topic_md) {
                md.push_back(std::move(topic_md.value()));
            }
        }
    }
    return md;
}

} // namespace cluster
