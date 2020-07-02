#include "cluster/service.h"

#include "cluster/members_manager.h"
#include "cluster/metadata_cache.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"

#include <seastar/core/sharded.hh>

namespace cluster {
service::service(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<topics_frontend>& tf,
  ss::sharded<members_manager>& mm,
  ss::sharded<metadata_cache>& cache)
  : controller_service(sg, ssg)
  , _topics_frontend(tf)
  , _members_manager(mm)
  , _md_cache(cache) {}

ss::future<join_reply>
service::join(join_request&& req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, broker = std::move(req.node)]() mutable {
          return _members_manager
            .invoke_on(
              members_manager::shard,
              get_smp_service_group(),
              [broker = std::move(broker)](members_manager& mm) mutable {
                  return mm.handle_join_request(std::move(broker));
              })
            .then([](result<join_reply> r) {
                if (!r) {
                    return join_reply{false};
                }
                return r.value();
            });
      });
}

ss::future<create_topics_reply>
service::create_topics(create_topics_request&& r, rpc::streaming_context&) {
    return ss::with_scheduling_group(
             get_scheduling_group(),
             [this, r = std::move(r)]() mutable {
                 return _topics_frontend.local().create_topics(
                   std::move(r.topics),
                   model::timeout_clock::now() + r.timeout);
             })
      .then([this](std::vector<topic_result> res) {
          // Fetch metadata for successfully created topics
          auto [md, cfg] = fetch_metadata_and_cfg(res);
          return create_topics_reply{
            std::move(res), std::move(md), std::move(cfg)};
      });
}

std::pair<std::vector<model::topic_metadata>, std::vector<topic_configuration>>
service::fetch_metadata_and_cfg(const std::vector<topic_result>& res) {
    std::vector<model::topic_metadata> md;
    std::vector<topic_configuration> cfg;
    md.reserve(res.size());
    for (const auto& r : res) {
        if (r.ec == errc::success) {
            auto topic_md = _md_cache.local().get_topic_metadata(r.tp_ns);
            auto topic_cfg = _md_cache.local().get_topic_cfg(r.tp_ns);
            if (topic_md && topic_cfg) {
                md.push_back(std::move(topic_md.value()));
                cfg.push_back(std::move(topic_cfg.value()));
            }
        }
    }
    return {std::move(md), std::move(cfg)};
}

} // namespace cluster
