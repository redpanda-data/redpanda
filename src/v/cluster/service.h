#pragma once
#include "cluster/controller_service.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"

#include <seastar/core/sharded.hh>

#include <vector>

namespace cluster {
class members_manager;
class topics_frontend;
class metadata_cache;

class service : public controller_service {
public:
    service(
      ss::scheduling_group,
      ss::smp_service_group,
      ss::sharded<topics_frontend>&,
      ss::sharded<members_manager>&,
      ss::sharded<metadata_cache>&);

    virtual ss::future<join_reply>
    join(join_request&&, rpc::streaming_context&) override;

    virtual ss::future<create_topics_reply>
    create_topics(create_topics_request&&, rpc::streaming_context&) override;

private:
    std::
      pair<std::vector<model::topic_metadata>, std::vector<topic_configuration>>
      fetch_metadata_and_cfg(const std::vector<topic_result>&);

    ss::sharded<topics_frontend>& _topics_frontend;
    ss::sharded<members_manager>& _members_manager;
    ss::sharded<metadata_cache>& _md_cache;
};
} // namespace cluster
