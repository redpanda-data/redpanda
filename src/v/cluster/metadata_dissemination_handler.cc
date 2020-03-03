#include "cluster/metadata_dissemination_handler.h"

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/metadata_dissemination_types.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "rpc/connection_cache.h"

namespace cluster {
metadata_dissemination_handler::metadata_dissemination_handler(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<metadata_cache>& mc)
  : metadata_dissemination_rpc_service(sg, ssg)
  , _md_cache(mc) {}

ss::future<update_leadership_reply>
metadata_dissemination_handler::update_leadership(
  update_leadership_request&& req, rpc::streaming_context& sc) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return do_update_leadership(std::move(req));
      });
}

ss::future<update_leadership_reply>
metadata_dissemination_handler::do_update_leadership(
  update_leadership_request&& req) {
    return _md_cache
      .invoke_on_all([req = std::move(req)](metadata_cache& cache) mutable {
          for (auto& leader : req.leaders) {
              cache.update_partition_leader(
                leader.ntp.tp.topic,
                leader.ntp.tp.partition,
                leader.term,
                leader.leader_id);
          }
      })
      .then([] { return ss::make_ready_future<update_leadership_reply>(); });
}

static get_leadership_reply
make_get_leadership_reply(const metadata_cache& cache) {
    auto all_md = cache.all_metadata();
    ntp_leaders leaders;
    for (auto& [tp, md] : all_md) {
        for (auto& p : md.partitions) {
            leaders.emplace_back(ntp_leader{
              model::ntp{.ns = model::ns("default"),
                         .tp = model::topic_partition{.topic = tp,
                                                      .partition = p.p_md.id}},
              .term = p.term_id,
              .leader_id = p.p_md.leader_node});
        }
    }
    return get_leadership_reply{std::move(leaders)};
}

ss::future<get_leadership_reply> metadata_dissemination_handler::get_leadership(
  get_leadership_request&& req, rpc::streaming_context& sc) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return ss::make_ready_future<get_leadership_reply>(
            make_get_leadership_reply(_md_cache.local()));
      });
}

} // namespace cluster