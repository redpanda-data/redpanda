#include "cluster/metadata_dissemination_service.h"

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/metadata_dissemination_rpc_service.h"
#include "cluster/metadata_dissemination_types.h"
#include "cluster/metadata_dissemination_utils.h"
#include "config/configuration.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "rpc/connection_cache.h"
#include "rpc/types.h"
#include "vlog.h"

namespace cluster {
metadata_dissemination_service::metadata_dissemination_service(
  ss::sharded<metadata_cache>& mc, ss::sharded<rpc::connection_cache>& clients)
  : _md_cache(mc)
  , _clients(clients)
  , _self(config::shard_local_cfg().node_id)
  , _dissemination_interval(
      config::shard_local_cfg().metadata_dissemination_interval) {
    _dispatch_timer.set_callback([this] {
        return ss::with_gate(
          _bg, [this] { return dispatch_disseminate_leadership(); });
    });
    _dispatch_timer.arm_periodic(_dissemination_interval);
}

void metadata_dissemination_service::disseminate_leadership(
  model::ntp ntp,
  model::term_id term,
  std::optional<model::node_id> leader_id) {
    vlog(
      clusterlog.trace,
      "Dissemination request for {}, leader {}",
      ntp,
      leader_id.value());

    _requests.push_back(ntp_leader{std::move(ntp), term, leader_id});
}

ss::future<> metadata_dissemination_service::initialize_leadership_metadata() {
    auto ids = _md_cache.local().all_broker_ids();
    auto it = std::find_if(
      std::cbegin(ids), std::cend(ids), [this](model::node_id id) {
          return id != _self;
      });

    if (it == ids.end()) {
        return ss::make_ready_future<>();
    }
    vlog(clusterlog.debug, "Initializing metadata using broker {}", *it);
    return dispatch_rpc<metadata_dissemination_rpc_client_protocol>(
      _clients, *it, [this](metadata_dissemination_rpc_client_protocol& c) {
          return c
            .get_leadership(
              get_leadership_request{},
              rpc::clock_type::now() + _dissemination_interval)
            .then([this](rpc::client_context<get_leadership_reply> reply) {
                return _md_cache.invoke_on_all(
                  [reply = std::move(reply.data)](
                    metadata_cache& cache) mutable {
                      for (auto& l : reply.leaders) {
                          cache.update_partition_leader(
                            l.ntp.tp.topic,
                            l.ntp.tp.partition,
                            l.term,
                            l.leader_id);
                      }
                  });
            });
      });
}

void metadata_dissemination_service::collect_pending_updates() {
    auto brokers = _md_cache.local().all_broker_ids();
    for (auto& ntp_leader : _requests) {
        auto tp_md = _md_cache.local().get_topic_metadata(
          ntp_leader.ntp.tp.topic);
        if (!tp_md) {
            // Topic metadata is not there anymore
            throw std::runtime_error(fmt::format(
              "Topic {} metadata does not exists", ntp_leader.ntp.tp.topic));
        }
        auto non_overlapping = calculate_non_overlapping_nodes(
          get_partition_members(ntp_leader.ntp.tp.partition, *tp_md), brokers);
        for (auto& id : non_overlapping) {
            if (!_pending_updates.contains(id)) {
                _pending_updates.emplace(id, ntp_leaders{});
            }
            _pending_updates[id].push_back(ntp_leader);
        }
    }
    _requests.clear();
}

ss::future<> metadata_dissemination_service::dispatch_disseminate_leadership() {
    collect_pending_updates();
    return ss::do_for_each(
      _pending_updates.begin(),
      _pending_updates.end(),
      [this](broker_updates_t::value_type& br_update) {
          return dispatch_one_update(br_update.first, br_update.second);
      });
}

ss::future<> metadata_dissemination_service::dispatch_one_update(
  model::node_id target_id, const ntp_leaders& leaders) {
    return cluster::dispatch_rpc<metadata_dissemination_rpc_client_protocol>(
             _clients,
             target_id,
             [this, &leaders, target_id](
               metadata_dissemination_rpc_client_protocol& proto) mutable {
                 vlog(
                   clusterlog.trace,
                   "Sending {} metadata updates to {}",
                   leaders.size(),
                   target_id);
                 return proto
                   .update_leadership(
                     update_leadership_request{leaders},
                     _dissemination_interval + rpc::clock_type::now())
                   .then([](rpc::client_context<update_leadership_reply> ctx) {
                       return std::move(ctx.data);
                   });
             })
      .then([this, target_id](update_leadership_reply) {
          _pending_updates.erase(target_id);
      })
      .handle_exception([](std::exception_ptr e) {
          vlog(clusterlog.warn, "Error when sending metadata update {}", e);
      });
}

ss::future<> metadata_dissemination_service::stop() {
    _dispatch_timer.cancel();
    return _bg.close();
}

} // namespace cluster