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
#include "utils/retry.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>

#include <chrono>
#include <exception>
namespace cluster {
metadata_dissemination_service::metadata_dissemination_service(
  ss::sharded<metadata_cache>& mc, ss::sharded<rpc::connection_cache>& clients)
  : _md_cache(mc)
  , _clients(clients)
  , _self(config::shard_local_cfg().node_id)
  , _dissemination_interval(
      config::shard_local_cfg().metadata_dissemination_interval) {
    _dispatch_timer.set_callback([this] {
        (void)ss::with_gate(
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

void metadata_dissemination_service::initialize_leadership_metadata() {
    auto ids = _md_cache.local().all_broker_ids();
    // Do nothing, single node case
    if (ids.size() <= 1) {
        return;
    }
    (void)ss::with_gate(_bg, [this, ids = std::move(ids)]() mutable {
        // We do not want to send requst to self
        auto it = std::find(std::cbegin(ids), std::cend(ids), _self);
        if (it != ids.end()) {
            ids.erase(it);
        }

        return update_metadata_with_retries(std::move(ids));
    });
}

static inline ss::future<>
wait_for_next_retry(std::chrono::seconds sleep_for, ss::abort_source& as) {
    return ss::sleep_abortable(sleep_for, as)
      .handle_exception_type([](const ss::sleep_aborted&) {
          vlog(clusterlog.debug, "Getting metadata cancelled");
      });
}

ss::future<> metadata_dissemination_service::update_metadata_with_retries(
  std::vector<model::node_id> ids) {
    return ss::do_with(
      request_retry_meta{.ids = std::move(ids)},
      [this](request_retry_meta& meta) {
          meta.next = std::cbegin(meta.ids);
          return ss::do_until(
            [this, &meta] { return meta.success || _bg.is_closed(); },
            [this, &meta]() mutable {
                return do_request_metadata_update(meta);
            });
      });
}

ss::future<> metadata_dissemination_service::do_request_metadata_update(
  request_retry_meta& meta) {
    return dispatch_get_metadata_update(*meta.next)
      .then([this, &meta](result<get_leadership_reply> r) {
          return process_get_update_reply(std::move(r), meta);
      })
      .handle_exception([](const std::exception_ptr& e) {
          vlog(clusterlog.debug, "Metadata update error: {}", e);
      })
      .then([&meta, this] {
          // Success case
          if (meta.success) {
              return ss::make_ready_future<>();
          }
          // Dispatch next retry
          ++meta.next;
          if (meta.next != meta.ids.end()) {
              return ss::make_ready_future<>();
          }
          // start from the beggining, after backoff elapsed
          meta.next = std::cbegin(meta.ids);
          return wait_for_next_retry(
            std::chrono::seconds(meta.backoff_policy.next_backoff()), _as);
      });
}

ss::future<> metadata_dissemination_service::process_get_update_reply(
  result<get_leadership_reply> reply_result, request_retry_meta& meta) {
    if (!reply_result) {
        vlog(
          clusterlog.debug,
          "Unable to initialize metadata using node {}",
          *meta.next);
        return ss::make_ready_future<>();
    }
    // Update all NTP leaders
    return _md_cache
      .invoke_on_all([reply = std::move(reply_result.value())](
                       metadata_cache& cache) mutable {
          for (auto& l : reply.leaders) {
              cache.update_partition_leader(l.ntp, l.term, l.leader_id);
          }
      })
      .then([&meta] { meta.success = true; });
}

ss::future<result<get_leadership_reply>>
metadata_dissemination_service::dispatch_get_metadata_update(
  model::node_id id) {
    vlog(clusterlog.debug, "Requesting metadata update from node {}", id);
    return _clients.local()
      .with_node_client<metadata_dissemination_rpc_client_protocol>(
        id, [this](metadata_dissemination_rpc_client_protocol c) {
            return c
              .get_leadership(
                get_leadership_request{},
                rpc::client_opts(
                  rpc::clock_type::now() + _dissemination_interval))
              .then([](rpc::client_context<get_leadership_reply> ctx) {
                  return std::move(ctx.data);
              });
        });
}

void metadata_dissemination_service::collect_pending_updates() {
    auto brokers = _md_cache.local().all_broker_ids();
    for (auto& ntp_leader : _requests) {
        auto tp_md = _md_cache.local().get_topic_metadata(
          model::topic_namespace_view(ntp_leader.ntp));

        if (!tp_md) {
            // Topic metadata is not there anymore, partition was removed
            clusterlog.debug(
              "Ignoring leadership dissemination for {}, metadata does not "
              "exists",
              ntp_leader.ntp);
            continue;
        }
        auto non_overlapping = calculate_non_overlapping_nodes(
          get_partition_members(ntp_leader.ntp.tp.partition, *tp_md), brokers);
        for (auto& id : non_overlapping) {
            if (!_pending_updates.contains(id)) {
                _pending_updates.emplace(id, update_retry_meta{ntp_leaders{}});
            }
            _pending_updates[id].updates.push_back(ntp_leader);
        }
    }
    _requests.clear();
}

void metadata_dissemination_service::cleanup_finished_updates() {
    std::vector<model::node_id> _to_remove;
    _to_remove.reserve(_pending_updates.size());
    for (auto& [node_id, meta] : _pending_updates) {
        if (meta.finished) {
            _to_remove.push_back(node_id);
        }
    }
    for (auto id : _to_remove) {
        _pending_updates.erase(id);
    }
}

ss::future<> metadata_dissemination_service::dispatch_disseminate_leadership() {
    collect_pending_updates();
    return ss::parallel_for_each(
             _pending_updates.begin(),
             _pending_updates.end(),
             [this](broker_updates_t::value_type& br_update) {
                 return dispatch_one_update(br_update.first, br_update.second);
             })
      .then([this] { cleanup_finished_updates(); });
}

ss::future<> metadata_dissemination_service::dispatch_one_update(
  model::node_id target_id, update_retry_meta& meta) {
    return _clients.local()
      .with_node_client<metadata_dissemination_rpc_client_protocol>(
        target_id,
        [this, &meta, target_id](
          metadata_dissemination_rpc_client_protocol proto) mutable {
            vlog(
              clusterlog.trace,
              "Sending {} metadata updates to {}",
              meta.updates.size(),
              target_id);
            return proto
              .update_leadership(
                update_leadership_request{meta.updates},
                rpc::client_opts(
                  _dissemination_interval + rpc::clock_type::now()))
              .then([](rpc::client_context<update_leadership_reply> ctx) {
                  return std::move(ctx.data);
              });
        })
      .then([target_id, &meta](result<update_leadership_reply> r) {
          if (r) {
              meta.finished = true;
              return;
          }
          vlog(
            clusterlog.warn,
            "Error sending metadata update {} to {}",
            r.error().message(),
            target_id);
      })
      .handle_exception([](std::exception_ptr e) {
          vlog(clusterlog.warn, "Error when sending metadata update {}", e);
      });
}

ss::future<> metadata_dissemination_service::stop() {
    _as.request_abort();
    _dispatch_timer.cancel();
    return _bg.close();
}

} // namespace cluster
