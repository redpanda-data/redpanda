// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/service.h"

#include "cluster/members_manager.h"
#include "cluster/metadata_cache.h"
#include "cluster/security_frontend.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/timeout_clock.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace cluster {
service::service(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<topics_frontend>& tf,
  ss::sharded<members_manager>& mm,
  ss::sharded<metadata_cache>& cache,
  ss::sharded<security_frontend>& sf)
  : controller_service(sg, ssg)
  , _topics_frontend(tf)
  , _members_manager(mm)
  , _md_cache(cache)
  , _security_frontend(sf) {}

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

ss::future<configuration_update_reply> service::update_node_configuration(
  configuration_update_request&& req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return _members_manager
            .invoke_on(
              members_manager::shard,
              get_smp_service_group(),
              [req = std::move(req)](members_manager& mm) mutable {
                  return mm.handle_configuration_update_request(std::move(req));
              })
            .then([](result<configuration_update_reply> r) {
                if (!r) {
                    return configuration_update_reply{false};
                }
                return r.value();
            });
      });
}

ss::future<finish_partition_update_reply> service::finish_partition_update(
  finish_partition_update_request&& req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return do_finish_partition_update(std::move(req));
      });
}

ss::future<finish_partition_update_reply>
service::do_finish_partition_update(finish_partition_update_request&& req) {
    auto ec
      = co_await _topics_frontend.local().finish_moving_partition_replicas(
        req.ntp,
        req.new_replica_set,
        config::shard_local_cfg().replicate_append_timeout_ms()
          + model::timeout_clock::now());

    errc e = ec ? errc::not_leader : errc::success;

    co_return finish_partition_update_reply{.result = e};
}

ss::future<update_topic_properties_reply> service::update_topic_properties(
  update_topic_properties_request&& req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return do_update_topic_properties(std::move(req));
      });
}

ss::future<update_topic_properties_reply>
service::do_update_topic_properties(update_topic_properties_request&& req) {
    // local topic frontend instance will eventually dispatch request to _raft0
    // core
    auto res = co_await _topics_frontend.local().update_topic_properties(
      req.updates,
      config::shard_local_cfg().replicate_append_timeout_ms()
        + model::timeout_clock::now());

    co_return update_topic_properties_reply{.results = std::move(res)};
}

ss::future<create_acls_reply>
service::create_acls(create_acls_request&& request, rpc::streaming_context&) {
    return ss::with_scheduling_group(
             get_scheduling_group(),
             [this, r = std::move(request)]() mutable {
                 return _security_frontend.local().create_acls(
                   std::move(r.data.bindings), r.timeout);
             })
      .then([](std::vector<errc> results) {
          return create_acls_reply{.results = std::move(results)};
      });
}

} // namespace cluster
