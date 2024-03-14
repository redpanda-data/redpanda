/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cluster/cloud_metadata/offsets_lookup.h"
#include "cluster/cloud_metadata/offsets_lookup_rpc_types.h"
#include "cluster/cloud_metadata/offsets_recovery_router.h"
#include "cluster/cloud_metadata/offsets_recovery_rpc_types.h"
#include "cluster/cloud_metadata/offsets_upload_router.h"
#include "cluster/offsets_recovery_rpc_service.h"
#include "config/configuration.h"

namespace cluster::cloud_metadata {

class offsets_recovery_rpc_service : public offsets_recovery_service {
public:
    offsets_recovery_rpc_service(
      ss::scheduling_group sg,
      ss::smp_service_group ssg,
      ss::sharded<cluster::cloud_metadata::offsets_lookup>& ol,
      ss::sharded<cluster::cloud_metadata::offsets_recovery_router>& orr,
      ss::sharded<cluster::cloud_metadata::offsets_upload_router>& our)
      : offsets_recovery_service(sg, ssg)
      , _offsets_lookup(ol)
      , _offsets_recovery_router(orr)
      , _offsets_upload_router(our)
      , _metadata_timeout_ms(
          config::shard_local_cfg()
            .cloud_storage_cluster_metadata_upload_timeout_ms.bind()) {}

    ss::future<offsets_lookup_reply> offsets_lookup(
      offsets_lookup_request req, rpc::streaming_context&) override {
        co_return co_await _offsets_lookup.local().lookup(std::move(req));
    }

    ss::future<offsets_upload_reply> offsets_upload(
      offsets_upload_request req, rpc::streaming_context&) override {
        if (!_offsets_upload_router.local_is_initialized()) {
            co_return offsets_upload_reply{.ec = errc::feature_disabled};
        }

        auto ntp = req.offsets_ntp;
        co_return co_await _offsets_upload_router.local().process_or_dispatch(
          std::move(req), std::move(ntp), _metadata_timeout_ms());
    }

    ss::future<offsets_recovery_reply> offsets_recovery(
      offsets_recovery_request req, rpc::streaming_context&) override {
        if (!_offsets_recovery_router.local_is_initialized()) {
            co_return offsets_recovery_reply{.ec = errc::feature_disabled};
        }

        auto ntp = req.offsets_ntp;
        co_return co_await _offsets_recovery_router.local().process_or_dispatch(
          std::move(req), std::move(ntp), 30s);
    }

private:
    ss::sharded<cluster::cloud_metadata::offsets_lookup>& _offsets_lookup;
    ss::sharded<cluster::cloud_metadata::offsets_recovery_router>&
      _offsets_recovery_router;
    ss::sharded<cluster::cloud_metadata::offsets_upload_router>&
      _offsets_upload_router;

    config::binding<std::chrono::milliseconds> _metadata_timeout_ms;
};

} // namespace cluster::cloud_metadata
