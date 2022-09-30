/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/fwd.h"
#include "cluster/internal_secret_service_impl.h"

namespace cluster {

/// internal_secret_service dispatches internal secret RPCs to the
/// internal_secret_frontend.
class internal_secret_service final : public impl::internal_secret_service {
public:
    internal_secret_service(
      ss::scheduling_group sc,
      ss::smp_service_group ssg,
      ss::sharded<cluster::internal_secret_frontend>& fe)
      : impl::internal_secret_service{sc, ssg}
      , _fe(fe) {}

    ss::future<fetch_internal_secret_reply>
    fetch(fetch_internal_secret_request&&, rpc::streaming_context&) override;

private:
    ss::sharded<cluster::internal_secret_frontend>& _fe;
};

} // namespace cluster
