// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/ephemeral_credential_rpc_service.h"
#include "cluster/fwd.h"

namespace cluster {

class ephemeral_credential_service final
  : public impl::ephemeral_credential_service {
public:
    ephemeral_credential_service(
      ss::scheduling_group sc,
      ss::smp_service_group ssg,
      ss::sharded<cluster::ephemeral_credential_frontend>& fe)
      : impl::ephemeral_credential_service{sc, ssg}
      , _fe(fe) {}

    ss::future<put_ephemeral_credential_reply> put_ephemeral_credential(
      put_ephemeral_credential_request&&, rpc::streaming_context&) override;

private:
    ss::sharded<ephemeral_credential_frontend>& _fe;
};

} // namespace cluster
