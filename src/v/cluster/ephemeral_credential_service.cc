// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/ephemeral_credential_service.h"

#include "cluster/ephemeral_credential_frontend.h"

namespace cluster {

ss::future<put_ephemeral_credential_reply>
ephemeral_credential_service::put_ephemeral_credential(
  put_ephemeral_credential_request r, rpc::streaming_context&) {
    co_await _fe.local().put(r.principal, r.user, r.credential);
    co_return errc::success;
}

} // namespace cluster
