// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/internal_secret_service.h"

#include "cluster/internal_secret_frontend.h"

namespace cluster {

ss::future<fetch_internal_secret_reply> internal_secret_service::fetch(
  fetch_internal_secret_request&& req, rpc::streaming_context&) {
    return _fe.local().fetch(std::move(req.key), req.timeout);
}

} // namespace cluster
