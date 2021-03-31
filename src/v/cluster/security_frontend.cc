/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/security_frontend.h"

#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/partition_allocator.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/types.h"
#include "model/errc.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/validation.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "random/generators.h"
#include "rpc/errc.h"
#include "rpc/types.h"

#include <seastar/core/coroutine.hh>

#include <regex>

namespace cluster {

security_frontend::security_frontend(
  ss::sharded<controller_stm>& s, ss::sharded<ss::abort_source>& as)
  : _stm(s)
  , _as(as) {}

ss::future<std::error_code> security_frontend::create_user(
  security::credential_user username,
  security::scram_credential credential,
  model::timeout_clock::time_point tout) {
    create_user_cmd cmd(std::move(username), std::move(credential));
    return replicate_and_wait(std::move(cmd), tout);
}

ss::future<std::error_code> security_frontend::delete_user(
  security::credential_user username, model::timeout_clock::time_point tout) {
    delete_user_cmd cmd(std::move(username), 0 /* unused */);
    return replicate_and_wait(std::move(cmd), tout);
}

template<typename Cmd>
ss::future<std::error_code> security_frontend::replicate_and_wait(
  Cmd&& cmd, model::timeout_clock::time_point timeout) {
    return _stm.invoke_on(
      controller_stm_shard,
      [cmd = std::forward<Cmd>(cmd), &as = _as, timeout](
        controller_stm& stm) mutable {
          return serialize_cmd(std::forward<Cmd>(cmd))
            .then([&stm, timeout, &as](model::record_batch b) {
                return stm.replicate_and_wait(
                  std::move(b), timeout, as.local());
            });
      });
}

} // namespace cluster
