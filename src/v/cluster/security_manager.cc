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
#include "cluster/security_manager.h"

#include "cluster/commands.h"
#include "model/metadata.h"
#include "raft/types.h"

#include <seastar/core/coroutine.hh>

#include <iterator>
#include <system_error>
#include <vector>

namespace cluster {

security_manager::security_manager(
  ss::sharded<security::credential_store>& credentials,
  ss::sharded<security::authorizer>& authorizer)
  : _credentials(credentials)
  , _authorizer(authorizer) {}

ss::future<std::error_code>
security_manager::apply_update(model::record_batch batch) {
    return deserialize(std::move(batch), commands).then([this](auto cmd) {
        return ss::visit(
          std::move(cmd),
          [this](create_user_cmd cmd) {
              return dispatch_updates_to_cores(std::move(cmd));
          },
          [this](delete_user_cmd cmd) {
              return dispatch_updates_to_cores(std::move(cmd));
          },
          [this](update_user_cmd cmd) {
              return dispatch_updates_to_cores(std::move(cmd));
          });
    });
}

/*
 * handle: update user command
 */
static std::error_code
do_apply(update_user_cmd cmd, security::credential_store& store) {
    auto removed = store.remove(cmd.key);
    if (!removed) {
        return errc::user_does_not_exist;
    }
    store.put(cmd.key, std::move(cmd.value));
    return errc::success;
}

/*
 * handle: delete user command
 */
static std::error_code
do_apply(delete_user_cmd cmd, security::credential_store& store) {
    auto removed = store.remove(cmd.key);
    return removed ? errc::success : errc::user_does_not_exist;
}

/*
 * handle: create user command
 */
static std::error_code
do_apply(create_user_cmd cmd, security::credential_store& store) {
    if (store.contains(cmd.key)) {
        return errc::user_exists;
    }
    store.put(cmd.key, std::move(cmd.value));
    return errc::success;
}

template<typename Cmd>
static ss::future<std::error_code> do_apply(
  ss::shard_id shard, Cmd cmd, ss::sharded<security::credential_store>& store) {
    return store.invoke_on(
      shard,
      [cmd = std::move(cmd)](security::credential_store& local_store) mutable {
          return do_apply(std::move(cmd), local_store);
      });
}

template<typename Cmd>
ss::future<std::error_code>
security_manager::dispatch_updates_to_cores(Cmd cmd) {
    using ret_t = std::vector<std::error_code>;
    return ss::do_with(
      ret_t{}, [this, cmd = std::move(cmd)](ret_t& ret) mutable {
          ret.reserve(ss::smp::count);
          return ss::parallel_for_each(
                   boost::irange(0, (int)ss::smp::count),
                   [this, &ret, cmd = std::move(cmd)](int shard) mutable {
                       return do_apply(shard, cmd, _credentials)
                         .then([&ret](std::error_code r) { ret.push_back(r); });
                   })
            .then([&ret] { return std::move(ret); })
            .then([](std::vector<std::error_code> results) mutable {
                auto ret = results.front();
                for (auto& r : results) {
                    vassert(
                      ret == r,
                      "State inconsistency across shards detected, "
                      "expected "
                      "result: {}, have: {}",
                      ret,
                      r);
                }
                return ret;
            });
      });
}

} // namespace cluster
