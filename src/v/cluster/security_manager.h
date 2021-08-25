/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/commands.h"
#include "model/record.h"
#include "security/authorizer.h"
#include "security/credential_store.h"

#include <seastar/core/sharded.hh>

namespace cluster {

/*
 * handle: delete acls command
 */
std::error_code do_apply(delete_acls_cmd cmd, security::authorizer& authorizer);

/*
 * handle: create acls command
 */
std::error_code do_apply(create_acls_cmd cmd, security::authorizer& authorizer);

/*
 * handle: update user command
 */
std::error_code
do_apply(update_user_cmd cmd, security::credential_store& store);

/*
 * handle: delete user command
 */
std::error_code
do_apply(delete_user_cmd cmd, security::credential_store& store);

/*
 * handle: create user command
 */
std::error_code
do_apply(create_user_cmd cmd, security::credential_store& store);

class security_manager final {
public:
    explicit security_manager(
      ss::sharded<security::credential_store>&,
      ss::sharded<security::authorizer>&);

    static constexpr auto commands = make_commands_list<
      create_user_cmd,
      delete_user_cmd,
      update_user_cmd,
      create_acls_cmd,
      delete_acls_cmd>();

    ss::future<std::error_code> apply_update(model::record_batch);

    bool is_batch_applicable(const model::record_batch& batch) const {
        return batch.header().type
                 == model::record_batch_type::user_management_cmd
               || batch.header().type
                    == model::record_batch_type::acl_management_cmd;
    }

    template<typename Cmd, typename Service>
    static ss::future<std::error_code>
    apply(ss::shard_id shard, Cmd cmd, ss::sharded<Service>& service) {
        return service.invoke_on(
          shard, [cmd = std::move(cmd)](auto& local_service) mutable {
              return do_apply(std::move(cmd), local_service);
          });
    }

private:
    ss::sharded<security::credential_store>& _credentials;
    ss::sharded<security::authorizer>& _authorizer;
};

} // namespace cluster
