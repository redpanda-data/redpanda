/*
 * Copyright 2021 Redpanda Data, Inc.
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
#include "cluster/fwd.h"
#include "model/record.h"
#include "security/fwd.h"

#include <seastar/core/sharded.hh>

namespace cluster {

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

    ss::future<> fill_snapshot(controller_snapshot&) const;
    ss::future<> apply_snapshot(model::offset, const controller_snapshot&);

private:
    template<typename Cmd, typename Service>
    ss::future<std::error_code>
    dispatch_updates_to_cores(Cmd, ss::sharded<Service>&);

    ss::sharded<security::credential_store>& _credentials;
    ss::sharded<security::authorizer>& _authorizer;
};

} // namespace cluster
