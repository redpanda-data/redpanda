/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "migrations/rbac_migrator.h"

#include "base/vlog.h"
#include "cluster/controller.h"
#include "cluster/security_frontend.h"
#include "features/logger.h"
#include "security/credential_store.h"
#include "security/role.h"
#include "security/types.h"

using namespace std::chrono_literals;

namespace features::migrators {

ss::future<> rbac_migrator::do_mutate() {
    auto to_role_member = [](const auto& cred) -> security::role_member {
        return {security::role_member_type::user, cred.first};
    };
    auto users_view = _controller.get_credential_store().local().range(
                        security::credential_store::is_not_ephemeral)
                      | std::ranges::views::transform(to_role_member);

    security::role role{{users_view.begin(), users_view.end()}};
    security::role_name role_name{security::default_role_name};

    vlog(
      featureslog.info,
      "Creating default role '{}' with {} users...",
      security::default_role_name,
      role.members().size());

    auto err = co_await _controller.get_security_frontend().local().create_role(
      role_name, std::move(role), model::timeout_clock::now() + 5s);

    if (err == cluster::errc::role_exists) {
        // If the leader running the feature migration loses leadership after
        // the role is created but before the feature migration is successfully
        // completed, the next leader will redo the feature migration. In that
        // case, we will get the role_exists error here, which we can safely
        // ignore.
        vlog(
          featureslog.info,
          "Default role '{}' already exists...",
          security::default_role_name);
    } else if (err) {
        vlog(
          featureslog.error,
          "Error while creating default role '{}': {}",
          security::default_role_name,
          err);
    } else {
        vlog(
          featureslog.info,
          "Created default role '{}'",
          security::default_role_name);
    }
}
} // namespace features::migrators
