/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_roles/apply_credentials.h"

#include "apply_abs_credentials.h"
#include "apply_abs_oauth_credentials.h"
#include "apply_aws_credentials.h"
#include "apply_gcp_credentials.h"

namespace cloud_roles {
apply_credentials make_credentials_applier(credentials creds) {
    return apply_credentials{ss::visit(
      std::move(creds),
      [](aws_credentials ac) -> std::unique_ptr<apply_credentials::impl> {
          return std::make_unique<apply_aws_credentials>(std::move(ac));
      },
      [](gcp_credentials gc) -> std::unique_ptr<apply_credentials::impl> {
          return std::make_unique<apply_gcp_credentials>(std::move(gc));
      },
      [](abs_credentials abs) -> std::unique_ptr<apply_credentials::impl> {
          return std::make_unique<apply_abs_credentials>(std::move(abs));
      },
      [](const abs_oauth_credentials& abs_oauth)
        -> std::unique_ptr<apply_credentials::impl> {
          return std::make_unique<apply_abs_oauth_credentials>(abs_oauth);
      })};
}

std::ostream& operator<<(std::ostream& os, const apply_credentials& ac) {
    return ac.print(os);
}
} // namespace cloud_roles
