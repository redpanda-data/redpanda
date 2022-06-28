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

#include "cloud_roles/apply_aws_credentials.h"
#include "cloud_roles/apply_gcp_credentials.h"

cloud_roles::apply_credentials
cloud_roles::make_credentials_applier(cloud_roles::credentials creds) {
    return apply_credentials{ss::visit(
      std::move(creds),
      [](cloud_roles::aws_credentials ac)
        -> std::unique_ptr<apply_credentials::impl> {
          return std::make_unique<cloud_roles::apply_aws_credentials>(
            std::move(ac));
      },
      [](cloud_roles::gcp_credentials gc)
        -> std::unique_ptr<apply_credentials::impl> {
          return std::make_unique<cloud_roles::apply_gcp_credentials>(
            std::move(gc));
      })};
}

std::ostream& cloud_roles::operator<<(
  std::ostream& os, const cloud_roles::apply_credentials& ac) {
    return ac.print(os);
}
