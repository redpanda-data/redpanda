/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_roles/types.h"

namespace cloud_roles {

std::ostream& operator<<(std::ostream& os, const gcp_credentials& gc) {
    fmt::print(
      os, "gcp_credentials{{oauth_token:**{}**}}", gc.oauth_token().size());
    return os;
}

std::ostream& operator<<(std::ostream& os, const aws_credentials& ac) {
    fmt::print(
      os,
      "aws_credentials{{access_key_id: **{}**, secret_access_key: **{}**, "
      "session_token: **{}**}}",
      ac.access_key_id().size(),
      ac.secret_access_key().size(),
      ac.session_token.value_or(s3_session_token{})().size());
    return os;
}

std::ostream& operator<<(std::ostream& os, const credentials& c) {
    ss::visit(c, [&os](auto creds) { os << creds; });
    return os;
}

} // namespace cloud_roles
