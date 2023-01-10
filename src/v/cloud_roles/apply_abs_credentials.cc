/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_roles/apply_abs_credentials.h"

namespace cloud_roles {

apply_abs_credentials::apply_abs_credentials(abs_credentials credentials)
  : _signature{credentials.storage_account, credentials.shared_key} {}

std::error_code
apply_abs_credentials::add_auth(http::client::request_header& header) const {
    return _signature.sign_header(header);
}

void apply_abs_credentials::reset_creds(credentials creds) {
    if (!std::holds_alternative<abs_credentials>(creds)) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "credential applier reset with incorrect credential type {}",
          creds));
    }
    auto abs_creds = std::get<abs_credentials>(creds);
    _signature = signature_abs{abs_creds.storage_account, abs_creds.shared_key};
};

std::ostream& apply_abs_credentials::print(std::ostream& os) const {
    fmt::print(os, "apply_abs_credentials");
    return os;
}

} // namespace cloud_roles
