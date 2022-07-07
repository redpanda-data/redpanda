/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_roles/apply_credentials.h"

namespace cloud_roles {

class apply_gcp_credentials final : public apply_credentials::impl {
public:
    explicit apply_gcp_credentials(gcp_credentials credentials);

    std::error_code
    add_auth(http::client::request_header& header) const override;

    void reset_creds(credentials creds) override;
    std::ostream& print(std::ostream& os) const override;

private:
    oauth_token_str _oauth_token;
};

} // namespace cloud_roles
