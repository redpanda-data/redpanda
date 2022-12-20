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
#include "cloud_roles/signature.h"

namespace cloud_roles {

class apply_abs_credentials final : public apply_credentials::impl {
public:
    explicit apply_abs_credentials(abs_credentials credentials);

    std::error_code
    add_auth(http::client::request_header& header) const override;

    void reset_creds(credentials creds) override;
    std::ostream& print(std::ostream& os) const override;

private:
    signature_abs _signature;
};

} // namespace cloud_roles
