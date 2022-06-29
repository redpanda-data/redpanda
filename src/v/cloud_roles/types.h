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

#include "bytes/iobuf.h"
#include "s3/signature.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <boost/beast/http/status.hpp>

#include <unordered_set>

namespace cloud_roles {

using s3_session_token = named_type<ss::sstring, struct s3_session_token_str>;

struct aws_credentials {
    s3::public_key_str access_key_id;
    s3::private_key_str secret_access_key;
    std::optional<s3_session_token> session_token;
    s3::aws_region_name region;
};

std::ostream& operator<<(std::ostream& os, const aws_credentials& ac);

using credentials = std::variant<aws_credentials>;

std::ostream& operator<<(std::ostream& os, const credentials& c);

} // namespace cloud_roles
