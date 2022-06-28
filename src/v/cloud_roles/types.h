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
#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <boost/beast/http/status.hpp>

#include <unordered_set>

namespace cloud_roles {

using oauth_token_str = named_type<ss::sstring, struct oauth_token_str_tag>;

struct gcp_credentials {
    oauth_token_str oauth_token;
};

std::ostream& operator<<(std::ostream& os, const gcp_credentials& gc);

using aws_region_name = named_type<ss::sstring, struct s3_aws_region_name>;
using public_key_str = named_type<ss::sstring, struct s3_public_key_str>;
using private_key_str = named_type<ss::sstring, struct s3_private_key_str>;
using timestamp = std::chrono::time_point<std::chrono::system_clock>;
using s3_session_token = named_type<ss::sstring, struct s3_session_token_str>;

struct aws_credentials {
    public_key_str access_key_id;
    private_key_str secret_access_key;
    std::optional<s3_session_token> session_token;
    aws_region_name region;
};

std::ostream& operator<<(std::ostream& os, const aws_credentials& ac);

using credentials = std::variant<aws_credentials, gcp_credentials>;

std::ostream& operator<<(std::ostream& os, const credentials& c);

} // namespace cloud_roles
