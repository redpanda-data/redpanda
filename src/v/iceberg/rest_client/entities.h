/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "http/rest_client/rest_entity.h"

namespace iceberg::rest_client {

struct name_space : public http::rest_client::rest_entity {
    explicit name_space(std::string_view base_url);

    inline ss::sstring resource_name() const override { return "namespaces"; }
};

} // namespace iceberg::rest_client
