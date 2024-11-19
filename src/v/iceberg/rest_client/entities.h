/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "container/fragmented_vector.h"
#include "http/rest_client/rest_entity.h"

namespace iceberg::rest_client {

struct table : public http::rest_client::rest_entity {
    table(
      std::string_view root_url,
      const chunked_vector<ss::sstring>& namespace_parts);

    ss::sstring resource_name() const final;

private:
    ss::sstring _namespace;
};

struct namespaces : public http::rest_client::rest_entity {
    explicit namespaces(std::string_view root_url);

    ss::sstring resource_name() const final;
};

} // namespace iceberg::rest_client
