/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/rest_client/entities.h"

#include "ssx/sformat.h"

namespace iceberg::rest_client {

table::table(
  std::string_view root_url, const chunked_vector<ss::sstring>& namespace_parts)
  : rest_entity(root_url)
  , _namespace{ssx::sformat("{}", fmt::join(namespace_parts, "\x1f"))} {}

ss::sstring table::resource_name() const {
    return fmt::format("namespaces/{}/tables", _namespace);
}

namespaces::namespaces(std::string_view root_url)
  : rest_entity(root_url) {}

ss::sstring namespaces::resource_name() const { return "namespaces"; }

} // namespace iceberg::rest_client
