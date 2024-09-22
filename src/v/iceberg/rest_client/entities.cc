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

#include "iceberg/rest_client/entities.h"

#include "iceberg/rest_client/types.h"

namespace iceberg::rest_client {

name_space::name_space(std::string_view base_url)
  : rest_entity{base_url} {}

} // namespace iceberg::rest_client
