/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/bytes.h"
#include "storage/kvstore.h"

#include <seastar/core/sharded.hh>

namespace cluster {

constexpr const char* cluster_uuid_key = "cluster_uuid";
constexpr storage::kvstore::key_space cluster_uuid_key_space
  = storage::kvstore::key_space::controller;

} // namespace cluster
