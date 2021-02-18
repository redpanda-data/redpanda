/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

namespace cluster {

class controller;
class controller_backend;
class controller_service;
class id_allocator_frontend;
class partition_leaders_table;
class partition_allocator;
class partition_manager;
class shard_table;
class topics_frontend;
class topic_table;
struct topic_table_delta;
class members_manager;
class members_table;
class metadata_cache;
class metadata_dissemination_service;

} // namespace cluster
