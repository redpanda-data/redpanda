/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

namespace cloud_storage {

class cache;
class partition_recovery_manager;
class remote;
class remote_partition;
class remote_path_provider;
class remote_segment;
class partition_manifest;
class topic_mount_manifest;
class topic_manifest;
class partition_probe;
class async_manifest_view;

struct log_recovery_result;
struct offset_range;
struct topic_recovery_service;

} // namespace cloud_storage
