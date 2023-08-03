/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

namespace storage {

class api;
class node_api;
class kvstore;
class log;
class log_manager;
class ntp_config;
class segment;
class segment_appender;
class simple_snapshot_manager;
class snapshot_manager;
class storage_resources;
class readers_cache;
class compaction_controller;
class offset_translator_state;
struct log_reader_config;
struct timequery_config;

} // namespace storage
