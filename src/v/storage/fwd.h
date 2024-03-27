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
class compacted_index_writer;
class compaction_controller;
class key_offset_map;
class kvstore;
class node_api;
class ntp_config;
class log;
class log_manager;
class probe;
class offset_translator;
class offset_translator_state;
class readers_cache;
class segment;
class segment_appender;
class simple_snapshot_manager;
class snapshot_manager;
class storage_resources;
struct compaction_config;
struct log_reader_config;
struct timequery_config;

} // namespace storage
