/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "coproc/pacemaker.h"
#include "coproc/wasm_event.h"
#include "model/record_batch_reader.h"

#include <seastar/core/future.hh>

#include <absl/container/btree_map.h>

namespace coproc::wasm {

/// Example of serialized json format to be stored in the copro staus topic
/// {
///     "node_id" : 5,
///     "status" : "up/down",
///     "coprocessors" : {
///         "name_a" : {
///             "input_topics": ["foo", "bar", "baz"],
///             "description": "This copro is interesting!"
///         },
///         "name_b" : {
///             "input_topics": ["dd"],
///             "description" : "Strips sensitive info from topic"
///         }
///     }
/// }

/// Queries each shard for the general status and a layout of what scripts and
/// topics per script are registered
///
/// The result is a single record where the key is node is, value is an
/// indicator of current wasm_engine status, and the headers are populated with
/// the script_id/topics of registered scripts
ss::future<model::record_batch_reader> current_status(
  model::node_id,
  ss::sharded<pacemaker>&,
  const absl::btree_map<script_id, deploy_attributes>&);

} // namespace coproc::wasm
