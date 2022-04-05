/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

namespace coproc {

class api;
class pacemaker;
class script_context;
class partition_manager;
class reconciliation_backend;

namespace wasm {

class script_database;
class script_dispatcher;
class async_event_handler;
class event_listener;

} // namespace wasm

} // namespace coproc
