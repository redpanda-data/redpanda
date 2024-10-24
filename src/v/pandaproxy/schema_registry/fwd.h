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

namespace pandaproxy::schema_registry {

class api;
struct configuration;
class schema_id_cache;
class schema_id_validation_probe;
class seq_writer;
class service;
class sharded_store;
class store;

} // namespace pandaproxy::schema_registry
