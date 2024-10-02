/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "pandaproxy/schema_registry/types.h"
#include "schema/registry.h"
#include "wasm/ffi.h"

namespace wasm {

/**
 * The WASM module for redpanda schema registry.
 *
 * This provides an ABI to WASM guests for the wasm::schema_registry wrapper
 */
class schema_registry_module {
public:
    explicit schema_registry_module(schema::registry*);
    schema_registry_module(const schema_registry_module&) = delete;
    schema_registry_module& operator=(const schema_registry_module&) = delete;
    schema_registry_module(schema_registry_module&&) = default;
    schema_registry_module& operator=(schema_registry_module&&) = default;
    ~schema_registry_module() = default;

    static constexpr std::string_view name = "redpanda_schema_registry";

    // Start ABI exports
    void check_abi_version_0();

    ss::future<int32_t> get_schema_definition_len(
      pandaproxy::schema_registry::schema_id, uint32_t*);

    ss::future<int32_t> get_schema_definition(
      pandaproxy::schema_registry::schema_id, ffi::array<uint8_t>);

    ss::future<int32_t> get_subject_schema_len(
      pandaproxy::schema_registry::subject,
      pandaproxy::schema_registry::schema_version,
      uint32_t*);

    ss::future<int32_t> get_subject_schema(
      pandaproxy::schema_registry::subject,
      pandaproxy::schema_registry::schema_version,
      ffi::array<uint8_t>);

    ss::future<int32_t> create_subject_schema(
      pandaproxy::schema_registry::subject,
      ffi::array<uint8_t>,
      pandaproxy::schema_registry::schema_id*);

    // End ABI exports

private:
    schema::registry* _sr;
};
} // namespace wasm
