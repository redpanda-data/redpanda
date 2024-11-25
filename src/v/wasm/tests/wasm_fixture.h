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

#include "model/record.h"
#include "model/transform.h"
#include "pandaproxy/schema_registry/types.h"
#include "serde/rw/rw.h"
#include "serde/rw/scalar.h"
#include "test_utils/test.h"
#include "wasm/api.h"
#include "wasm/schema_registry.h"
#include "wasm/transform_probe.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

class fake_schema_registry;

class WasmTestFixture : public ::testing::Test {
public:
    static constexpr model::timestamp NOW = model::timestamp(1687201340524ULL);
    static constexpr size_t MAX_MEMORY = 2_MiB;

    static void SetUpTestSuite();

    void SetUp() override;
    void TearDown() override;

    void load_wasm(const std::string& path);
    model::record_batch make_tiny_batch();
    model::record_batch make_tiny_batch(iobuf record_value);
    model::record_batch transform(const model::record_batch&);
    // For the "dynamic" wasm transform, issues a command record with the
    // corresponding value
    template<typename T>
    void execute_command(std::string_view cmd, T&& value) {
        iobuf k;
        k.append(cmd.data(), cmd.size());
        iobuf v;
        serde::write(v, std::forward<T>(value));
        storage::record_batch_builder b(
          model::record_batch_type::raft_data, model::offset(1));
        b.add_raw_kv(std::move(k), std::move(v));
        transform(std::move(b).build());
    }

    model::transform_metadata meta() const { return _meta; };

    wasm::engine* engine() { return _engine.get(); }

    const std::vector<pandaproxy::schema_registry::subject_schema>&
    registered_schemas() const;

private:
    std::unique_ptr<wasm::runtime> _runtime;
    ss::shared_ptr<wasm::factory> _factory;
    ss::shared_ptr<wasm::engine> _engine;
    std::unique_ptr<wasm::transform_probe> _probe;
    fake_schema_registry* _sr;
    model::transform_metadata _meta;
};
