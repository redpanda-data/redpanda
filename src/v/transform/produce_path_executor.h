/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/transform.h"
#include "wasm/fwd.h"
#include "wasm/request_metadata.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

#include <expected>
#include <memory>

namespace wasm {
class transform_probe;
}

namespace transform {

class service;

/// Error from produce-path transform execution.
enum class execute_errc {
    /// The WASM engine could not be created or started.
    engine_unavailable,
    /// The WASM transform trapped or timed out.
    transform_failed,
    /// The transform produced zero output records.
    no_output_records,
    /// The transform dropped all records from the input topic for
    /// an idempotent producer (would break sequence tracking).
    empty_batch_idempotent,
    /// Writing to a fan-out output topic failed.
    fanout_write_failed,
};

struct execute_error {
    execute_errc code;
    ss::sstring message;
};

/// Result of execute(): either the (possibly null) transformed batch,
/// or an error. A null batch means the transform routed everything
/// to output topics and nothing should be written to the input topic.
using execute_result
  = std::expected<std::unique_ptr<model::record_batch>, execute_error>;

/// Executes produce-path WASM transforms inline during Kafka produce.
///
/// Holds started engines keyed by transform_id. Engines are created
/// lazily on first use and stay alive until evicted (transform delete
/// or shutdown). The shared_engine's internal mutex serializes
/// concurrent transform() calls on the same engine.
///
/// Thread-local: one instance per shard, accessed via sharded<service>.
class produce_path_executor {
public:
    explicit produce_path_executor(service& svc);
    produce_path_executor(const produce_path_executor&) = delete;
    produce_path_executor& operator=(const produce_path_executor&) = delete;
    produce_path_executor(produce_path_executor&&) = delete;
    produce_path_executor& operator=(produce_path_executor&&) = delete;
    ~produce_path_executor() = default;

    /// Execute a produce-path transform if one exists for this topic.
    ///
    /// If no transform is deployed for this topic, returns the original
    /// batch unchanged. If a transform exists, runs the WASM engine
    /// inline and returns the transformed batch with the original
    /// batch identity preserved.
    ss::future<execute_result> execute(
      model::topic_namespace_view,
      std::unique_ptr<model::record_batch>,
      std::optional<wasm::request_metadata> = std::nullopt);

    /// Stop all engines. Called during service shutdown.
    ss::future<> stop();

    /// Evict the engine for a given transform. Called when a
    /// transform is deleted or redeployed.
    ss::future<> evict(model::transform_id);

private:
    struct engine_entry {
        ss::shared_ptr<wasm::engine> engine;
        std::unique_ptr<wasm::transform_probe> probe;
        std::vector<model::topic_namespace> output_topics;
    };

    ss::future<engine_entry*> get_or_create_engine(model::transform_id);

    service& _svc;
    chunked_hash_map<model::transform_id, engine_entry> _engines;
};

} // namespace transform
