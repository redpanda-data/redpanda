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

#include "transform/produce_path_executor.h"

#include "cluster/errc.h"
#include "model/record.h"
#include "model/record_utils.h"
#include "model/transform.h"
#include "transform/api.h"
#include "transform/logger.h"
#include "transform/rpc/client.h"
#include "wasm/engine.h"
#include "wasm/transform_probe.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/coroutine/as_future.hh>

namespace transform {

produce_path_executor::produce_path_executor(service& svc)
  : _svc(svc) {}

ss::future<execute_result> produce_path_executor::execute(
  model::topic_namespace_view topic,
  std::unique_ptr<model::record_batch> batch,
  std::optional<wasm::request_metadata> request_info) {
    auto transform_id = _svc.get_produce_path_transform(topic);
    if (!transform_id) {
        co_return std::move(batch);
    }

    auto* entry = co_await get_or_create_engine(*transform_id);
    if (!entry) {
        co_return std::unexpected(
          execute_error{
            .code = execute_errc::engine_unavailable,
            .message = "produce-path transform engine unavailable",
          });
    }

    auto orig_header = batch->header();

    ss::chunked_fifo<model::transformed_data> input_records;
    chunked_hash_map<
      model::topic_namespace,
      ss::chunked_fifo<model::transformed_data>>
      output_records;

    auto transform_fut = co_await ss::coroutine::as_future(
      entry->engine->transform(
        std::move(*batch),
        entry->probe.get(),
        [&input_records, &output_records, &entry](
          this auto,
          std::optional<model::topic_view> topic,
          model::transformed_data data) -> ss::future<wasm::write_success> {
            if (!topic) {
                input_records.push_back(std::move(data));
                co_return wasm::write_success::yes;
            }
            for (const auto& out : entry->output_topics) {
                if (std::string_view{out.tp()} == topic.value()()) {
                    output_records[out].push_back(std::move(data));
                    co_return wasm::write_success::yes;
                }
            }
            co_return wasm::write_success::no;
        },
        std::move(request_info)));

    if (transform_fut.failed()) {
        co_return std::unexpected(
          execute_error{
            .code = execute_errc::transform_failed,
            .message = ss::format(
              "produce-path transform rejected record: {}",
              transform_fut.get_exception()),
          });
    }

    size_t total_records = input_records.size();
    for (const auto& [_, recs] : output_records) {
        total_records += recs.size();
    }
    if (total_records == 0) {
        co_return std::unexpected(
          execute_error{
            .code = execute_errc::no_output_records,
            .message = "produce-path transform produced no records",
          });
    }
    if (input_records.empty() && orig_header.producer_id >= 0) {
        co_return std::unexpected(
          execute_error{
            .code = execute_errc::empty_batch_idempotent,
            .message = "produce-path transform dropped all records from "
                       "input topic for an idempotent producer",
          });
    }

    // Write fan-out batches to output topics before returning the input
    // batch. Failure here means the caller should not write the input
    // batch -- giving us all-or-nothing semantics.
    for (auto& [topic_ns, recs] : output_records) {
        if (recs.empty()) {
            continue;
        }
        auto fanout_batch = model::transformed_data::make_batch(
          model::timestamp::now(), std::move(recs));

        ss::chunked_fifo<model::record_batch> batches;
        batches.push_back(std::move(fanout_batch));

        // TODO: route fan-out writes across output partitions instead
        // of always targeting partition 0. The sidecar path does linear
        // probing from the input partition to find a non-disabled
        // candidate (see compute_output_partition in api.cc). Until we
        // replicate that logic, fan-out writes will fail if partition 0
        // of the output topic is disabled, and all fan-out traffic from
        // every input partition funnels into a single output partition.
        auto ec = co_await _svc.rpc_client().produce(
          model::topic_partition(topic_ns.tp, model::partition_id(0)),
          std::move(batches));

        if (ec != cluster::errc::success) {
            co_return std::unexpected(
              execute_error{
                .code = execute_errc::fanout_write_failed,
                .message = ss::format(
                  "produce-path fan-out write to {} failed: {}",
                  topic_ns,
                  cluster::error_category().message(int(ec))),
              });
        }
    }

    if (!input_records.empty()) {
        auto new_batch = model::transformed_data::make_batch(
          orig_header.first_timestamp, std::move(input_records));
        new_batch.header().producer_id = orig_header.producer_id;
        new_batch.header().producer_epoch = orig_header.producer_epoch;
        new_batch.header().base_sequence = orig_header.base_sequence;
        new_batch.header().attrs = orig_header.attrs;
        new_batch.header().crc = model::crc_record_batch(new_batch);
        new_batch.header().header_crc = model::internal_header_only_crc(
          new_batch.header());

        co_return std::make_unique<model::record_batch>(std::move(new_batch));
    }

    // All records were routed to output topics. Return nullptr to signal
    // "nothing to write to input topic."
    co_return std::unique_ptr<model::record_batch>(nullptr);
}

ss::future<> produce_path_executor::stop() {
    for (auto& [id, entry] : _engines) {
        co_await entry.engine->stop();
    }
    _engines.clear();
}

ss::future<> produce_path_executor::warm(model::transform_id id) {
    // Pre-fetch the engine and start it. If a concurrent produce
    // triggers get_or_create_engine for the same id, it will find
    // the engine already in the map.
    auto it = _engines.find(id);
    if (it != _engines.end()) {
        co_return;
    }
    co_await get_or_create_engine(id);
}

bool produce_path_executor::is_running(model::transform_id id) const {
    return _engines.contains(id);
}

ss::future<> produce_path_executor::evict(model::transform_id id) {
    auto it = _engines.find(id);
    if (it != _engines.end()) {
        co_await it->second.engine->stop();
        _engines.erase(it);
    }
}

ss::future<produce_path_executor::engine_entry*>
produce_path_executor::get_or_create_engine(model::transform_id id) {
    auto it = _engines.find(id);
    if (it != _engines.end()) {
        co_return &it->second;
    }
    auto result = co_await _svc.get_produce_path_engine(id);
    if (!result) {
        co_return nullptr;
    }
    co_await result->engine->start();
    // Re-check after the co_awaits: another coroutine (warm() or a
    // concurrent produce) may have created the engine while we were
    // suspended. If so, stop the one we just created and use theirs.
    it = _engines.find(id);
    if (it != _engines.end()) {
        co_await result->engine->stop();
        co_return &it->second;
    }
    auto probe = std::make_unique<wasm::transform_probe>();
    probe->setup_metrics(result->name);
    auto [inserted, _] = _engines.emplace(
      id,
      engine_entry{
        .engine = std::move(result->engine),
        .probe = std::move(probe),
        .output_topics = std::move(result->output_topics),
      });
    co_return &inserted->second;
}

} // namespace transform
