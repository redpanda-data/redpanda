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

#include "model/record.h"
#include "model/record_utils.h"
#include "model/transform.h"
#include "transform/api.h"
#include "transform/logger.h"
#include "wasm/engine.h"
#include "wasm/transform_probe.h"

#include <seastar/core/chunked_fifo.hh>

namespace transform {

produce_path_executor::produce_path_executor(service& svc)
  : _svc(svc) {}

ss::future<std::unique_ptr<model::record_batch>> produce_path_executor::execute(
  model::topic_namespace_view topic,
  std::unique_ptr<model::record_batch> batch) {
    auto transform_id = _svc.get_produce_path_transform(topic);
    if (!transform_id) {
        co_return batch;
    }

    auto* entry = co_await get_or_create_engine(*transform_id);
    if (!entry) {
        throw std::runtime_error("produce-path transform engine unavailable");
    }

    auto orig_header = batch->header();

    ss::chunked_fifo<model::transformed_data> records;
    co_await entry->engine->transform(
      std::move(*batch),
      entry->probe.get(),
      [&records](
        this auto,
        std::optional<model::topic_view> topic,
        model::transformed_data data) -> ss::future<wasm::write_success> {
          if (topic) {
              co_return wasm::write_success::no;
          }
          records.push_back(std::move(data));
          co_return wasm::write_success::yes;
      });

    if (records.empty()) {
        throw std::runtime_error("produce-path transform produced no records");
    }

    auto new_batch = model::transformed_data::make_batch(
      orig_header.first_timestamp, std::move(records));
    new_batch.header().producer_id = orig_header.producer_id;
    new_batch.header().producer_epoch = orig_header.producer_epoch;
    new_batch.header().base_sequence = orig_header.base_sequence;
    new_batch.header().attrs = orig_header.attrs;
    new_batch.header().crc = model::crc_record_batch(new_batch);
    new_batch.header().header_crc = model::internal_header_only_crc(
      new_batch.header());

    co_return std::make_unique<model::record_batch>(std::move(new_batch));
}

ss::future<> produce_path_executor::stop() {
    for (auto& [id, entry] : _engines) {
        co_await entry.engine->stop();
    }
    _engines.clear();
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
    auto probe = std::make_unique<wasm::transform_probe>();
    probe->setup_metrics(result->name);
    auto [inserted, _] = _engines.emplace(
      id,
      engine_entry{
        .engine = std::move(result->engine),
        .probe = std::move(probe),
      });
    co_return &inserted->second;
}

} // namespace transform
