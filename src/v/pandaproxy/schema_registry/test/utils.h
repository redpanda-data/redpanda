// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once
#include "pandaproxy/schema_registry/seq_writer.h"
#include "pandaproxy/schema_registry/transport.h"

#include <stdexcept>

class sequence_state_checker_test
  : public pandaproxy::schema_registry::sequence_state_checker {
public:
    explicit sequence_state_checker_test(
      writes_disabled_t client_writes_disabled = writes_disabled_t::no,
      writes_disabled_t sync_writes_disabled = writes_disabled_t::no)
      : _client_writes_disabled(client_writes_disabled)
      , _sync_writes_disabled(sync_writes_disabled) {}

    writes_disabled_t writes_disabled(
      pandaproxy::schema_registry::write_source source,
      const pandaproxy::schema_registry::context&) const final {
        switch (source) {
        case pandaproxy::schema_registry::write_source::client:
            return _client_writes_disabled;
        case pandaproxy::schema_registry::write_source::schema_registry_sync:
            return _sync_writes_disabled;
        }
    }

private:
    writes_disabled_t _client_writes_disabled;
    writes_disabled_t _sync_writes_disabled;
};

/// Transport whose operations all throw, for tests where the transport
/// must not be reached.
class noop_transport : public pandaproxy::schema_registry::transport {
public:
    ss::future<> stop() final { return ss::now(); }
    ss::future<pandaproxy::schema_registry::produce_result>
    produce(model::record_batch) override {
        throw std::runtime_error("noop_transport::produce not implemented");
    }
    ss::future<model::offset> get_high_watermark() override {
        throw std::runtime_error(
          "noop_transport::get_high_watermark not implemented");
    }
    ss::future<> consume_range(
      model::offset,
      model::offset,
      ss::noncopyable_function<
        ss::future<ss::stop_iteration>(model::record_batch)>) override {
        throw std::runtime_error(
          "noop_transport::consume_range not implemented");
    }
    ss::future<cluster::errc> create_topic(
      model::topic_namespace_view,
      int32_t,
      cluster::topic_properties,
      int16_t) final {
        throw std::runtime_error(
          "noop_transport::create_topic not implemented");
    }
};

/// Transport that applies writes and records produced batches.
class accepting_transport final : public noop_transport {
public:
    ss::future<pandaproxy::schema_registry::produce_result>
    produce(model::record_batch batch) override {
        auto base = batch.base_offset();
        produced.push_back(std::move(batch));
        return ss::make_ready_future<
          pandaproxy::schema_registry::produce_result>(
          pandaproxy::schema_registry::produce_result{.base_offset = base});
    }

    ss::future<model::offset> get_high_watermark() override {
        return ss::make_ready_future<model::offset>(model::offset{0});
    }

    chunked_vector<model::record_batch> produced;
};
