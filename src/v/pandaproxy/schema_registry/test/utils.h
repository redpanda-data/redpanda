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
      writes_disabled_t wd = writes_disabled_t::no)
      : _wd(wd) {}
    writes_disabled_t writes_disabled() const final { return _wd; }

private:
    writes_disabled_t _wd;
};

/// No-op transport used in tests where seq_writer is only instantiated to
/// receive consume_to_store offset updates.
class noop_transport final : public pandaproxy::schema_registry::transport {
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
