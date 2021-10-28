/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/non_replicable_partition.h"

#include "storage/log.h"

namespace {

/// Ensures that operations on a model::record_batch_reader occur within the
/// context of a gate for safe shutdown. If operations are observed while the
/// gate is closed the caller will observe the ss::gate_closed_exception
class gated_shutdown_reader final : public model::record_batch_reader::impl {
public:
    gated_shutdown_reader(
      ss::gate& g,
      std::unique_ptr<model::record_batch_reader::impl> impl) noexcept
      : _gate(g)
      , _impl(std::move(impl)) {}

    bool is_end_of_stream() const final {
        return _gate.is_closed() || _impl->is_end_of_stream();
    }

    ss::future<model::record_batch_reader::storage_t>
    do_load_slice(model::timeout_clock::time_point t) final {
        auto holder = _gate.hold();
        return _impl->do_load_slice(t).finally([holder] {});
    }

    void print(std::ostream& os) final { os << "gated_shutdown_reader"; }

private:
    ss::gate& _gate;
    std::unique_ptr<model::record_batch_reader::impl> _impl;
};

} // namespace

namespace cluster {
/// Not neccessary to hold gate open until all processing completes, as that can
/// be done by invoking the write within the context of the \ref
/// non_replicable_partition gate. This class just makes the operation
/// cancellable
safe_shutdown_appender::safe_shutdown_appender(
  ss::gate& g, storage::log_appender&& appender) noexcept
  : _gate(g)
  , _appender(std::move(appender)) {}

ss::future<ss::stop_iteration>
safe_shutdown_appender::operator()(model::record_batch& rb) {
    if (_gate.is_closed()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    }
    return _appender.operator()(rb);
}

ss::future<storage::append_result> safe_shutdown_appender::end_of_stream() {
    return _appender.end_of_stream();
}

non_replicable_partition::non_replicable_partition(
  storage::log log, ss::lw_shared_ptr<partition> source) noexcept
  : _log(log)
  , _source(source) {}

ss::future<model::record_batch_reader>
non_replicable_partition::make_reader(storage::log_reader_config config) {
    return ss::with_gate(_gate, [this, config = std::move(config)] {
        return _log.make_reader(config).then(
          [this](model::record_batch_reader rdr) {
              return model::make_record_batch_reader<gated_shutdown_reader>(
                _gate, std::move(rdr).release());
          });
    });
}

safe_shutdown_appender
non_replicable_partition::make_appender(storage::log_append_config write_cfg) {
    auto appender = _log.make_appender(write_cfg);
    return safe_shutdown_appender(_gate, std::move(appender));
}

ss::future<std::optional<storage::timequery_result>>
non_replicable_partition::timequery(storage::timequery_config cfg) {
    return ss::with_gate(
      _gate, [this, cfg = std::move(cfg)] { return _log.timequery(cfg); });
}
} // namespace cluster
