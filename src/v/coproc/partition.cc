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

#include "coproc/partition.h"

#include "storage/log.h"

namespace coproc {

partition::partition(
  storage::log log, ss::lw_shared_ptr<cluster::partition> source) noexcept
  : _log(log)
  , _source(source) {}

ss::future<model::record_batch_reader>
partition::make_reader(storage::log_reader_config config) {
    return ss::try_with_gate(
      _gate, [this, config] { return _log.make_reader(config); });
}

storage::log_appender
partition::make_appender(storage::log_append_config write_cfg) {
    _gate.check();
    return _log.make_appender(write_cfg);
}

ss::future<std::optional<storage::timequery_result>>
partition::timequery(storage::timequery_config cfg) {
    return ss::try_with_gate(
      _gate, [this, cfg] { return _log.timequery(cfg); });
}

} // namespace coproc
