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

#pragma once

#include "cluster/partition.h"
#include "storage/fwd.h"

#include <seastar/core/gate.hh>

namespace cluster {

class safe_shutdown_appender {
public:
    safe_shutdown_appender(
      ss::gate& g, storage::log_appender&& appender) noexcept;

    ss::future<ss::stop_iteration> operator()(model::record_batch&);
    ss::future<storage::append_result> end_of_stream();

private:
    ss::gate& _gate;
    storage::log_appender _appender;
};

class non_replicable_partition {
public:
    non_replicable_partition(
      storage::log, ss::lw_shared_ptr<partition>) noexcept;

    ss::future<> start() { return ss::now(); }
    ss::future<> stop() { return _gate.close(); }

    const model::ntp& source() const { return _source->ntp(); }
    const model::ntp& ntp() const { return _log.config().ntp(); }
    const storage::ntp_config& config() const { return _log.config(); }
    bool is_leader() const { return _source->is_leader(); }
    model::revision_id get_revision_id() const {
        return _log.config().get_revision();
    }

    /// \brief Returns a reader that enters the \ref _gate when it performs
    /// operations
    ss::future<model::record_batch_reader>
      make_reader(storage::log_reader_config);

    /// \brief Returns an appender that ensures appends will not occur after
    /// future returned from stop() resolves
    safe_shutdown_appender make_appender(storage::log_append_config);

    /// \brief Invokes the timequey method on the \ref _log within the context
    /// of the \ref gate
    ss::future<std::optional<storage::timequery_result>>
      timequery(storage::timequery_config);

private:
    ss::gate _gate;
    storage::log _log;
    ss::lw_shared_ptr<partition> _source;
};

} // namespace cluster
