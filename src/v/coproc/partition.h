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

#pragma once

#include "cluster/partition.h"
#include "storage/fwd.h"

#include <seastar/core/gate.hh>

namespace coproc {

class partition {
public:
    partition(storage::log, ss::lw_shared_ptr<cluster::partition>) noexcept;

    ss::future<> start() { return ss::now(); }
    ss::future<> stop() { return _gate.close(); }

    model::offset start_offset() const { return _log.offsets().start_offset; }
    model::offset dirty_offset() const { return _log.offsets().dirty_offset; }

    const model::ntp& source() const { return _source->ntp(); }
    const model::ntp& ntp() const { return _log.config().ntp(); }
    const storage::ntp_config& config() const { return _log.config(); }
    bool is_elected_leader() const { return _source->is_elected_leader(); }
    bool is_leader() const { return _source->is_leader(); }
    model::term_id term() const { return _source->term(); }
    model::revision_id get_revision_id() const {
        return _log.config().get_revision();
    }

    std::optional<model::offset>
    get_term_last_offset(model::term_id term) const {
        return _source->get_term_last_offset(term);
    }

    /// \brief Returns a reader that enters the \ref _gate when it performs
    /// operations
    ss::future<model::record_batch_reader>
      make_reader(storage::log_reader_config);

    /// \brief Returns an appender that ensures appends will not occur after
    /// future returned from stop() resolves
    storage::log_appender make_appender(storage::log_append_config);

    /// \brief Invokes the timequery method on the \ref _log within the context
    /// of the \ref gate
    ss::future<std::optional<storage::timequery_result>>
      timequery(storage::timequery_config);

    /// \brief get a handle to the source partition
    ss::lw_shared_ptr<cluster::partition> source_partition() { return _source; }

private:
    ss::gate _gate;
    storage::log _log;
    ss::lw_shared_ptr<cluster::partition> _source;
};

} // namespace coproc
