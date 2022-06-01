/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "storage/log.h"

#include <seastar/util/log.hh>

#include <fmt/core.h>

namespace storage {
extern ss::logger fuzzlogger;

class opfuzz {
public:
    struct op_context {
        model::term_id* term;
        storage::log* log;
        ss::abort_source* _as;
    };
    struct op {
        op() noexcept = default;
        op(const op&) = default;
        op& operator=(const op&) = delete;
        op(op&&) noexcept = default;
        op& operator=(op&&) noexcept = delete;
        virtual ~op() noexcept = default;

        // main api
        virtual const char* name() const = 0;
        virtual ss::future<> invoke(op_context ctx) = 0;

        // unset if an op should not run concurrently with compaction
        bool concurrent_compaction_safe{true};
    };

    enum class op_name : int {
        min = 0,
        append = 0,
        append_with_multiple_terms,
        append_op_foreign,
        compact,
        remove_all_compacted_indices,
        truncate,
        truncate_prefix,
        read,
        flush,
        term_roll,
        max = term_roll,
    };

    opfuzz(storage::log l, size_t ops_count)
      : _log(std::move(l)) {
        generate_workload(ops_count);
    }
    ~opfuzz() noexcept = default;
    opfuzz(const opfuzz&) = delete;
    opfuzz& operator=(const opfuzz&) = delete;
    opfuzz(opfuzz&&) noexcept = default;
    opfuzz& operator=(opfuzz&&) noexcept = default;

    ss::future<> execute();
    const storage::log& log() const { return _log; }

private:
    std::unique_ptr<op> random_operation();
    void generate_workload(size_t count);

    model::term_id _term = model::term_id(0);
    std::vector<std::unique_ptr<op>> _workload;
    storage::log _log;
    ss::abort_source _as;
};
} // namespace storage
