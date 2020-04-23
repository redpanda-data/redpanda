#pragma once

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/tests/utils/random_batch.h"

#include <seastar/util/log.hh>

#include <fmt/core.h>

namespace storage {
extern ss::logger fuzzlogger;

class opfuzz {
public:
    struct op_context {
        model::term_id* term;
        storage::log* log;
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
    };

    enum class op_name : int {
        min = 0,
        append = 0,
        append_with_multiple_terms,
        truncate,
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
};
} // namespace storage
