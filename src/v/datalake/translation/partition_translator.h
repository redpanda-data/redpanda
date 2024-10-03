/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/outcome.h"
#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "datalake/errors.h"
#include "datalake/fwd.h"
#include "features/fwd.h"
#include "model/record_batch_reader.h"
#include "random/simple_time_jitter.h"
#include "ssx/semaphore.h"
#include "utils/prefix_logger.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

namespace kafka {
class read_committed_reader;
}

namespace datalake::translation {

/**
 * A partition translator is responsible for translating data from a given
 * partition periodically. A single instance of translator should be active for
 * a given partition and is attached to the leader replica.
 *
 * The translator periodically wakes up, checks if there is new data that is
 * pending translation and creates a reader for the data range and hands it off
 * to low level iceberg translator that converts the data.
 *
 * The translator works in tandem with the datalake coordinator responsible for
 * this ntp. A successful local iceberg translation of a data range is followed
 * by that state checkpointing with the coordinator. A coordinator tracks the
 * latest translated offset for a given ntp (along with other metadata) which is
 * also then synchronized with the local translation stm that enforces
 * max_collectible_offset across all replicas. The translator tries to work
 * with the stm state as much as possible (to avoid round trips to the
 * coordinator)
 *
 * It is possible that the translation stm can get out of sync with what is
 * on the coordinator, so it is reconciled as needed. It is ok to work on a
 * stale stm state as the eventual checkpoint request with the coordinator
 * fenced based on the translator term.
 *
 * The logic for this translator is like this..
 *
 * while (!aborted && !term_changed):
 *    sleep(interval)
 *    reconcile_with_coordinator_if_needed()
 *    md = translate_newly_arrived_data_since_last_checkpoint()
 *    checkpoint_with_coordinator(md)
 *    sync_stm_with_coordinator(md)
 */

class partition_translator {
public:
    explicit partition_translator(
      ss::lw_shared_ptr<cluster::partition> partition,
      ss::sharded<coordinator::frontend>* frontend,
      ss::sharded<features::feature_table>* features,
      std::chrono::milliseconds translation_interval,
      ss::scheduling_group sg,
      size_t reader_max_bytes,
      ssx::semaphore& parallel_translations);

    ss::future<> stop();

    std::chrono::milliseconds translation_interval() const;
    void reset_translation_interval(std::chrono::milliseconds new_base);

private:
    struct backoff {
        static constexpr std::chrono::milliseconds initial_sleep{300};
        explicit backoff(std::chrono::milliseconds max)
          : max_sleep(max) {}

        std::chrono::milliseconds next() {
            current_sleep = std::min(
              std::max(initial_sleep, current_sleep * 2), max_sleep);
            return current_sleep;
        }

        void reset() { current_sleep = std::chrono::milliseconds{1}; }

        template<
          typename Func,
          typename ShouldRetry,
          typename FuncRet = std::invoke_result_t<Func>>
        requires std::
          predicate<ShouldRetry, typename ss::futurize<FuncRet>::value_type>
          ss::futurize_t<FuncRet>
          retry(int num_attempts, Func&& f, ShouldRetry&& should_retry) {
            reset();
            while (num_attempts-- > 0) {
                auto result = co_await ss::futurize_invoke(f);
                if (num_attempts > 0 && should_retry(result)) {
                    co_await ss::sleep(next());
                    continue;
                }
                co_return result;
            }
            __builtin_unreachable();
        }

        std::chrono::milliseconds current_sleep{1};
        std::chrono::milliseconds max_sleep;
    };

    bool can_continue() const;

    ss::future<std::optional<kafka::offset>> reconcile_with_coordinator();

    ss::future<> do_translate();

    using translation_success = ss::bool_class<struct translation_success>;
    ss::future<translation_success> do_translate_once();
    ss::future<model::record_batch_reader> make_reader();
    ss::future<std::optional<coordinator::translated_offset_range>>
      do_translation_for_range(kafka::read_committed_reader);

    using checkpoint_result = ss::bool_class<struct checkpoint_result>;
    ss::future<checkpoint_result>
      checkpoint_translated_data(coordinator::translated_offset_range);

    kafka::offset min_offset_for_translation() const;
    // Returns max consumable offset for translation.
    kafka::offset max_offset_for_translation() const;

    model::term_id _term;
    ss::lw_shared_ptr<cluster::partition> _partition;
    ss::shared_ptr<translation_stm> _stm;
    ss::sharded<coordinator::frontend>* _frontend;
    ss::sharded<features::feature_table>* _features;
    using jitter_t
      = simple_time_jitter<ss::lowres_clock, std::chrono::milliseconds>;
    jitter_t _jitter;
    // Maximum number of bytes read in one go of translation.
    // Memory usage tracking is not super sophisticated here, so we assume
    // all data batches from the reader are buffered in the writer until
    // they are flushed to disk. This is also factored into determining
    // how many parallel translations can run at one point as we operate under
    // a memory budget for all translations (semaphore below).
    size_t _max_bytes_per_reader;
    ssx::semaphore& _parallel_translations;
    using needs_reconciliation = ss::bool_class<struct needs_reconciliation>;
    needs_reconciliation _reconcile{needs_reconciliation::yes};
    std::filesystem::path _writer_scratch_space;
    ss::gate _gate;
    ss::abort_source _as;
    prefix_logger _logger;
};

} // namespace datalake::translation
