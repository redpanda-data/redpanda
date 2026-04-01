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

#include "base/units.h"
#include "constants/common.h"
#include "datalake/translation/scheduling.h"
#include "test_utils/test.h"

using namespace std::chrono_literals;

namespace datalake::translation::scheduling {

/**
 * A mock of the partition translator logic implementing the translator API
 * for scheduling testing. The logic closely follows what the partition
 * translator does.
 */
class mock_translator : public translator {
public:
    explicit mock_translator(
      model::ntp ntp,
      clock::duration max_target_lag,
      size_t throughput_bytes_per_sec,
      size_t num_writers);
    ~mock_translator() override = default;

    const translator_id& id() const override;
    ss::future<>
    init(scheduling_notifications&, reservations_tracker&) override;
    ss::future<> close() noexcept override;
    translation_status status() const override;
    void start_translation(clock::duration translate_for) override;
    void stop_translation(stop_reason) override;
    std::chrono::milliseconds current_lag_ms() const override {
        return std::chrono::milliseconds{0};
    }
    void set_finish_translation() final {
        _finish_translation_requested = true;
    }
    bool get_finish_translation() final {
        return _finish_translation_requested;
    }

private:
    // Mock of a single parquet file writer.
    struct mock_partition_writer {
        size_t total_bytes_translated{0};
        size_t total_bytes_reserved{0};
        size_t remaining_free_bytes{0};
        chunked_vector<ssx::semaphore_units> memory_units;
        chunked_vector<size_t> checkpointed_files;
        reservations_tracker* mem_tracker;

        ss::future<> write(size_t bytes, ss::abort_source& as);
        void flush();
        void checkpoint();
    };
    ss::future<> do_translate(clock::duration deadline);
    ss::future<> translation_loop();
    ss::future<> notify_ready();
    ss::future<> notify_done();
    // non checkpointed.
    size_t total_bytes_reserved() const;
    ss::future<> maybe_checkpoint();
    ss::future<> flush_writers();
    struct mock_inflight_translation_state {
        clock::time_point start_time;
        clock::duration translate_for;
        ss::abort_source as;
        ss::gate gate;
    };

    model::ntp _ntp;
    clock::duration _max_target_lag;
    size_t _translation_tput_bytes_per_sec;
    // throughput spread evently among writers
    // to simulate partitioning.
    const size_t _num_writers;
    scheduling_notifications* _notifier;
    reservations_tracker* _mem_tracker;
    ss::gate _gate;
    ss::abort_source _as;
    bool _started{false};
    // partition writers
    chunked_vector<mock_partition_writer> _writers;
    // Only set if translation is in progress
    std::optional<mock_inflight_translation_state> _translation_state;
    ss::timer<clock> _translation_timer;
    ss::condition_variable _wait_for_scheduler_cb;
    clock::time_point _next_checkpoint;
    bool _finish_translation_requested{false};
};

// A translator that overshoots deadline and requires explict force flushing
// from the scheduler.
class delaying_translator : public mock_translator {
public:
    explicit delaying_translator(
      model::ntp ntp,
      clock::duration max_target_lag,
      size_t throughput_bytes_per_sec,
      size_t num_writers)
      : mock_translator(
          std::move(ntp),
          max_target_lag,
          throughput_bytes_per_sec,
          num_writers) {}
    void start_translation(clock::duration deadline) override;
};

class exceptional_translator : public mock_translator {
public:
    explicit exceptional_translator(
      model::ntp ntp,
      clock::duration max_target_lag,
      size_t throughput_bytes_per_sec,
      size_t num_writers)
      : mock_translator(
          std::move(ntp),
          max_target_lag,
          throughput_bytes_per_sec,
          num_writers) {}
    ss::future<>
    init(scheduling_notifications&, reservations_tracker&) override;
    void start_translation(clock::duration deadline) override;
    void stop_translation(stop_reason) override;
};

class noop_disk_manager : public disk_manager {
public:
    // the default noop disk manager implements an infinite disk
    ss::future<size_t> reserve() override { co_return 1_MiB; }
};

class scheduler_fixture : public seastar_test {
public:
    virtual ss::lw_shared_ptr<scheduler> make_scheduler() {
        return ss::make_lw_shared<scheduler>(
          total_memory, block_size, make_scheduling_policy(), _disk_manager);
    }

    virtual std::unique_ptr<scheduling_policy> make_scheduling_policy() {
        return scheduling_policy::make_default(
          config::mock_binding(max_concurrent_translators), task_time_quota);
    }

    ss::future<> SetUpAsync() override {
        _scheduler = make_scheduler();
        return ss::now();
    }

    ss::future<> TearDownAsync() override { return _scheduler->stop(); }

protected:
    // scheduler properties
    static constexpr size_t total_memory = 400_MiB;
    static constexpr size_t block_size = 4_MiB;
    static constexpr size_t max_concurrent_translators = 4;
    static constexpr clock::duration task_time_quota
      = std::chrono::duration_cast<clock::duration>(250ms);
    static constexpr clock::duration high_task_time_quota
      = std::chrono::duration_cast<clock::duration>(60s);

    // translator properties
    // bucketed in S/M/L specs to mix and match
    static constexpr clock::duration large_target_lag
      = std::chrono::duration_cast<clock::duration>(6h);
    static constexpr size_t large_translation_throughput = 32_MiB;
    static constexpr size_t large_concurrent_writers
      = constants::common::default_concurrency;

    static constexpr clock::duration medium_target_lag
      = std::chrono::duration_cast<clock::duration>(1h);
    static constexpr size_t medium_translation_throughput = 4_MiB;
    static constexpr size_t medium_concurrent_writers = 8;

    static constexpr clock::duration small_lag_duration
      = std::chrono::duration_cast<clock::duration>(1min);
    static constexpr size_t small_partition_throughput = 1_MiB;
    static constexpr size_t small_writers_per_translator = 2;

    struct translator_spec {
        clock::duration max_target_lag;
        size_t translation_throughput_bytes_per_sec{0};
        size_t num_concurrent_writers{0};
    };

    model::ntp make_ntp();

    static translator_spec make_default_spec();

    static translator_spec make_random_spec();

    std::unique_ptr<translator>
    make_normal_translator(translator_spec spec = make_default_spec());

    std::unique_ptr<translator>
    make_delaying_translator(translator_spec spec = make_default_spec());

    std::unique_ptr<translator>
    make_exceptional_translator(translator_spec spec = make_default_spec());

    std::unique_ptr<translator>
    make_random_translator(translator_spec spec = make_random_spec());

    int _partition_counter{0};
    ss::lw_shared_ptr<scheduler> _scheduler;

    noop_disk_manager _disk_manager;
};

} // namespace datalake::translation::scheduling
