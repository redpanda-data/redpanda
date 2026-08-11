/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/translation/partition_translator.h"
#include "datalake/translation/tests/scheduler_fixture.h"
#include "model/namespace.h"
#include "model/tests/random_batch.h"
#include "test_utils/async.h"

#include <seastar/util/defer.hh>

using namespace datalake::translation;
using namespace datalake::coordinator;

static ss::logger test_logger{"translator_test"};

static const ss::scheduling_group test_sg = ss::default_scheduling_group();
static thread_local model::partition_id::type partition_counter = 0;
static constexpr std::chrono::milliseconds retry_initial_backoff{1ms};
static constexpr std::chrono::milliseconds retry_max_timeout{10ms};

static model::ntp next_ntp() {
    return {
      model::kafka_namespace,
      model::topic{"test"},
      model::partition_id{partition_counter++}};
}

// Encapsulates test context and also acts as a medium of communication between
// test driver and the translator interfaces that hold a reference to this
// context.
class fake_test_ctx {
public:
    explicit fake_test_ctx(
      model::ntp ntp, model::revision_id rev, model::term_id term)
      : _ntp(std::move(ntp))
      , _rev(rev)
      , _term(term)
      , _max_translatable_offset(kafka::offset{100}) {}

    const model::ntp& ntp() const { return _ntp; }
    model::revision_id rev() const { return _rev; }
    model::term_id term() const { return _term; }

    void set_max_translatable_offset(kafka::offset offset) {
        _max_translatable_offset = offset;
    }

    kafka::offset max_translatable_offset() const {
        return _max_translatable_offset;
    }

    kafka::offset max_translated_offset() const {
        return _highest_translated_offset;
    }

    // coordinator API error propagation
    void set_error_on_add_translated_files(bool error) {
        _error_on_add_translated_files = error;
    }

    bool error_on_add_translated_files() const {
        return _error_on_add_translated_files;
    }

    void set_coordinator_backpressure(bool b) { _coordinator_backpressure = b; }

    bool coordinator_backpressure() const { return _coordinator_backpressure; }

    void note_backpressure_rejection() { _backpressure_rejections++; }

    ss::future<> wait_for_backpressure_rejections(
      size_t n, std::chrono::seconds timeout = 10s) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(
          timeout, [n, this] { return _backpressure_rejections >= n; });
    }

    // translation error propagation
    void set_error_on_translation(bool error) { _error_on_translation = error; }

    bool error_on_translation() const { return _error_on_translation; }

    void set_error_on_flush(bool error) { _error_on_flush = error; }

    bool error_on_flush() const { return _error_on_flush; }

    void translation_attempt() { _num_translation_attempts++; }

    void set_should_finish_inflight_translation(bool should_finish) {
        _should_finish_inflight_translation = should_finish;
    }
    bool should_finish_inflight_translation() {
        return _should_finish_inflight_translation;
    }

    const auto& translated_files() const { return _files; }

    void add_translated_files(
      const model::topic_partition& tp, chunked_vector<data_file> files) {
        auto& partition_files = _files[tp];
        for (auto& file : files) {
            partition_files.push_back(std::move(file));
        }
    }

    ss::future<> wait_for_translation_attempts(
      size_t attempts, std::chrono::seconds timeout = 10s) {
        auto target = _num_translation_attempts + attempts;
        RPTEST_REQUIRE_EVENTUALLY_CORO(timeout, [target, this] {
            return _num_translation_attempts >= target;
        });
    }

    void update_highest_translated_offset(kafka::offset offset) {
        _highest_translated_offset = std::max(
          _highest_translated_offset, offset);
        _max_translatable_offset = std::max(
          _max_translatable_offset,
          _highest_translated_offset + kafka::offset_delta(100));
    }

    std::optional<size_t> translation_backlog() const {
        return _translation_backlog;
    }

    std::chrono::milliseconds target_lag() const { return 1s; }

private:
    model::ntp _ntp;
    model::revision_id _rev;
    model::term_id _term;

    kafka::offset _highest_translated_offset{};
    kafka::offset _max_translatable_offset{};

    bool _error_on_add_translated_files{false};
    bool _coordinator_backpressure{false};
    size_t _backpressure_rejections{0};
    bool _error_on_translation{false};
    bool _error_on_flush{false};
    size_t _num_translation_attempts{0};
    bool _should_finish_inflight_translation{true};

    absl::flat_hash_map<model::topic_partition, chunked_vector<data_file>>
      _files;
    std::optional<size_t> _translation_backlog;
};

class fake_data_src : public data_source {
public:
    explicit fake_data_src(fake_test_ctx& partition)
      : _test_ctx(partition) {}

    void close() noexcept final {}

    const model::ntp& ntp() const final { return _test_ctx.ntp(); }

    model::revision_id topic_revision() const final { return _test_ctx.rev(); }

    model::term_id term() const final { return _test_ctx.term(); }

    ss::future<std::optional<kafka::offset>> wait_for_data_to_translate(
      std::optional<kafka::offset> last_translated_offset,
      ss::lowres_clock::time_point,
      ss::abort_source& as) final {
        auto deadline = ss::lowres_clock::now() + 5ms;
        while (!as.abort_requested() && ss::lowres_clock::now() < deadline) {
            auto offset = max_offset_for_translation();
            vlog(
              test_logger.trace,
              "current max offset: {}",
              offset.value_or(kafka::offset{}));
            if (offset && offset.value() >= kafka::offset{0} && (!last_translated_offset || offset.value() > last_translated_offset.value())) {
                co_return last_translated_offset
                  ? kafka::next_offset(last_translated_offset.value())
                  : min_offset_for_translation();
            }
            co_await ss::sleep_abortable(1ms, as);
        }
        co_return std::nullopt;
    }

    ss::future<std::optional<model::record_batch_reader>>
    make_log_reader(kafka::offset o, ss::abort_source&) final {
        auto batches = co_await model::test::make_random_batches(
          kafka::offset_cast(o), 500, false);
        auto reader = model::make_generating_record_batch_reader(
          [batches = std::move(batches)]() mutable {
              return ss::make_ready_future<model::record_batch_reader::data_t>(
                std::move(batches));
          });
        co_return co_await ss::make_ready_future<
          std::optional<model::record_batch_reader>>(std::move(reader));
    }

    kafka::offset min_offset_for_translation() const final {
        return kafka::offset{0};
    }

    std::optional<kafka::offset> max_offset_for_translation() const final {
        return _test_ctx.max_translatable_offset();
    }

    ss::future<std::error_code> replicate_highest_translated_offset(
      kafka::offset offset,
      std::optional<model::timestamp>,
      model::term_id,
      model::timeout_clock::duration,
      ss::abort_source&) final {
        _test_ctx.update_highest_translated_offset(offset);
        co_return std::make_error_code(std::errc());
    }

private:
    fake_test_ctx& _test_ctx;
};

class fake_coordinator_api : public coordinator_api {
public:
    explicit fake_coordinator_api(fake_test_ctx& test_ctx)
      : _test_ctx(test_ctx) {}

    ss::future<add_translated_data_files_reply>
    add_translated_data_files(add_translated_data_files_request request) final {
        add_translated_data_files_reply reply{};
        if (_test_ctx.error_on_add_translated_files()) {
            reply.errc = errc::not_leader;
            co_return reply;
        }
        reply.errc = errc::ok;
        if (request.ranges.empty()) {
            co_return reply;
        }
        auto& last = request.ranges.back();
        auto it = _last_added_offsets.find(request.tp);
        if (it == _last_added_offsets.end() || it->second < last.last_offset) {
            _last_added_offsets[request.tp] = last.last_offset;
            for (auto& range : request.ranges) {
                _test_ctx.add_translated_files(
                  request.tp, std::move(range.files));
            }
        } else {
            reply.errc = errc::failed;
        }
        co_return reply;
    }

    ss::future<fetch_latest_translated_offset_reply>
    fetch_latest_translated_offset(
      fetch_latest_translated_offset_request request) final {
        fetch_latest_translated_offset_reply reply;
        reply.errc = errc::ok;
        auto it = _last_added_offsets.find(request.tp);
        if (it != _last_added_offsets.end()) {
            reply.last_added_offset = it->second;
        }
        if (_test_ctx.coordinator_backpressure()) {
            _test_ctx.note_backpressure_rejection();
            reply.backpressure = true;
        }
        co_return reply;
    }

private:
    absl::flat_hash_map<model::topic_partition, kafka::offset>
      _last_added_offsets;

    fake_test_ctx& _test_ctx;
};

class fake_translation_ctx : public translation_context {
public:
    explicit fake_translation_ctx(fake_test_ctx& test_ctx)
      : _test_ctx(test_ctx) {}

    ~fake_translation_ctx() {
        vassert(
          !_inflight_translation,
          "finish() must be called in all cases for cleanup");
    }

    ss::future<> translate_now(
      model::record_batch_reader rdr, kafka::offset, ss::abort_source&) final {
        _test_ctx.translation_attempt();
        _inflight_translation = true;
        auto batches = co_await model::consume_reader_to_fragmented_memory(
          std::move(rdr), model::timeout_clock::now() + 10s);
        if (_test_ctx.error_on_translation()) {
            throw std::runtime_error("translation error simulation");
        }
        if (batches.empty()) {
            co_return;
        }
        auto min = model::offset_cast(batches.begin()->base_offset());
        auto max = model::offset_cast(batches.back().last_offset());
        for (auto& batch : batches) {
            _translated_bytes += batch.size_bytes();
        }
        // keep track of translation boundaries for result propagation
        _min_offset_translated = std::min(
          min, _min_offset_translated.value_or(kafka::offset::max()));
        _max_offset_translated = std::max(
          max, _max_offset_translated.value_or(kafka::offset::min()));
    }

    std::optional<kafka::offset> last_translated_offset() const final {
        return _max_offset_translated;
    }

    size_t flushed_bytes() const final { return _flushed_bytes; }

    ss::future<> flush() final {
        if (_test_ctx.error_on_flush()) {
            return ss::make_exception_future(
              std::runtime_error("flush error simulation"));
        }
        _flushed_bytes += _translated_bytes;
        _translated_bytes = 0;
        return ss::make_ready_future();
    }

    ss::future<
      checked<datalake::coordinator::translated_offset_range, translation_errc>>
    finish(retry_chain_node&, ss::abort_source&) final {
        _inflight_translation = false;
        translated_offset_range result;
        if (_min_offset_translated && _max_offset_translated) {
            result.start_offset = _min_offset_translated.value();
            result.last_offset = _max_offset_translated.value();
        }
        data_file file;
        file.file_size_bytes = _flushed_bytes;
        result.files.push_back(std::move(file));
        _flushed_bytes = 0;
        _min_offset_translated.reset();
        _max_offset_translated.reset();
        co_return result;
    }

    ss::future<> discard() final {
        _inflight_translation = false;
        _flushed_bytes = 0;
        _min_offset_translated.reset();
        _max_offset_translated.reset();
        return ss::make_ready_future();
    }

    size_t buffered_bytes() const final { return _buffered_bytes; }

    void report_translation_lag(int64_t) final {}

    void report_commit_lag(int64_t) final {}

    void report_backpressure_backoff() final {}

private:
    size_t _translated_bytes{0};
    size_t _flushed_bytes{0};
    std::optional<kafka::offset> _min_offset_translated;
    std::optional<kafka::offset> _max_offset_translated;
    fake_test_ctx& _test_ctx;
    bool _inflight_translation{false};
    size_t _buffered_bytes{0};
};

class fake_lag_tracker : public translation_lag_tracker {
public:
    explicit fake_lag_tracker(fake_test_ctx& test_ctx)
      : _test_ctx(test_ctx) {}

    bool should_finish_inflight_translation() final {
        return _test_ctx.should_finish_inflight_translation();
    }

    std::chrono::milliseconds current_lag_ms() const final {
        return std::chrono::milliseconds(0);
    }

    virtual void notify_new_data_for_translation(kafka::offset){

    };

    virtual void notify_data_translated(kafka::offset){};

    virtual void
      notify_inflight_translation_iteration(std::optional<kafka::offset>){};

    virtual std::optional<model::timestamp>
    get_translated_offset_timestamp_estimate(kafka::offset) {
        return model::timestamp::now();
    };

    /**
     * Returns an estimate size of data that are ready to be translated.
     */

    std::chrono::milliseconds target_lag() const final {
        return _test_ctx.target_lag();
    }

    std::optional<size_t> translation_backlog() const final {
        return _test_ctx.translation_backlog();
    }

    scheduling::clock::time_point next_checkpoint_deadline() const final {
        return scheduling::clock::now();
    }

private:
    fake_test_ctx& _test_ctx;
};

class partition_translator_fixture : public scheduling::scheduler_fixture {
public:
    ss::future<> SetUpAsync() override {
        co_await scheduling::scheduler_fixture::SetUpAsync();
    }

    std::unique_ptr<scheduling::scheduling_policy>
    make_scheduling_policy() override {
        return scheduling::scheduling_policy::make_default(
          config::mock_binding(max_concurrent_translators), task_quota);
    }

protected:
    ss::future<bool> add_translator(fake_test_ctx& ctx) {
        return _scheduler->add_translator(
          std::make_unique<partition_translator>(
            test_sg,
            std::make_unique<fake_coordinator_api>(ctx),
            std::make_unique<fake_data_src>(ctx),
            std::make_unique<fake_translation_ctx>(ctx),
            std::make_unique<fake_lag_tracker>(ctx),
            simple_time_jitter<ss::lowres_clock, std::chrono::milliseconds>{
              retry_max_timeout, retry_initial_backoff}));
    }

    fake_test_ctx& make_test_context() {
        _test_ctxs.emplace_back(
          next_ntp(), model::revision_id{0}, model::term_id{0});
        return _test_ctxs.back();
    }

private:
    static constexpr auto task_quota
      = std::chrono::duration_cast<scheduling::clock::duration>(2ms);

    chunked_vector<fake_test_ctx> _test_ctxs;
};

TEST_F_CORO(partition_translator_fixture, test_cleanup_on_translation_error) {
    auto& test_ctx = make_test_context();
    test_ctx.set_error_on_translation(true);
    co_await add_translator(test_ctx);
    co_await test_ctx.wait_for_translation_attempts(5);
    // Ensure translation does not make any progress
    ASSERT_LT_CORO(test_ctx.max_translated_offset(), kafka::offset{0});
    // undo
    test_ctx.set_error_on_translation(false);
    co_await test_ctx.wait_for_translation_attempts(5);
    ASSERT_GT_CORO(test_ctx.max_translated_offset(), kafka::offset{0});
}

TEST_F_CORO(partition_translator_fixture, test_cleanup_on_flush_error) {
    auto& test_ctx = make_test_context();
    test_ctx.set_error_on_flush(true);
    co_await add_translator(test_ctx);
    co_await test_ctx.wait_for_translation_attempts(5);
    // Ensure translation does not make any progress
    ASSERT_LT_CORO(test_ctx.max_translated_offset(), kafka::offset{0});
    // undo
    test_ctx.set_error_on_flush(false);
    co_await test_ctx.wait_for_translation_attempts(5);
    ASSERT_GT_CORO(test_ctx.max_translated_offset(), kafka::offset{0});
}

TEST_F_CORO(partition_translator_fixture, test_coordinator_retries) {
    auto& test_ctx = make_test_context();
    // simulate no leader conditions
    test_ctx.set_error_on_add_translated_files(true);
    co_await add_translator(test_ctx);
    // Ensure translation does not make any progress
    ASSERT_LT_CORO(test_ctx.max_translated_offset(), kafka::offset{0});
    test_ctx.set_error_on_add_translated_files(false);
    co_await test_ctx.wait_for_translation_attempts(10);
    ASSERT_GT_CORO(test_ctx.max_translated_offset(), kafka::offset{0});
}

TEST_F_CORO(partition_translator_fixture, test_coordinator_backpressure) {
    auto& test_ctx = make_test_context();
    test_ctx.set_coordinator_backpressure(true);
    co_await add_translator(test_ctx);

    // Wait until the translator has actually been rejected a few times, so we
    // know it polled and backed off rather than simply not having started yet.
    co_await test_ctx.wait_for_backpressure_rejections(3);
    ASSERT_LT_CORO(test_ctx.max_translated_offset(), kafka::offset{0});

    // Once the coordinator stops shedding load, translation resumes.
    test_ctx.set_coordinator_backpressure(false);
    co_await test_ctx.wait_for_translation_attempts(10);
    ASSERT_GT_CORO(test_ctx.max_translated_offset(), kafka::offset{0});
}

TEST_F_CORO(partition_translator_fixture, test_batching) {
    auto& test_ctx = make_test_context();
    // Let the translator batch to the limit
    test_ctx.set_should_finish_inflight_translation(false);
    co_await add_translator(test_ctx);

    auto stop = false;
    auto produce_f = ss::do_until(
      [&stop] { return stop; },
      [&test_ctx]() {
          auto current = test_ctx.max_translatable_offset();
          test_ctx.set_max_translatable_offset(
            current + kafka::offset_delta(10000));
          return ss::sleep(1ms);
      });

    std::exception_ptr ex = nullptr;
    try {
        RPTEST_REQUIRE_EVENTUALLY_CORO(20s, [&test_ctx]() {
            const auto& files = test_ctx.translated_files();
            const auto it = files.find(test_ctx.ntp().tp);
            if (it != files.end()) {
                vlog(test_logger.trace, "translated files: {}", it->second);
            }
            return it != files.end() && it->second.size() >= 2
                   && std::ranges::all_of(it->second, [](const auto& entry) {
                          return entry.file_size_bytes >= 32_MiB;
                      });
        });
    } catch (...) {
        ex = std::current_exception();
    }
    stop = true;
    co_await std::move(produce_f);
    if (ex) {
        RPTEST_FAIL_CORO(fmt::to_string(ex));
    }
}

using scheduling::scheduler_fixture;

TEST_F_CORO(scheduler_fixture, test_writer_reservations_accounting) {
    auto& reservations = *_scheduler->reservations();
    {
        translator_mem_tracker writer{reservations};

        auto cleanup = ss::defer([&writer] { writer.release(); });

        ss::abort_source as;

        size_t total_usage = 0;
        ASSERT_EQ_CORO(writer.current_usage(), total_usage);
        ASSERT_EQ_CORO(writer.total_reserved(), 0);

        auto bytes = 1_MiB;
        // update initial usage, should result in a reservation
        co_await writer.reserve_bytes(bytes, as);
        total_usage += bytes;

        ASSERT_EQ_CORO(writer.current_usage(), total_usage);
        ASSERT_EQ_CORO(writer.total_reserved(), block_size);

        // try 3 more times, we are still within the block limit;
        while (total_usage < block_size) {
            co_await writer.reserve_bytes(bytes, as);
            total_usage += bytes;
        }

        ASSERT_EQ_CORO(writer.current_usage(), block_size);
        ASSERT_EQ_CORO(writer.total_reserved(), block_size);

        // update again, should reserve a new block.
        co_await writer.reserve_bytes(bytes, as);
        total_usage += bytes;

        ASSERT_EQ_CORO(writer.current_usage(), total_usage);
        ASSERT_EQ_CORO(writer.total_reserved(), 2 * block_size);

        // exhaust all memory
        while (writer.current_usage() != total_memory) {
            co_await writer.reserve_bytes(bytes, as);
            total_usage += 1_MiB;
        }

        ss::promise<> done;
        // update again, should block on reservations due to memory limit
        // exhaustion.
        auto f = ss::with_timeout(
          ss::steady_clock_type::now() + 500ms,
          writer.reserve_bytes(bytes, as).finally(
            [&done] { done.set_value(); }));

        ASSERT_THROW_CORO(co_await std::move(f), ss::timed_out_error);

        as.request_abort();
        co_await std::move(done).get_future();
    }
    ASSERT_EQ_CORO(reservations.allocated_memory(), 0);
}
