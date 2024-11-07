/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/translation/partition_translator.h"

#include "cluster/archival/types.h"
#include "cluster/partition.h"
#include "datalake/batching_parquet_writer.h"
#include "datalake/coordinator/frontend.h"
#include "datalake/coordinator/translated_offset_range.h"
#include "datalake/data_writer_interface.h"
#include "datalake/logger.h"
#include "datalake/record_multiplexer.h"
#include "datalake/translation/state_machine.h"
#include "datalake/translation_task.h"
#include "kafka/utils/txn_reader.h"
#include "resource_mgmt/io_priority.h"
#include "ssx/future-util.h"
#include "utils/lazy_abort_source.h"

#include <seastar/coroutine/as_future.hh>

namespace datalake::translation {

namespace {

// A simple utility to conditionally retry with backoff on failures.
static constexpr std::chrono::milliseconds initial_backoff{300};
static constexpr std::chrono::milliseconds max_translation_task_timeout{3min};
static constexpr std::string_view iceberg_file_path_prefix = "datalake-iceberg";
template<
  typename Func,
  typename ShouldRetry,
  typename FuncRet = std::invoke_result_t<Func>,
  typename RetValueType = ss::futurize<FuncRet>::value_type>
requires std::predicate<ShouldRetry, RetValueType>
ss::futurize_t<FuncRet> retry_with_backoff(
  retry_chain_node& parent_rcn, Func&& f, ShouldRetry&& should_retry) {
    parent_rcn.check_abort();
    auto rcn = retry_chain_node(&parent_rcn);
    while (true) {
        auto result_f = co_await ss::coroutine::as_future<RetValueType>(
          ss::futurize_invoke(f));
        auto failed = result_f.failed();
        // eagerly take the exception out to avoid ignored exceptional
        // futures as retry() below can throw.
        std::exception_ptr ex = failed ? result_f.get_exception() : nullptr;
        auto retry = rcn.retry();
        if (!retry.is_allowed) {
            // No more retries allowed, propagated whatever we have.
            if (failed) {
                vassert(
                  ex != nullptr,
                  "Invalid exception, should be non null on a failed future.");
                std::rethrow_exception(ex);
            }
            co_return result_f.get();
        }
        // Further retries are allowed, check for exceptions if any.
        if (!failed) {
            auto result = result_f.get();
            if (!should_retry(result)) {
                co_return result;
            }
        }
        co_await ss::sleep_abortable(retry.delay, *retry.abort_source);
    }
}

} // namespace

static constexpr std::chrono::milliseconds translation_jitter{500};
constexpr ::model::timeout_clock::duration wait_timeout = 5s;
constexpr size_t max_rows_per_row_group = std::numeric_limits<size_t>::max();
constexpr size_t max_bytes_per_row_group = std::numeric_limits<size_t>::max();

partition_translator::partition_translator(
  ss::lw_shared_ptr<cluster::partition> partition,
  ss::sharded<coordinator::frontend>* frontend,
  ss::sharded<features::feature_table>* features,
  std::unique_ptr<cloud_data_io>* cloud_io,
  schema_manager* schema_mgr,
  type_resolver* type_resolver,
  std::chrono::milliseconds translation_interval,
  ss::scheduling_group sg,
  size_t reader_max_bytes,
  std::unique_ptr<ssx::semaphore>* parallel_translations)
  : _term(partition->raft()->term())
  , _partition(std::move(partition))
  , _stm(_partition->raft()
           ->stm_manager()
           ->get<datalake::translation::translation_stm>())
  , _frontend(frontend)
  , _features(features)
  , _cloud_io(cloud_io)
  , _schema_mgr(schema_mgr)
  , _type_resolver(type_resolver)
  , _partition_proxy(std::make_unique<kafka::partition_proxy>(
      kafka::make_partition_proxy(_partition)))
  , _jitter{translation_interval, translation_jitter}
  , _max_bytes_per_reader(reader_max_bytes)
  , _parallel_translations(parallel_translations)
  , _writer_scratch_space(std::filesystem::temp_directory_path())
  , _logger(prefix_logger{
      datalake_log, fmt::format("{}-term-{}", _partition->ntp(), _term)}) {
    vassert(_stm, "No translation stm found for {}", _partition->ntp());
    start_translation_in_background(sg);
}

void partition_translator::start_translation_in_background(
  ss::scheduling_group sg) {
    ssx::repeat_until_gate_closed_or_aborted(_gate, _as, [this, sg] {
        return ss::with_scheduling_group(sg, [this] {
            return do_translate().handle_exception(
              [this](const std::exception_ptr& e) {
                  if (!ssx::is_shutdown_exception(e)) {
                      vlog(
                        _logger.warn,
                        "ignoring exception during translation : {}",
                        e);
                  }
              });
        });
    });
    vlog(_logger.debug, "started partition translator in term {}", _term);
}

std::chrono::milliseconds partition_translator::translation_interval() const {
    return _jitter.base_duration();
}

void partition_translator::reset_translation_interval(
  std::chrono::milliseconds new_base) {
    _jitter = jitter_t{new_base, translation_jitter};
    vlog(
      _logger.info,
      "Iceberg translation interval reset to: {}",
      _jitter.base_duration());
}

ss::future<> partition_translator::stop() {
    vlog(_logger.debug, "stopping partition translator in term {}", _term);
    auto f = _gate.close();
    _as.request_abort();
    co_await std::move(f);
}

kafka::offset partition_translator::min_offset_for_translation() const {
    return model::offset_cast(_partition_proxy->local_start_offset());
}

std::optional<kafka::offset>
partition_translator::max_offset_for_translation() const {
    // We factor in LSO to ensure only committed transactional batches are
    // translated.
    auto lso = _partition_proxy->last_stable_offset();
    if (lso.has_error()) {
        vlog(
          _logger.warn,
          "unable to compute lso for translation: {}",
          lso.error());
        return std::nullopt;
    }
    return kafka::prev_offset(model::offset_cast(lso.value()));
}

ss::future<std::optional<coordinator::translated_offset_range>>
partition_translator::do_translation_for_range(
  retry_chain_node& parent, model::record_batch_reader rdr) {
    // This configuration only writes a single row group per file but we limit
    // the bytes via the reader max_bytes.
    auto writer_factory = std::make_unique<batching_parquet_writer_factory>(
      local_path{_writer_scratch_space},
      "", // todo(iceberg): generate a prefix with offset information
      max_rows_per_row_group,
      max_bytes_per_row_group);
    auto task = translation_task{**_cloud_io, *_schema_mgr, *_type_resolver};
    const auto& ntp = _partition->ntp();
    auto remote_path_prefix = remote_path{
      fmt::format("{}/{}/{}", iceberg_file_path_prefix, ntp.path(), _term)};
    lazy_abort_source las{[this] {
        return can_continue() ? std::nullopt
                              : std::make_optional("translator stopping");
    }};
    auto result = co_await task.translate(
      ntp,
      std::move(writer_factory),
      std::move(rdr),
      remote_path_prefix,
      parent,
      las);
    if (result.has_error()) {
        vlog(_logger.warn, "Error translating range {}", result.error());
        co_return std::nullopt;
    }
    co_return std::move(result.value());
}

ss::future<partition_translator::translation_success>
partition_translator::do_translate_once(retry_chain_node& parent_rcn) {
    if (
      !_partition->get_ntp_config().iceberg_enabled()
      || !_features->local().is_active(features::feature::datalake_iceberg)) {
        vlog(_logger.debug, "iceberg config/feature disabled, nothing to do.");
        co_return translation_success::yes;
    }
    auto reconcile_result = co_await reconcile_with_coordinator();
    if (!reconcile_result) {
        vlog(_logger.warn, "reconciliation with coordinator failed");
        co_return translation_success::no;
    }
    vlog(_logger.trace, "reconciliation result: {}", reconcile_result.value());
    auto read_begin_offset = kafka::next_offset(reconcile_result.value());
    auto max_offset = max_offset_for_translation();
    if (!max_offset) {
        co_return translation_success::no;
    }
    auto read_end_offset = max_offset.value();
    if (read_end_offset < read_begin_offset) {
        vlog(
          _logger.debug,
          "translation up to date, next begin : {}, max readable kafka offset: "
          "{}, log lso: {}, nothing to do.",
          read_begin_offset,
          read_end_offset,
          _partition->last_stable_offset());
        co_return translation_success::yes;
    }
    // We have some data to translate, make a reader
    // and dispatch to the iceberg translator
    //
    // todo: what if we have too little to read, eg: slow moving topic.
    // Currently we rely on user setting the translation interval
    // appropriately to ensure each attempt has something substantial
    // to read. We can add some heuristics to estimate the size and
    // backoff if it is too small and wait for enough data to
    // accumulate. The resulting parquet files are only performant
    // if there is a big chunk of data in them. Smaller parquet files
    // are an overhead for iceberg metadata.
    auto log_reader = co_await _partition_proxy->make_reader(
      {kafka::offset_cast(read_begin_offset),
       kafka::offset_cast(read_end_offset),
       0,
       _max_bytes_per_reader,
       datalake_priority(),
       std::nullopt,
       std::nullopt,
       _as});

    auto units = co_await ss::get_units(**_parallel_translations, 1, _as);
    vlog(
      _logger.debug,
      "translating data in kafka range: [{}, {}]",
      read_begin_offset,
      read_end_offset);
    auto tracker = kafka::aborted_transaction_tracker::create_default(
      _partition_proxy.get(), std::move(log_reader.ot_state));
    auto kafka_reader
      = model::make_record_batch_reader<kafka::read_committed_reader>(
        std::move(tracker), std::move(log_reader.reader));
    auto translation_result = co_await do_translation_for_range(
      parent_rcn, std::move(kafka_reader));
    units.return_all();
    vlog(_logger.debug, "translation result: {}", translation_result);
    units.return_all();
    if (
      translation_result
      && co_await checkpoint_translated_data(
        parent_rcn, std::move(translation_result.value()))) {
        co_return translation_success::yes;
    }
    co_return translation_success::no;
}

ss::future<partition_translator::checkpoint_result>
partition_translator::checkpoint_translated_data(
  retry_chain_node& rcn,
  coordinator::translated_offset_range translated_range) {
    if (translated_range.files.empty()) {
        co_return checkpoint_result::yes;
    }
    auto last_offset = translated_range.last_offset;
    coordinator::add_translated_data_files_request request;
    request.tp = _partition->ntp().tp;
    request.translator_term = _term;
    request.ranges.emplace_back(std::move(translated_range));
    vlog(_logger.trace, "Adding translated data file, request: {}", request);
    auto result = co_await retry_with_backoff(
      rcn,
      [this, request = std::move(request)] {
          return _frontend->local().add_translated_data_files(request.copy());
      },
      [this](coordinator::add_translated_data_files_reply result) {
          return can_continue() && is_retriable(result.errc);
      });
    vlog(_logger.trace, "Adding translated data file, result: {}", result);
    co_return result.errc == coordinator::errc::ok
      && !(co_await _stm->reset_highest_translated_offset(
        last_offset, _term, wait_timeout, _as));
}

ss::future<std::optional<kafka::offset>>
partition_translator::reconcile_with_coordinator() {
    auto request = coordinator::fetch_latest_translated_offset_request{};
    request.tp = _partition->ntp().tp;
    vlog(_logger.trace, "fetch_latest_translated_offset, request: {}", request);
    auto resp = co_await _frontend->local().fetch_latest_translated_offset(
      request);
    vlog(_logger.trace, "fetch_latest_translated_offset, response: {}", resp);
    if (resp.errc != coordinator::errc::ok) {
        vlog(_logger.warn, "reconciliation failed, response: {}", resp);
        co_return std::nullopt;
    }
    // No file entry signifies the translation was just enabled on the
    // topic. In such a case we start translation from the local start
    // of the log. The underlying assumption is that there is a reasonable
    // retention policy in place that clamps how big the local log is.
    auto translated_offset = resp.last_added_offset
                               ? resp.last_added_offset.value()
                               : kafka::prev_offset(
                                   min_offset_for_translation());
    auto reset_error = co_await _stm->reset_highest_translated_offset(
      translated_offset, _term, wait_timeout, _as);
    if (reset_error) {
        co_return std::nullopt;
    }
    co_return co_await _stm->highest_translated_offset(wait_timeout);
}

bool partition_translator::can_continue() const {
    return !_as.abort_requested() && _term == _partition->raft()->term();
}

ss::future<> partition_translator::do_translate() {
    while (can_continue()) {
        co_await ss::sleep_abortable(_jitter.next_duration(), _as);
        retry_chain_node rcn{
          _as, max_translation_task_timeout, initial_backoff};
        co_await retry_with_backoff(
          rcn,
          [this, &rcn] { return do_translate_once(rcn); },
          [this](translation_success result) {
              return can_continue() && result == translation_success::no;
          });
    }
}

} // namespace datalake::translation
