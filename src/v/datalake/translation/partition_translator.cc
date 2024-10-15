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

#include "cluster/partition.h"
#include "datalake/batching_parquet_writer.h"
#include "datalake/coordinator/frontend.h"
#include "datalake/coordinator/translated_offset_range.h"
#include "datalake/data_writer_interface.h"
#include "datalake/logger.h"
#include "datalake/record_multiplexer.h"
#include "datalake/translation/state_machine.h"
#include "kafka/server/partition_proxy.h"
#include "kafka/utils/txn_reader.h"
#include "resource_mgmt/io_priority.h"
#include "ssx/future-util.h"

namespace datalake::translation {

static constexpr std::chrono::milliseconds translation_jitter{500};
static constexpr int max_attempts = 5;
constexpr ::model::timeout_clock::duration wait_timeout = 5s;
constexpr size_t max_rows_per_row_group = std::numeric_limits<size_t>::max();
constexpr size_t max_bytes_per_row_group = std::numeric_limits<size_t>::max();

partition_translator::partition_translator(
  ss::lw_shared_ptr<cluster::partition> partition,
  ss::sharded<coordinator::frontend>* frontend,
  ss::sharded<features::feature_table>* features,
  std::chrono::milliseconds translation_interval,
  ss::scheduling_group sg,
  size_t reader_max_bytes,
  ssx::semaphore& parallel_translations)
  : _term(partition->raft()->term())
  , _partition(std::move(partition))
  , _frontend(frontend)
  , _features(features)
  , _jitter{translation_interval, translation_jitter}
  , _max_bytes_per_reader(reader_max_bytes)
  , _parallel_translations(parallel_translations)
  , _writer_scratch_space(std::filesystem::temp_directory_path())
  , _logger(prefix_logger{
      datalake_log, fmt::format("{}-term-{}", _partition->ntp(), _term)}) {
    _stm = _partition->raft()
             ->stm_manager()
             ->get<datalake::translation::translation_stm>();
    vassert(_stm, "No translation stm found for {}", _partition->ntp());
    ssx::repeat_until_gate_closed(_gate, [this, sg] {
        return ss::with_scheduling_group(sg, [this] { return do_translate(); });
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
    auto min_offset = _partition->raft_start_offset();
    return model::offset_cast(
      _partition->get_offset_translator_state()->from_log_offset(min_offset));
}

kafka::offset partition_translator::max_offset_for_translation() const {
    // We factor in LSO to ensure only committed transactional batches are
    // translated and we clamp it to committed offset to avoid translation
    // of majorty appended but not flushed data (eg: acks=0/1/write_caching).
    auto max_log_offset = std::min(
      model::prev_offset(_partition->last_stable_offset()),
      _partition->committed_offset());
    return model::offset_cast(
      _partition->get_offset_translator_state()->from_log_offset(
        max_log_offset));
}

ss::future<std::optional<coordinator::translated_offset_range>>
partition_translator::do_translation_for_range(
  kafka::read_committed_reader rdr) {
    // This configuration only writes a single row group per file but we limit
    // the bytes via the reader max_bytes.
    auto writer_factory = std::make_unique<batching_parquet_writer_factory>(
      _writer_scratch_space,
      fmt::format("rp-parquet-translator-term-{}", _term),
      max_rows_per_row_group,
      max_bytes_per_row_group);
    auto multiplexer = datalake::record_multiplexer(std::move(writer_factory));
    auto result = co_await std::move(rdr).consume(
      std::move(multiplexer), model::no_timeout);
    if (result.has_error()) {
        vlog(_logger.warn, "Error translating range {}", result.error());
        co_return std::nullopt;
    }
    co_return std::move(result.value());
}

ss::future<partition_translator::translation_success>
partition_translator::do_translate_once() {
    if (
      !_partition->get_ntp_config().iceberg_enabled()
      || !_features->local().is_active(features::feature::datalake_iceberg)) {
        co_return translation_success::no;
    }
    auto reconcile_result = co_await reconcile_with_coordinator();
    if (!reconcile_result) {
        vlog(_logger.warn, "reconciliation with coordinator failed");
        co_return translation_success::no;
    }
    vlog(_logger.trace, "reconciliation result: {}", reconcile_result.value());
    auto read_begin_offset = kafka::next_offset(reconcile_result.value());
    auto read_end_offset = max_offset_for_translation();
    if (read_begin_offset >= read_end_offset) {
        vlog(
          _logger.trace,
          "translation up to date until kafka offset: {}, lso: {}, nothing to "
          "do",
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
    auto proxy = kafka::make_partition_proxy(_partition);
    auto log_reader = co_await proxy.make_reader(
      {kafka::offset_cast(read_begin_offset),
       kafka::offset_cast(read_end_offset),
       0,
       _max_bytes_per_reader,
       datalake_priority(),
       std::nullopt,
       std::nullopt,
       _as});

    vlog(
      _logger.debug,
      "translating data in kafka range: [{}, {}]",
      read_begin_offset,
      read_end_offset);
    auto tracker = kafka::aborted_transaction_tracker::create_default(
      &proxy, std::move(log_reader.ot_state));
    auto kafka_reader = kafka::read_committed_reader(
      std::move(tracker), std::move(log_reader.reader));
    auto units = co_await ss::get_units(_parallel_translations, 1, _as);
    auto translation_result = co_await do_translation_for_range(
      std::move(kafka_reader));
    units.return_all();
    if (
      translation_result
      && co_await checkpoint_translated_data(
        std::move(translation_result.value()))) {
        co_return translation_success::yes;
    }
    co_return translation_success::no;
}

ss::future<partition_translator::checkpoint_result>
partition_translator::checkpoint_translated_data(
  coordinator::translated_offset_range translated_range) {
    if (translated_range.files.empty()) {
        co_return checkpoint_result::yes;
    }
    auto last_offset = translated_range.last_offset;
    coordinator::add_translated_data_files_request request;
    request.tp = _partition->ntp().tp;
    request.translator_term = _term;
    request.ranges.emplace_back(std::move(translated_range));
    vlog(_logger.trace, "Adding tranlsated data file, request: {}", request);
    // There is a separate retry policy to handle
    // transient errors like coordinator readership changes.
    backoff retry_policy{2000ms};
    auto result = co_await retry_policy.retry(
      max_attempts,
      [this, request = std::move(request)] {
          return _frontend->local().add_translated_data_files(request.copy());
      },
      [this](coordinator::add_translated_data_files_reply result) {
          return can_continue() && is_retriable(result.errc);
      });
    vlog(_logger.trace, "Adding tranlsated data file, result: {}", result);
    co_return result.errc == coordinator::errc::ok
      && !(co_await _stm->sync_with_coordinator(
        last_offset, _term, wait_timeout, _as));
}

ss::future<std::optional<kafka::offset>>
partition_translator::reconcile_with_coordinator() {
    if (_reconcile) {
        auto request = coordinator::fetch_latest_data_file_request{};
        request.tp = _partition->ntp().tp;
        vlog(_logger.trace, "fetch_latest_data_file, request: {}", request);
        auto resp = co_await _frontend->local().fetch_latest_data_file(request);
        vlog(_logger.trace, "fetch_latest_data_file, response: {}", resp);
        if (resp.errc != coordinator::errc::ok) {
            co_return std::nullopt;
        }
        // No file entry signifies the translation was just enabled on the
        // topic. In such a case we start translation from the local start
        // of the log. The underlying assumption is that there is a reasonable
        // retention policy in place that clamps how big the local log is.
        auto sync_offset = resp.last_added_offset
                             ? resp.last_added_offset.value()
                             : kafka::prev_offset(min_offset_for_translation());
        auto sync_error = co_await _stm->sync_with_coordinator(
          sync_offset, _term, wait_timeout, _as);
        if (sync_error) {
            co_return std::nullopt;
        }
    }
    // Reconciliation is an optimization to avoid wasteful
    // translation work (particularly on term changes). Worst case
    // we do translation work on a stale offset range which then gets
    // rejected on the coordinator checkpoint and we are forced to reconcile
    // again. Here we try to avoid round trips to the coordinator when
    // the translator thinks it is not stale.
    co_return co_await _stm->highest_translated_offset(wait_timeout);
}

bool partition_translator::can_continue() const {
    return !_as.abort_requested() && _term == _partition->raft()->term();
}

ss::future<> partition_translator::do_translate() {
    while (can_continue()) {
        co_await ss::sleep_abortable(_jitter.next_duration(), _as);
        backoff retry_policy{_jitter.base_duration()};
        auto result = co_await retry_policy.retry(
          max_attempts,
          [this] { return do_translate_once(); },
          [this](translation_success result) {
              auto retry = can_continue() && result == translation_success::no;
              if (retry) {
                  _reconcile = needs_reconciliation::yes;
              }
              return retry;
          });
        if (result) {
            _reconcile = needs_reconciliation::no;
        }
    }
}

} // namespace datalake::translation
