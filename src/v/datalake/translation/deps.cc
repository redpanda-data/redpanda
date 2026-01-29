/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/translation/deps.h"

#include "cluster/notification.h"
#include "cluster/partition.h"
#include "datalake/coordinator/frontend.h"
#include "datalake/local_parquet_file_writer.h"
#include "datalake/logger.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/record_translator.h"
#include "datalake/serde_parquet_writer.h"
#include "datalake/translation/state_machine.h"
#include "datalake/translation/utils.h"
#include "datalake/translation_task.h"
#include "kafka/data/partition_proxy.h"
#include "kafka/utils/txn_reader.h"
#include "utils/human.h"

#include <seastar/util/defer.hh>

namespace {
datalake::translation::translation_errc
map_error_code(datalake::translation_task::errc errc) {
    using datalake::translation::translation_errc;
    switch (errc) {
    case datalake::translation_task::errc::file_io_error:
        return translation_errc::file_io_error;
    case datalake::translation_task::errc::cloud_io_error:
        return translation_errc::cloud_io_error;
    case datalake::translation_task::errc::flush_error:
        return translation_errc::flush_error;
    case datalake::translation_task::errc::no_data:
        return translation_errc::no_data;
    case datalake::translation_task::errc::oom_error:
        return translation_errc::oom_error;
    case datalake::translation_task::errc::time_limit_exceeded:
        return translation_errc::time_limit_exceeded;
    case datalake::translation_task::errc::shutting_down:
        return translation_errc::shutting_down;
    case datalake::translation_task::errc::out_of_disk:
        return translation_errc::out_of_disk;
    case datalake::translation_task::errc::type_resolution_error:
        return translation_errc::type_resolution_error;
    }
}
} // namespace

namespace datalake::translation {

namespace {
ss::future<cluster::errc> wait_stm_translated(
  ss::shared_ptr<translation_stm> stm,
  model::offset o,
  model::timeout_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    try {
        co_await stm->wait_translated(model::prev_offset(o), deadline, as);
    } catch (const ss::abort_requested_exception&) {
        co_return cluster::errc::shutting_down;
    } catch (const ss::timed_out_error&) {
        co_return cluster::errc::timeout;
    }
    co_return cluster::errc::success;
}
} // namespace

ss::future<reservation_error>
noop_disk_tracker::reserve_bytes(size_t, ss::abort_source&) noexcept {
    return ss::make_ready_future<reservation_error>(reservation_error::ok);
}
ss::future<> noop_disk_tracker::free_bytes(size_t, ss::abort_source&) {
    return ss::make_ready_future<>();
}
void noop_disk_tracker::release() {}
void noop_disk_tracker::release_unused() {}

ss::future<reservation_error>
noop_mem_tracker::reserve_bytes(size_t, ss::abort_source&) noexcept {
    return ss::make_ready_future<reservation_error>(reservation_error::ok);
}
ss::future<> noop_mem_tracker::free_bytes(size_t, ss::abort_source&) {
    return ss::make_ready_future<>();
}
void noop_mem_tracker::release() {}
writer_disk_tracker& noop_mem_tracker::disk() { return _disk; }

ss::future<reservation_error> translator_mem_tracker::reserve_bytes(
  size_t bytes, ss::abort_source& as) noexcept {
    _current_usage += bytes;
    try {
        while (_current_usage > _reservations.count()) {
            // reserve any deficit
            auto reservation = co_await _reservations_tracker.reserve_memory(
              as);
            if (_reservations.count()) {
                _reservations.adopt(std::move(reservation));
            } else {
                _reservations = std::move(reservation);
            }
        }
    } catch (const translator_out_of_memory_error&) {
        co_return reservation_error::out_of_memory;
    } catch (const translator_shutdown_error&) {
        co_return reservation_error::shutting_down;
    } catch (const translator_time_quota_exceeded_error&) {
        co_return reservation_error::time_quota_exceeded;
    } catch (const translator_out_of_disk_error&) {
        co_return reservation_error::out_of_disk;
    } catch (...) {
        co_return reservation_error::unknown;
    }
    co_return reservation_error::ok;
}

ss::future<>
translator_mem_tracker::free_bytes(size_t bytes, ss::abort_source&) {
    _current_usage -= std::min(_current_usage, bytes);
    auto excess_reservation = total_reserved() - _current_usage;
    // In cases where the underlying translator decided to flush its buffered
    // memory to disk we want to avoid holding excess memory units.
    //
    // We do hold on one reservation block so that some meaningful progess can
    // still be made by the translator in cases where memory units are highly
    // contended.
    if (excess_reservation > _reservations_tracker.reservation_block_size()) {
        _reservations.return_units(
          excess_reservation - _reservations_tracker.reservation_block_size());
    }
    return ss::now();
}

void translator_mem_tracker::release() {
    _current_usage = 0;
    _reservations.return_all();
}

size_t translator_mem_tracker::current_usage() const { return _current_usage; }

size_t translator_mem_tracker::total_reserved() const {
    return _reservations.count();
}

writer_disk_tracker& translator_mem_tracker::disk() { return _disk; }

ss::future<reservation_error> translator_disk_tracker::reserve_bytes(
  size_t bytes, ss::abort_source& as) noexcept {
    _current_usage += bytes;
    try {
        while (_current_usage > _reservations.count()) {
            auto reservation = co_await _reservations_tracker.reserve_disk(
              bytes, as);
            if (_reservations.count()) {
                _reservations.adopt(std::move(reservation));
            } else {
                _reservations = std::move(reservation);
            }
        }
    } catch (const translator_out_of_memory_error&) {
        co_return reservation_error::out_of_memory;
    } catch (const translator_shutdown_error&) {
        co_return reservation_error::shutting_down;
    } catch (const translator_time_quota_exceeded_error&) {
        co_return reservation_error::time_quota_exceeded;
    } catch (const translator_out_of_disk_error&) {
        co_return reservation_error::out_of_disk;
    } catch (...) {
        co_return reservation_error::unknown;
    }
    co_return reservation_error::ok;
}
ss::future<>
translator_disk_tracker::free_bytes(size_t bytes, ss::abort_source&) {
    // we don't update the reservation here as the next time we call
    // reserve_bytes we'll have already reserved an excess amount
    _current_usage -= std::min(_current_usage, bytes);
    return ss::now();
}
void translator_disk_tracker::release() {
    _current_usage = 0;
    _reservations.return_all();
}
void translator_disk_tracker::release_unused() {
    if (_reservations.count() > _current_usage) {
        const auto units = _reservations.count() - _current_usage;
        _reservations.return_units(units);
        vlog(
          datalake_log.debug,
          "Releasing {} excess disk reservation. Current reserved {}",
          human::bytes(units),
          human::bytes(_reservations.count()));
    }
}

// Creates or alters the table by delegating to the coordinator.
class coordinator_table_creator final : public table_creator {
public:
    explicit coordinator_table_creator(coordinator::frontend& fe)
      : coordinator_fe_(fe) {}

    ss::future<checked<std::nullopt_t, errc>> ensure_table(
      const model::topic& topic,
      model::revision_id topic_revision,
      record_schema_components comps) const final {
        auto ensure_res = co_await coordinator_fe_.ensure_table_exists(
          coordinator::ensure_table_exists_request{
            topic,
            topic_revision,
            comps,
          });
        switch (ensure_res.errc) {
        case coordinator::errc::ok:
            co_return std::nullopt;
        case coordinator::errc::incompatible_schema:
            co_return errc::incompatible_schema;
        default:
            co_return errc::failed;
        }
    }

    ss::future<checked<std::nullopt_t, errc>> ensure_dlq_table(
      const model::topic& topic,
      const model::revision_id topic_revision) const final {
        auto ensure_res = co_await coordinator_fe_.ensure_dlq_table_exists(
          coordinator::ensure_dlq_table_exists_request{
            topic,
            topic_revision,
          });
        switch (ensure_res.errc) {
        case coordinator::errc::ok:
            co_return std::nullopt;
        case coordinator::errc::incompatible_schema:
            co_return errc::incompatible_schema;
        default:
            co_return errc::failed;
        }
    }

private:
    coordinator::frontend& coordinator_fe_;
};

std::unique_ptr<table_creator>
make_default_table_creator(coordinator::frontend& fe) {
    return std::make_unique<coordinator_table_creator>(fe);
}

class default_coordinator_api : public coordinator_api {
public:
    explicit default_coordinator_api(coordinator::frontend& frontend)
      : _frontend(frontend) {}

    ss::future<coordinator::add_translated_data_files_reply>
    add_translated_data_files(
      coordinator::add_translated_data_files_request request) final {
        return _frontend.add_translated_data_files(std::move(request));
    }

    ss::future<coordinator::fetch_latest_translated_offset_reply>
    fetch_latest_translated_offset(
      coordinator::fetch_latest_translated_offset_request request) final {
        return _frontend.fetch_latest_translated_offset(std::move(request));
    }

private:
    coordinator::frontend& _frontend;
};

std::unique_ptr<coordinator_api>
coordinator_api::make_default_coordinator_api(coordinator::frontend& frontend) {
    return std::make_unique<default_coordinator_api>(frontend);
}

namespace {
std::optional<kafka::offset> calculate_max_offset_for_translation(
  const kafka::partition_proxy& partition_proxy) {
    // We factor in LSO to ensure only committed transactional batches are
    // translated.
    auto lso = partition_proxy.last_stable_offset();
    if (lso.has_error()) {
        return std::nullopt;
    }
    return kafka::prev_offset(model::offset_cast(lso.value()));
}
std::chrono::milliseconds calculate_target_lag(
  const cluster::topic_table& topics, const model::ntp& ntp) {
    const auto& topic_cfg = topics.get_topic_cfg(
      model::topic_namespace_view{ntp});
    auto default_lag = config::shard_local_cfg().iceberg_target_lag_ms.value();
    if (!topic_cfg.has_value()) {
        return default_lag;
    }
    return topic_cfg->properties.iceberg_target_lag_ms.value_or(default_lag);
}
kafka::offset calculate_min_offset_for_translation(
  bool read_replica_mode, const kafka::partition_proxy& partition_proxy) {
    if (read_replica_mode) {
        return model::offset_cast(partition_proxy.start_offset());
    }
    return model::offset_cast(partition_proxy.local_start_offset());
}

struct timestamped_offset {
    explicit timestamped_offset(kafka::offset o)
      : timestamped_offset(o, model::timestamp::now()) {}
    timestamped_offset(kafka::offset o, model::timestamp ts)
      : offset(o)
      , ts(ts) {}
    kafka::offset offset;
    model::timestamp ts;

    friend std::ostream&
    operator<<(std::ostream& o, const timestamped_offset& to);
};
std::ostream& operator<<(std::ostream& o, const timestamped_offset& to) {
    fmt::print(o, "{{offset: {}, timestamp: {}}}", to.offset, to.ts);
    return o;
}

} // namespace

class partition_data_source : public data_source {
public:
    explicit partition_data_source(
      ss::lw_shared_ptr<cluster::partition> partition)
      : _partition(std::move(partition))
      , _stm(_partition->raft()->stm_manager()->get<translation_stm>())
      , _partition_proxy(
          std::make_unique<kafka::partition_proxy>(
            kafka::make_partition_proxy(_partition)))
      , _partition_flush_subscription(_partition->register_flush_hook(
          std::bind_front(&wait_stm_translated, _stm))) {}

    void close() noexcept final {
        _partition->unregister_flush_hook(_partition_flush_subscription);
    }

    const model::ntp& ntp() const final { return _partition->ntp(); }

    model::revision_id topic_revision() const final {
        return _partition->get_topic_revision_id();
    }

    model::term_id term() const final { return _partition->term(); }

    ss::future<std::optional<kafka::offset>> wait_for_data_to_translate(
      std::optional<kafka::offset> last_translated_offset,
      ss::lowres_clock::time_point deadline,
      ss::abort_source& as) final {
        // todo: add logic to wait for enough data to translate.
        // currently we just break even if a single batch of data is available
        constexpr auto poll_duration = 2s;
        while (!has_more_data_to_translate(last_translated_offset)) {
            co_await ss::sleep_abortable(poll_duration, as);
            if (ss::lowres_clock::now() >= deadline) {
                co_return std::nullopt;
            }
        }
        if (last_translated_offset) {
            co_return kafka::next_offset(last_translated_offset.value());
        }
        co_return min_offset_for_translation();
    }

    ss::future<std::optional<model::record_batch_reader>>
    make_log_reader(kafka::offset begin_offset, ss::abort_source& as) final {
        // Bump the reader start offset up to the log start. Untranslated data
        // should typically be pinned, so the input offset should be above the
        // log start in normal circumstances, but it's possible that e.g. data
        // was removed when Iceberg was disabled.
        auto reader_start_offset = std::max(
          begin_offset, model::offset_cast(_partition_proxy->start_offset()));
        auto max_translatable_offset = max_offset_for_translation();
        if (
          !max_translatable_offset
          || max_translatable_offset.value() < reader_start_offset) {
            co_return std::nullopt;
        }
        auto log_reader = co_await _partition_proxy->make_reader(
          {reader_start_offset,
           max_translatable_offset.value(),
           0,
           std::numeric_limits<size_t>::max(),
           std::nullopt,
           as});
        auto tracker = kafka::aborted_transaction_tracker::create_default(
          _partition_proxy.get(), std::move(log_reader.ot_state));
        co_return model::make_record_batch_reader<kafka::read_committed_reader>(
          std::move(tracker), std::move(log_reader.reader));
    }

    kafka::offset min_offset_for_translation() const final {
        auto highest_translated = _stm->cached_highest_translated_offset();
        if (highest_translated != kafka::offset::min()) {
            return kafka::next_offset(highest_translated);
        }
        return calculate_min_offset_for_translation(
          _partition->is_read_replica_mode_enabled(), *_partition_proxy);
    }

    std::optional<kafka::offset> max_offset_for_translation() const final {
        return calculate_max_offset_for_translation(*_partition_proxy);
    }

    ss::future<std::error_code> replicate_highest_translated_offset(
      kafka::offset new_offset,
      std::optional<model::timestamp> translation_timestamp,
      model::term_id term,
      model::timeout_clock::duration timeout,
      ss::abort_source& as) final {
        return _stm->reset_highest_translated_offset(
          new_offset, translation_timestamp, term, timeout, as);
    }

private:
    bool has_more_data_to_translate(
      std::optional<kafka::offset> last_translated_offset) {
        auto max_translatable_offset = max_offset_for_translation();
        if (!max_translatable_offset) {
            return false;
        }
        auto translated_offset = last_translated_offset.value_or(
          kafka::offset{});
        return max_translatable_offset.value() > translated_offset;
    }

    ss::lw_shared_ptr<cluster::partition> _partition;
    ss::shared_ptr<translation_stm> _stm;
    std::unique_ptr<kafka::partition_proxy> _partition_proxy;
    cluster::partition_flush_hook_id _partition_flush_subscription;
};

std::unique_ptr<data_source> data_source::make_default_data_source(
  ss::lw_shared_ptr<cluster::partition> partition) {
    return std::make_unique<partition_data_source>(std::move(partition));
}

std::ostream& operator<<(std::ostream& o, translation_errc ec) {
    switch (ec) {
    case no_data:
        return o << "translation_errc::no_data";
    case file_io_error:
        return o << "translation_errc::file_io_error";
    case cloud_io_error:
        return o << "translation_errc::cloud_io_error";
    case flush_error:
        return o << "translation_errc::flush_error";
    case discard_error:
        return o << "translation_errc::discard_error";
    case oom_error:
        return o << "translation_errc::oom_error";
    case time_limit_exceeded:
        return o << "translation_errc::time_limit_exceeded";
    case shutting_down:
        return o << "translation_errc::shutting_down";
    case out_of_disk:
        return o << "translation_errc::out_of_disk";
    case type_resolution_error:
        return o << "translation_errc::type_resolution_error";
    }
}

class partition_translation_context : public translation_context {
public:
    explicit partition_translation_context(
      local_path writer_scratch_space,
      const model::ntp& ntp,
      model::revision_id topic_revision,
      cloud_data_io& uploader,
      schema_manager& schema_mgr,
      std::unique_ptr<type_resolver> type_resolver,
      std::unique_ptr<record_translator> record_translator,
      std::unique_ptr<table_creator> table_creator,
      location_provider location_provider,
      scheduling::reservations_tracker& reservations,
      ss::sharded<cluster::topic_table>* topics,
      ss::sharded<features::feature_table>* features,
      ss::lw_shared_ptr<translation_probe> probe)
      : _writer_scratch_space(std::move(writer_scratch_space))
      , _ntp(ntp)
      , _topic_revision(topic_revision)
      , _cloud_io(uploader)
      , _schema_mgr(schema_mgr)
      , _type_resolver(std::move(type_resolver))
      , _record_translator(std::move(record_translator))
      , _table_creator(std::move(table_creator))
      , _location_provider(std::move(location_provider))
      , _reservations(reservations)
      , _topics(topics->local())
      , _features(features->local())
      , _probe(std::move(probe))
      , _invalid_record_action(compute_invalid_record_action())
      , _cp_enabled(
          translation_task::custom_partitioning_enabled{
            _features.is_active(features::feature::datalake_iceberg_ga)})
      , _mem_tracker(translator_mem_tracker{_reservations}) {}

    ss::future<> translate_now(
      model::record_batch_reader reader,
      kafka::offset start_offset,
      ss::abort_source& as) final {
        if (!_in_progress_translation) {
            _in_progress_translation.emplace(
              translation_task{
                _ntp,
                _topic_revision,
                make_writer_factory(),
                _cloud_io,
                &_features,
                _schema_mgr,
                *_type_resolver,
                *_record_translator,
                *_table_creator,
                _invalid_record_action,
                _location_provider,
                *_probe});
        }
        return _in_progress_translation->translate_once(
          std::move(reader), start_offset, as);
    }

    size_t flushed_bytes() const final {
        size_t result = 0;
        if (_in_progress_translation) {
            result = _in_progress_translation->flushed_bytes();
        }
        return result;
    }

    std::optional<kafka::offset> last_translated_offset() const final {
        return _in_progress_translation
                 ? _in_progress_translation->last_translated_offset()
                 : std::nullopt;
    }

    ss::future<> flush() final {
        if (_in_progress_translation) {
            // TODO: The flush here *does not* fully release memory associated
            // with the underlying file output stream because Seastar only
            // allows flush() on stream close(). The default buffer size is
            // 8KiB, this means up to 8KiB per writer can still be buffered even
            // after flush which is not accounted in reservations. This could be
            // an issue if there is an explosion of file writer instances. We
            // try to factor 10KiB overhead per writer, when it is created but
            // it will be released as soon as the flush is called. An
            // improvement could be to account for the fixed reservation cost
            // across flush calls and only release on finish.
            vlog(datalake_log.trace, "[{}] flushing writers", _ntp);
            return _in_progress_translation->flush()
              .then_wrapped([](auto result_f) {
                  if (result_f.failed()) {
                      return ss::make_exception_future(
                        result_f.get_exception());
                  }
                  auto result = result_f.get();
                  if (result.has_error()) {
                      return ss::make_exception_future(
                        std::runtime_error(
                          fmt::format(
                            "Error flushing in-progress translation: {}",
                            result.error())));
                  }
                  return ss::now();
              })
              .finally([this]() {
                  _mem_tracker.release();
                  // the translator finished a round of translation and may be
                  // taken out of the running state, so give back unused units.
                  _mem_tracker.disk().release_unused();
              });
        }
        return ss::now();
    }

    ss::future<checked<coordinator::translated_offset_range, translation_errc>>
    finish(retry_chain_node& rcn, ss::abort_source& as) final {
        // This is strictly not needed as flush() is always called after
        // every scheduler iteration but we do it just to be extra cautious.
        auto cleanup = ss::defer([this] {
            _mem_tracker.release();
            // staging data will be deleted via finish()
            _mem_tracker.disk().release();
        });
        if (!_in_progress_translation) {
            co_return translation_errc::no_data;
        }
        vlog(datalake_log.debug, "[{}] finishing translation", _ntp);
        auto task = std::exchange(_in_progress_translation, std::nullopt);
        auto result
          = co_await std::move(task.value()).finish(_cp_enabled, rcn, as);

        if (result.has_error()) {
            co_return map_error_code(result.error());
        }
        co_return std::move(result.value());
    }

    ss::future<> discard() final {
        // staging data will be deleted via discard()
        auto cleanup = ss::defer([this] { _mem_tracker.disk().release(); });
        if (!_in_progress_translation) {
            co_return;
        }
        auto task = std::exchange(_in_progress_translation, std::nullopt);
        co_await std::move(task.value()).discard().discard_result();
    }

    size_t buffered_bytes() const final { return _mem_tracker.current_usage(); }

    void report_translation_lag(int64_t new_lag) final {
        _probe->update_translation_offset_lag(new_lag);
    }

    void report_commit_lag(int64_t new_lag) final {
        _probe->update_commit_offset_lag(new_lag);
    }

private:
    scheduling::clock::duration compute_target_lag() const {
        // todo: In addition to integrating with config subsystem, an additional
        // task that needs to be done is to keep track of translation intervals
        // centrally some place (coordinator?). This is needed so that we do not
        // lose the interval boundaries due to a crash. For example imagine
        // keeping it in memory and target lag is set to 1d and the leadership
        // transfer happens after half a day, we cannot just reset the interval
        // window on the new leader. That probably works for smaller lag values
        // but has some oddness with large lags.
        return calculate_target_lag(_topics, _ntp);
    }

    std::unique_ptr<parquet_file_writer_factory> make_writer_factory() {
        return std::make_unique<local_parquet_file_writer_factory>(
          _writer_scratch_space, // storage temp files are written to
          fmt::to_string(_ntp.tp.partition), // file prefix
          ss::make_shared<serde_parquet_writer_factory>(),
          _mem_tracker);
    }

    model::iceberg_invalid_record_action compute_invalid_record_action() const {
        const auto& topic_cfg = _topics.get_topic_cfg(
          model::topic_namespace_view{_ntp});
        auto default_action
          = config::shard_local_cfg().iceberg_invalid_record_action.value();
        if (!topic_cfg) {
            return default_action;
        }
        return topic_cfg->properties.iceberg_invalid_record_action.value_or(
          default_action);
    }

    local_path _writer_scratch_space;
    const model::ntp& _ntp;
    model::revision_id _topic_revision;
    cloud_data_io& _cloud_io;
    schema_manager& _schema_mgr;
    std::unique_ptr<type_resolver> _type_resolver;
    std::unique_ptr<record_translator> _record_translator;
    std::unique_ptr<table_creator> _table_creator;
    location_provider _location_provider;
    scheduling::reservations_tracker& _reservations;
    cluster::topic_table& _topics;
    features::feature_table& _features;
    ss::lw_shared_ptr<translation_probe> _probe;
    model::iceberg_invalid_record_action _invalid_record_action;
    translation_task::custom_partitioning_enabled _cp_enabled;
    translator_mem_tracker _mem_tracker;
    std::optional<translation_task> _in_progress_translation;
};

std::unique_ptr<translation_context>
translation_context::make_default_translation_context(
  local_path writer_scratch_space,
  const model::ntp& ntp,
  model::revision_id topic_revision,
  cloud_data_io& uploader,
  schema_manager& schema_mgr,
  std::unique_ptr<type_resolver> type_resolver,
  std::unique_ptr<record_translator> record_translator,
  std::unique_ptr<table_creator> table_creator,
  location_provider location_provider,
  scheduling::reservations_tracker& reservations,
  ss::sharded<cluster::topic_table>* topics,
  ss::sharded<features::feature_table>* features,
  ss::lw_shared_ptr<translation_probe> probe) {
    return std::make_unique<partition_translation_context>(
      std::move(writer_scratch_space),
      ntp,
      topic_revision,
      uploader,
      schema_mgr,
      std::move(type_resolver),
      std::move(record_translator),
      std::move(table_creator),
      std::move(location_provider),
      reservations,
      topics,
      features,
      std::move(probe));
}

class partition_lag_tracker : public translation_lag_tracker {
public:
    explicit partition_lag_tracker(
      ss::lw_shared_ptr<cluster::partition> partition,
      cluster::topic_table& topics)
      : _partition(std::move(partition))
      , _partition_proxy(
          std::make_unique<kafka::partition_proxy>(
            kafka::make_partition_proxy(_partition)))
      , _topics(topics)
      , _stm(_partition->raft()->stm_manager()->get<translation_stm>()) {}

    bool should_finish_inflight_translation() final {
        // we finish inflight translation if the lag greater than 90% of the
        // target lag.
        return current_lag_ms() >= target_lag_goal();
    }

    std::chrono::milliseconds target_lag() const final {
        return calculate_target_lag(_topics, _partition->ntp());
    }

    std::chrono::milliseconds current_lag_ms() const final {
        static constexpr std::chrono::milliseconds no_lag{0};
        auto max_offset_for_translation = calculate_max_offset_for_translation(
          *_partition_proxy);

        auto last_translated = _stm->cached_highest_translated_offset();

        /**
         * Everything there is to translate was translated
         */
        if (last_translated == max_offset_for_translation) {
            vlog(
              datalake_log.trace,
              "[{}] translation up-to-date: last_translated={}, "
              "max_translatable={}",
              _partition->ntp(),
              last_translated,
              max_offset_for_translation.value_or(kafka::offset{-1}));
            return no_lag;
        }
        const auto last_ts = _stm->cached_last_translated_timestamp();
        const auto now = model::timestamp::now();
        // No translation happened yet, use the difference between the
        // translation target timestamp and now
        if (!last_ts.has_value()) {
            // no target is set and we do not have any translated offsets, it
            // means that the lag is 0
            vlog(
              datalake_log.trace,
              "[{}] lag calculation - no last_ts: last_translated={}, "
              "max_translatable={}",
              _partition->ntp(),
              last_translated,
              max_offset_for_translation.value_or(kafka::offset{-1}));
            if (!_translation_target.has_value()) {
                return no_lag;
            }
            return std::chrono::milliseconds{
              std::max<int64_t>(0, (now - _translation_target->ts).value())};
        }

        vlog(
          datalake_log.trace,
          "[{}] lag calculation - using last_ts: "
          "last_translated={}, max_translatable={}, last_ts={}",
          _partition->ntp(),
          last_translated,
          max_offset_for_translation.value_or(kafka::offset{-1}),
          last_ts.value());
        return std::chrono::milliseconds{
          std::max<int64_t>(0, (now - last_ts.value()).value())};
    }

    void notify_new_data_for_translation(
      kafka::offset max_translatable_offset) final {
        // don't set a new _translation_target until the current one is cleared
        // out. this way we can't get into a situation where we're continually
        // chasing the tip of the log but never reaching it (e.g. due to produce
        // resources exceeding translate resources).
        vlog(
          datalake_log.trace,
          "[{}] New data for translation with offset {}, current "
          "translation_target: {}",
          _partition->ntp(),
          max_translatable_offset,
          _translation_target);
        if (!_translation_target.has_value()) {
            _translation_target.emplace(max_translatable_offset);
        }
    }

    void notify_data_translated(kafka::offset translated_offset) final {
        // time we found new translatable records, along with the system time we
        // found them. the general idea here is that, once we've translated
        // up to an offset that meets or exceeds the stored target offset, any
        // additional offsets must have become translatable at some
        // _translation_target.ts + T. at this point, we can replicate the
        // cached timestamp, since it provides a lower bound for when
        // new_offset became translatable, from the perspective of the
        // translator.
        // If translation met or exceeded the target, also reset it to nullopt
        // so that a new target can be set the next time fresh offsets become
        // available for translation.
        vlog(
          datalake_log.trace,
          "[{}] New offset translated {}, current "
          "translation_target: {}",
          _partition->ntp(),
          translated_offset,
          _translation_target);
        if (
          _translation_target.has_value()
          && translated_offset >= _translation_target->offset) {
            _translation_target.reset();
        }
        // Reset inflight translation lto. The lag tracker is notified about
        // completed translation.
        _inflight_translation_lto.reset();
    }

    void notify_inflight_translation_iteration(
      std::optional<kafka::offset> translated_offset) final {
        vlog(
          datalake_log.trace,
          "[{}] Inflight translation iteration with offset {} ",
          _partition->ntp(),
          translated_offset);
        if (!translated_offset) {
            _inflight_translation_lto.reset();
            return;
        }
        _inflight_translation_lto.emplace(translated_offset.value());
    }

    std::optional<size_t> translation_backlog() const final {
        auto lso_res = _partition_proxy->last_stable_offset();
        if (lso_res.has_error()) {
            return std::nullopt;
        }
        auto max_translatable = kafka::prev_offset(
          model::offset_cast(lso_res.value()));
        auto checkpointed_lto = _stm->cached_highest_translated_offset();
        auto next_to_translate
          = checkpointed_lto == kafka::offset::min()
              ? calculate_min_offset_for_translation(
                  _partition->is_read_replica_mode_enabled(), *_partition_proxy)
              : kafka::next_offset(checkpointed_lto);
        if (_inflight_translation_lto) {
            next_to_translate = std::max(
              kafka::next_offset(*_inflight_translation_lto),
              next_to_translate);
        }
        return _partition_proxy->estimate_size_between(
          next_to_translate, max_translatable);
    }

    std::optional<model::timestamp> get_translated_offset_timestamp_estimate(
      kafka::offset last_translated_offset) final {
        if (!_translation_target) {
            return std::nullopt;
        }

        if (last_translated_offset >= _translation_target->offset) {
            return _translation_target->ts;
        }

        return std::nullopt;
    }

    scheduling::clock::time_point next_checkpoint_deadline() const final {
        auto time_left = target_lag_goal() - current_lag_ms();
        return scheduling::clock::now() + time_left;
    }

private:
    // percentage of the target lag that we should finish inflight translation
    // at (in range [0, 1])
    static constexpr auto target_lag_percentage = 0.9;
    /**
     * Returns a duration which is a percentage of the target lag. We do not
     * want to finish the translation at the target lag as not keeping up with
     * the translation would trigger backpressure mechanism. In order to avoid
     * that we use a percentage of the target lag.
     */
    scheduling::clock::duration target_lag_goal() const {
        return std::chrono::duration_cast<scheduling::clock::duration>(
          target_lag_percentage * target_lag());
    }

    ss::lw_shared_ptr<cluster::partition> _partition;
    std::unique_ptr<kafka::partition_proxy> _partition_proxy;
    cluster::topic_table& _topics;
    ss::shared_ptr<translation_stm> _stm;
    // updated to max_offset_for_translation and current system time when
    // wait_for_data_to_translated finds untranslated offsets on the input
    // partition. the timestamp here is used as a rough approximation of
    // the corresponding record's write time, replicated when we translate
    // an offset meeting or exceeding target.offset.
    std::optional<timestamped_offset> _translation_target;
    std::optional<kafka::offset> _inflight_translation_lto;
};

std::unique_ptr<translation_lag_tracker>
translation_lag_tracker::make_default_lag_tracker(
  ss::lw_shared_ptr<cluster::partition> partition,
  cluster::topic_table& topics) {
    return std::make_unique<partition_lag_tracker>(
      std::move(partition), topics);
}

} // namespace datalake::translation
