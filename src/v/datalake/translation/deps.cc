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

#include "cluster/partition.h"
#include "datalake/coordinator/frontend.h"
#include "datalake/local_parquet_file_writer.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/record_translator.h"
#include "datalake/serde_parquet_writer.h"
#include "datalake/translation/state_machine.h"
#include "datalake/translation_task.h"
#include "kafka/data/partition_proxy.h"
#include "kafka/utils/txn_reader.h"

namespace datalake::translation {

static constexpr auto poll_duration = 2s;

// rebase on Oren's PR to pick this up from configurations
static constexpr auto target_lag = 1min;

ss::future<> noop_mem_tracker::maybe_reserve_memory(size_t, ss::abort_source&) {
    return ss::make_ready_future<>();
}
void noop_mem_tracker::update_current_memory_usage(size_t) {}
void noop_mem_tracker::release() {}

ss::future<> writer_reservations_impl::maybe_reserve_memory(
  size_t bytes, ss::abort_source& as) {
    while (_available_memory < bytes) {
        auto reservation = co_await _reservations_tracker.reserve_memory(as);
        _available_memory += reservation.count();
        _total_reserved_memory += reservation.count();
        _reservations.push_back(std::move(reservation));
    }
    _available_memory -= bytes;
    co_return;
}

void writer_reservations_impl::update_current_memory_usage(size_t used_bytes) {
    vassert(
      used_bytes <= _total_reserved_memory,
      "Used more bytes {} than reserved {}",
      used_bytes,
      _total_reserved_memory);
    _available_memory = _total_reserved_memory - used_bytes;
}

void writer_reservations_impl::release() {
    _available_memory = 0;
    _total_reserved_memory = 0;
    _reservations.clear();
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
      coordinator::add_translated_data_files_request request) override {
        return _frontend.add_translated_data_files(std::move(request));
    }

    ss::future<coordinator::fetch_latest_translated_offset_reply>
    fetch_latest_translated_offset(
      coordinator::fetch_latest_translated_offset_request request) override {
        return _frontend.fetch_latest_translated_offset(std::move(request));
    }

private:
    coordinator::frontend& _frontend;
};

std::unique_ptr<coordinator_api>
coordinator_api::make_default_coordinator_api(coordinator::frontend& frontend) {
    return std::make_unique<default_coordinator_api>(frontend);
}

class partition_data_source : public data_source {
public:
    explicit partition_data_source(
      ss::lw_shared_ptr<cluster::partition> partition)
      : _partition(std::move(partition))
      , _stm(_partition->raft()->stm_manager()->get<translation_stm>())
      , _partition_proxy(std::make_unique<kafka::partition_proxy>(
          kafka::make_partition_proxy(_partition))) {}

    const model::ntp& ntp() const override { return _partition->ntp(); }

    model::revision_id topic_revision() const override {
        return _partition->get_topic_revision_id();
    }

    model::term_id term() const override { return _partition->term(); }

    ss::future<kafka::offset> wait_for_data_to_translate(
      std::optional<kafka::offset> last_translated_offset,
      ss::abort_source& as) override {
        // todo: add logic to wait for enough data to translate.
        // currently we just break even if a single batch of data is available
        while (!has_more_data_to_translate(last_translated_offset)) {
            co_await ss::sleep_abortable(poll_duration, as);
        }
        auto read_begin_offset = min_offset_for_translation();
        if (last_translated_offset) {
            read_begin_offset = kafka::next_offset(
              last_translated_offset.value());
        }
        co_return read_begin_offset;
    }

    ss::future<std::optional<model::record_batch_reader>> make_log_reader(
      kafka::offset begin_offset,
      ss::io_priority_class io_priority,
      ss::abort_source& as) override {
        auto max_translatable_offset = max_offset_for_translation();
        if (
          !max_translatable_offset
          || max_translatable_offset.value() < begin_offset) {
            co_return std::nullopt;
        }
        auto log_reader = co_await _partition_proxy->make_reader(
          {kafka::offset_cast(begin_offset),
           kafka::offset_cast(max_translatable_offset.value()),
           0,
           std::numeric_limits<size_t>::max(),
           io_priority,
           std::nullopt,
           std::nullopt,
           as});
        auto tracker = kafka::aborted_transaction_tracker::create_default(
          _partition_proxy.get(), std::move(log_reader.ot_state));
        co_return model::make_record_batch_reader<kafka::read_committed_reader>(
          std::move(tracker), std::move(log_reader.reader));
    }

    kafka::offset min_offset_for_translation() const override {
        if (_partition->is_read_replica_mode_enabled()) {
            return model::offset_cast(_partition_proxy->start_offset());
        }
        return model::offset_cast(_partition_proxy->local_start_offset());
    }

    std::optional<kafka::offset> max_offset_for_translation() const override {
        // We factor in LSO to ensure only committed transactional batches are
        // translated.
        auto lso = _partition_proxy->last_stable_offset();
        if (lso.has_error()) {
            return std::nullopt;
        }
        return kafka::prev_offset(model::offset_cast(lso.value()));
    }

    ss::future<std::error_code> update_highest_translated_offset(
      kafka::offset new_offset,
      model::term_id term,
      model::timeout_clock::duration timeout,
      ss::abort_source& as) override {
        return _stm->reset_highest_translated_offset(
          new_offset, term, timeout, as);
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
};

std::unique_ptr<data_source> data_source::make_default_data_source(
  ss::lw_shared_ptr<cluster::partition> partition) {
    return std::make_unique<partition_data_source>(std::move(partition));
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
      remote_path upload_path_prefix,
      scheduling::reservations_tracker& reservations,
      ss::sharded<cluster::topic_table>* topics,
      ss::sharded<features::feature_table>* features)
      : _writer_scratch_space(std::move(writer_scratch_space))
      , _ntp(ntp)
      , _topic_revision(topic_revision)
      , _cloud_io(uploader)
      , _schema_mgr(schema_mgr)
      , _type_resolver(std::move(type_resolver))
      , _record_translator(std::move(record_translator))
      , _table_creator(std::move(table_creator))
      , _location_provider(std::move(location_provider))
      , _upload_path_prefix(std::move(upload_path_prefix))
      , _reservations(reservations)
      , _topics(topics->local())
      , _features(features->local())
      , _invalid_record_action(compute_invalid_record_action())
      , _cp_enabled(translation_task::custom_partitioning_enabled{
          _features.is_active(features::feature::datalake_iceberg_ga)})
      , _target_lag(compute_target_lag()) {}

    ss::future<> translate_now(
      model::record_batch_reader reader, ss::abort_source& as) override {
        if (!_in_progress_translation) {
            _in_progress_translation.emplace(
              _ntp,
              _topic_revision,
              make_writer_factory(),
              _cloud_io,
              _schema_mgr,
              *_type_resolver,
              *_record_translator,
              *_table_creator,
              _invalid_record_action,
              _location_provider);
            _discard_translated_state = false;
        }
        if (_discard_translated_state) {
            return ss::make_exception_future(
              "state changed, reset translation");
        }
        return _in_progress_translation->translate_once(std::move(reader), as);
    }

    void reconcile_properties() override {
        auto old_invalid_action = _invalid_record_action;
        auto old_cp_enabled = _cp_enabled;
        auto old_target_lag = _target_lag;

        _invalid_record_action = compute_invalid_record_action();
        _cp_enabled = translation_task::custom_partitioning_enabled{
          _features.is_active(features::feature::datalake_iceberg_ga)};
        _target_lag = compute_target_lag();

        if (_in_progress_translation) {
            // discard any state so far
            _discard_translated_state = old_invalid_action
                                          != _invalid_record_action
                                        || old_cp_enabled != _cp_enabled
                                        || old_target_lag != _target_lag;
        }
    }

    ss::future<> flush() override {
        if (_in_progress_translation) {
            return _in_progress_translation->flush().then_wrapped(
              [](auto result_f) {
                  if (result_f.failed()) {
                      return ss::make_exception_future(
                        result_f.get_exception());
                  }
                  auto result = result_f.get();
                  if (result.has_error()) {
                      return ss::make_exception_future(result.error());
                  }
                  return ss::now();
              });
        }
        return ss::now();
    }

    ss::future<std::optional<coordinator::translated_offset_range>>
    finish(retry_chain_node& rcn, ss::abort_source& as) override {
        if (!_in_progress_translation) {
            co_return std::nullopt;
        }
        as.check();
        auto task = std::exchange(_in_progress_translation, std::nullopt);
        auto result = co_await std::move(task.value())
                        .finish(_cp_enabled, _upload_path_prefix, rcn, as);
        if (result.has_error() || _discard_translated_state) {
            co_return std::nullopt;
        }
        co_return std::make_optional(std::move(result.value()));
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
        return std::chrono::duration_cast<scheduling::clock::duration>(
          target_lag);
    }

    std::unique_ptr<parquet_file_writer_factory> make_writer_factory() {
        return std::make_unique<local_parquet_file_writer_factory>(
          _writer_scratch_space, // storage temp files are written to
          "",                    // file prefix
          ss::make_shared<serde_parquet_writer_factory>(),
          std::make_unique<writer_reservations_impl>(_reservations));
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
    remote_path _upload_path_prefix;
    scheduling::reservations_tracker& _reservations;
    cluster::topic_table& _topics;
    features::feature_table& _features;
    model::iceberg_invalid_record_action _invalid_record_action;
    translation_task::custom_partitioning_enabled _cp_enabled;
    scheduling::clock::duration _target_lag;

    std::optional<translation_task> _in_progress_translation;
    bool _discard_translated_state{false};
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
  remote_path upload_path_prefix,
  scheduling::reservations_tracker& reservations,
  ss::sharded<cluster::topic_table>* topics,
  ss::sharded<features::feature_table>* features) {
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
      std::move(upload_path_prefix),
      reservations,
      topics,
      features);
}

} // namespace datalake::translation
