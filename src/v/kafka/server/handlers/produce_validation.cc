/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/handlers/produce_validation.h"

#include "kafka/protocol/logger.h"
#include "ssx/sformat.h"

namespace {
static constexpr auto validate_rate_interval = std::chrono::minutes(1);
}

namespace kafka {

namespace {

// Validates the input timestamp using the broker time and the allowable
// `log.message.timestamp.{before/after}.max.ms` drift.
//
// This is a no-op for `timestamp_type == APPEND_TIME`:
// https://kafka.apache.org/documentation/#brokerconfigs_log.message.timestamp.after.max.ms
std::optional<error_code_and_msg> validate_timestamp(
  model::timestamp timestamp,
  model::offset offset,
  model::timestamp broker_time,
  model::timestamp_type timestamp_type,
  std::chrono::milliseconds message_timestamp_before_max_ms,
  std::chrono::milliseconds message_timestamp_after_max_ms,
  kafka::kafka_probe& probe,
  const model::ntp& ntp) {
    if (timestamp_type == model::timestamp_type::append_time) {
        // These validations are skipped for `APPEND_TIME`.
        // https://kafka.apache.org/documentation/#brokerconfigs_log.message.timestamp.after.max.ms
        return std::nullopt;
    }

    auto delta = broker_time - timestamp;
    auto is_invalid = delta > model::timestamp(
                        message_timestamp_before_max_ms.count())
                      || model::timestamp(-1 * delta()) > model::timestamp(
                           message_timestamp_after_max_ms.count());
    if (is_invalid) {
        thread_local static ss::logger::rate_limit rate(validate_rate_interval);
        auto msg = ssx::sformat(
          "Timestamp {} of message for partition {} with offset {} is out of "
          "range. The timestamp should be within [{}, {}] of the broker time.",
          timestamp,
          ntp,
          offset,
          message_timestamp_before_max_ms,
          message_timestamp_after_max_ms);
        klog.log(ss::log_level::warn, rate, "{}", msg);
        probe.produce_bad_create_time();
        return error_code_and_msg{
          .err = error_code::invalid_timestamp, .msg = std::move(msg)};
    }

    return std::nullopt;
}

// Validates the provided batch's `first` and `max` timestamps using the broker
// time and the allowable `log.message.timestamp.{before/after}.max.ms` drift.
// This is usually called in `legacy` validation mode, or when it is decided to
// not decompress the incoming batch in the produce path, as validation would
// then occur for the timestamp of each record instead.
//
// This is a no-op for `timestamp_type == APPEND_TIME`:
// https://kafka.apache.org/documentation/#brokerconfigs_log.message.timestamp.after.max.ms
std::optional<error_code_and_msg> validate_batch_timestamps(
  const model::record_batch& batch,
  model::timestamp broker_time,
  model::timestamp_type timestamp_type,
  std::chrono::milliseconds message_timestamp_before_max_ms,
  std::chrono::milliseconds message_timestamp_after_max_ms,
  kafka::kafka_probe& probe,
  const model::ntp& ntp) {
    if (timestamp_type == model::timestamp_type::append_time) {
        // These validations are skipped for `APPEND_TIME`.
        // https://kafka.apache.org/documentation/#brokerconfigs_log.message.timestamp.after.max.ms
        return std::nullopt;
    }

    std::optional<error_code_and_msg> res;

    const auto& header = batch.header();

    // reject if first_timestamp is invalid.
    res = validate_timestamp(
      header.first_timestamp,
      header.base_offset,
      broker_time,
      timestamp_type,
      message_timestamp_before_max_ms,
      message_timestamp_after_max_ms,
      probe,
      ntp);

    if (res.has_value()) {
        return res;
    }

    // reject if max_timestamp is invalid.
    res = validate_timestamp(
      header.max_timestamp,
      header.last_offset(),
      broker_time,
      timestamp_type,
      message_timestamp_before_max_ms,
      message_timestamp_after_max_ms,
      probe,
      ntp);

    return res;
}

// Sets the `max_timestamp` in `batch` with either:
// 1. The `batch_max_timestamp` if `timestamp_type == CREATE_TIME`
// 2. The `broker_time` if `timestamp_type == APPEND_TIME`.
//
// If `timestamp_type == CREATE_TIME` and `batch_max_timestamp ==
// timestamp::missing (-1)`, this is a no-op.
void maybe_set_max_timestamp(
  model::record_batch& batch,
  std::optional<model::timestamp> max_ts,
  model::timestamp broker_time,
  model::timestamp_type timestamp_type) {
    // Override with `broker_time` for `APPEND_TIME` type.
    if (timestamp_type == model::timestamp_type::append_time) {
        max_ts = broker_time;
    }

    if (max_ts.has_value()) {
        batch.set_max_timestamp(timestamp_type, max_ts.value());
    }
}

// Iterate over all records in the batch, invoking `f()` on each.
// Invoking this function shows that iteration over the records within the batch
// is possible (i.e format validation). For corrupted batches, an
// `INVALID_RECORD` error code and message are returned.
std::optional<error_code_and_msg> iterate_over_records(
  const model::record_batch& b,
  ss::noncopyable_function<ss::stop_iteration(model::record)> f) {
    dassert(
      !b.compressed(),
      "Cannot iterate over records within a compressed batch.");
    try {
        b.for_each_record(std::move(f));
    } catch (const std::exception& e) {
        vlog(
          klog.error,
          "Caught exception while validating record timestamps for batch {}: "
          "{}",
          b.header(),
          e.what());
        return error_code_and_msg{
          .err = error_code::invalid_record, .msg = e.what()};
    }

    return std::nullopt;
}

// This function computes the `max_timestamp` from the records within a batch
// and returns it in the happy path. If there is an issue with iterating over
// the records, it is returned via std::unexpected(error_code_and_msg).
std::expected<model::timestamp, error_code_and_msg>
compute_max_timestamp(const model::record_batch& b) {
    int64_t max_timestamp_delta = 0;

    auto res = iterate_over_records(b, [&](model::record r) mutable {
        max_timestamp_delta = std::max(
          r.timestamp_delta(), max_timestamp_delta);
        return ss::stop_iteration::no;
    });

    if (res.has_value()) {
        return std::unexpected(res.value());
    }

    model::timestamp max_timestamp(
      b.header().first_timestamp() + max_timestamp_delta);
    return max_timestamp;
}

// This function validates record timestamps and computes the `max_timestamp`
// from the records within a batch in one pass. If there is
// an issue with iterating over the records, it is returned via
// std::unexpected(error_code_and_msg).
std::expected<model::timestamp, error_code_and_msg>
validate_records_and_compute_max_timestamp(
  model::record_batch& b,
  const model::record_batch& iterable_batch_ref,
  model::timestamp broker_time,
  model::timestamp_type timestamp_type,
  std::chrono::milliseconds message_timestamp_before_max_ms,
  std::chrono::milliseconds message_timestamp_after_max_ms,
  kafka::kafka_probe& probe,
  const model::ntp& ntp) {
    std::optional<error_code_and_msg> res;
    int64_t max_timestamp = -1;
    auto iterable_res = iterate_over_records(
      iterable_batch_ref, [&](model::record r) mutable {
          auto timestamp = model::timestamp{
            b.header().first_timestamp() + r.timestamp_delta()};
          auto offset = b.base_offset() + model::offset_delta(r.offset_delta());
          res = validate_timestamp(
            timestamp,
            offset,
            broker_time,
            timestamp_type,
            message_timestamp_before_max_ms,
            message_timestamp_after_max_ms,
            probe,
            ntp);
          max_timestamp = std::max(timestamp(), max_timestamp);

          return res.has_value() ? ss::stop_iteration::yes
                                 : ss::stop_iteration::no;
      });

    if (iterable_res.has_value()) {
        // Prefer to return an error describing an invalid format rather than an
        // invalid timestamp.
        res = iterable_res;
    }

    if (res.has_value()) {
        return std::unexpected(res.value());
    }

    return model::timestamp(max_timestamp);
}

// Whether or not the incoming batch requires decompression depends on the
// validation mode, and properties of the batch itself. Currently, the logic is:
// 1. Never decompress in `legacy` mode.
// 2. Decompress in `relaxed` mode _only_ if the batch's `max_timestamp` is not
// properly set by the producer.
// 3. Always decompress in `strict` mode.
bool should_decompress(
  const model::record_batch& b, model::kafka_batch_validation_mode m) {
    if (!b.compressed()) {
        return false;
    }

    using mode = model::kafka_batch_validation_mode;
    switch (m) {
    case mode::legacy:
        // Never decompress in `legacy` mode.
        return false;
    case mode::relaxed:
        // Decompress only if the `max_timestamp` needs to be set.
        return b.header().max_timestamp == model::timestamp::missing();
    case mode::strict:
        // Perform all strict checks.
        return true;
    }
}

// `iterable_batch_ref` is guaranteed to be a iterable, decompressed version of
// `batch` if it has a value. Modifications to the underlying data should/can
// only be made on `batch`.
std::optional<error_code_and_msg> validate_batch(
  model::record_batch& batch,
  std::optional<std::reference_wrapper<const model::record_batch>>
    iterable_batch_ref,
  model::kafka_batch_validation_mode validation_mode,
  model::timestamp_type timestamp_type,
  std::chrono::milliseconds message_timestamp_before_max_ms,
  std::chrono::milliseconds message_timestamp_after_max_ms,
  kafka::kafka_probe& probe,
  const model::ntp& ntp,
  std::optional<std::string_view> client_id) {
    std::optional<error_code_and_msg> res{std::nullopt};
    const auto broker_time = model::timestamp::now();
    const auto has_iterable_batch = iterable_batch_ref.has_value();

    using mode = model::kafka_batch_validation_mode;
    switch (validation_mode) {
    case mode::legacy: {
        // The following checks are performed in `legacy` mode:
        // 1. Iterate over records if batch is uncompressed, set `max_timestamp`
        // with result OR with broker time if `APPEND_TIME` is the timestamp
        // type. There are no guarantees that a batch will have a
        // `max_timestamp` set in `legacy` mode.
        // 2. Check batch header timestamps.
        std::optional<model::timestamp> max_ts{std::nullopt};
        if (has_iterable_batch) {
            auto max_ts_res = compute_max_timestamp(iterable_batch_ref->get());
            if (!max_ts_res.has_value()) {
                return max_ts_res.error();
            } else {
                max_ts = max_ts_res.value();
            }
        }

        maybe_set_max_timestamp(batch, max_ts, broker_time, timestamp_type);

        res = validate_batch_timestamps(
          batch,
          broker_time,
          timestamp_type,
          message_timestamp_before_max_ms,
          message_timestamp_after_max_ms,
          probe,
          ntp);

        if (res.has_value()) {
            return res;
        }

        if (batch.header().max_timestamp == model::timestamp::missing()) {
            // Warn the user that the `max_timestamp` is unset for an
            // accepted batch.
            thread_local static ss::logger::rate_limit rate(
              validate_rate_interval);
            klog.log(
              ss::log_level::warn,
              rate,
              "Produced batch for partition {} has max_timestamp left unset "
              "({{-1}}) by client (client_id: {}). Accepting batch since '{}' "
              "is set to '{}'. It is strongly recommended that you update your "
              "client to set the max_timestamp when producing: {}.",
              ntp,
              client_id,
              config::shard_local_cfg().kafka_produce_batch_validation.name(),
              validation_mode,
              batch.header());
        }
        break;
    }
    case mode::relaxed: {
        // The following checks are performed in `relaxed` mode:
        // 1. Validate record timestamps and set `max_timestamp` if it is
        // missing or if original batch was uncompressed (`iterable_batch_ref`
        // will have a set value in either case). It is guaranteed that a batch
        // will have a `max_timestamp` set in `relaxed` mode.
        // 2. Validate header timestamps if record timestamps were not checked
        // in check 1.
        if (
          batch.header().max_timestamp == model::timestamp::missing()
          && batch.compressed()) {
            // Warn the user that the `max_timestamp` is unset for an
            // batch, forcing us to decompress it.
            thread_local static ss::logger::rate_limit rate(
              validate_rate_interval);
            klog.log(
              ss::log_level::warn,
              rate,
              "Produced batch for partition {} has max_timestamp left unset "
              "({{-1}}) by client (client_id: {}). Decompressing batch and "
              "setting max_timestamp manually since '{}' is set to '{}'. It is "
              "strongly recommended that you update your client to set the "
              "max_timestamp when producing: {}.",
              ntp,
              client_id,
              config::shard_local_cfg().kafka_produce_batch_validation.name(),
              validation_mode,
              batch.header());
        }

        std::optional<model::timestamp> max_ts{std::nullopt};
        if (has_iterable_batch) {
            // Validate records and compute max timestamp in one pass.
            auto max_ts_res = validate_records_and_compute_max_timestamp(
              batch,
              iterable_batch_ref->get(),
              broker_time,
              timestamp_type,
              message_timestamp_before_max_ms,
              message_timestamp_after_max_ms,
              probe,
              ntp);
            if (!max_ts_res.has_value()) {
                return max_ts_res.error();
            } else {
                max_ts = max_ts_res.value();
            }
        }

        maybe_set_max_timestamp(batch, max_ts, broker_time, timestamp_type);

        // If record timestamps were not validated because the batch was not
        // decompressed in order to set the `max_timestamp`, we have to validate
        // batch timestamps.
        if (!has_iterable_batch) {
            res = validate_batch_timestamps(
              batch,
              broker_time,
              timestamp_type,
              message_timestamp_before_max_ms,
              message_timestamp_after_max_ms,
              probe,
              ntp);
        }

        if (res.has_value()) {
            return res;
        }

        break;
    }
    case mode::strict: {
        // The following checks are performed in `strict` mode:
        // 1. Iterate over records and set max_timestamp. It is guaranteed that
        // a batch will have a `max_timestamp` set in `strict` mode.
        // 2. Check record timestamps.
        // TODO: validate offsets, control batches, versioning, etc.
        // See checks present here:
        // github.com/apache/kafka/blob/trunk/storage/src/main/java/org/apache/kafka/storage/internals/log/LogValidator.java#L438

        dassert(
          has_iterable_batch,
          "Batch must be iterable in kafka_produce_batch_validation::strict.");

        // Validate records and compute max timestamp in one pass.
        std::optional<model::timestamp> max_ts{std::nullopt};
        auto max_ts_res = validate_records_and_compute_max_timestamp(
          batch,
          iterable_batch_ref->get(),
          broker_time,
          timestamp_type,
          message_timestamp_before_max_ms,
          message_timestamp_after_max_ms,
          probe,
          ntp);

        if (!max_ts_res.has_value()) {
            return max_ts_res.error();
        } else {
            max_ts = max_ts_res.value();
        }

        maybe_set_max_timestamp(batch, max_ts, broker_time, timestamp_type);

        break;
    }
    }

    return res;
}

} // namespace

ss::future<std::optional<error_code_and_msg>>
validate_batch(const validation_args& args) {
    const auto& validation_mode
      = config::shard_local_cfg().kafka_produce_batch_validation();

    std::optional<model::record_batch> maybe_decompressed_batch;
    std::optional<std::reference_wrapper<const model::record_batch>>
      maybe_decompressed_batch_ref;

    auto& batch = args.batch;

    if (batch.compressed()) {
        if (should_decompress(batch, validation_mode)) {
            try {
                maybe_decompressed_batch = co_await model::decompress_batch(
                  batch);
            } catch (...) {
                co_return error_code_and_msg{
                  .err = error_code::corrupt_message,
                  .msg = "unable to decompress batch",
                };
            }
            maybe_decompressed_batch_ref = maybe_decompressed_batch.value();
        }
    } else {
        maybe_decompressed_batch_ref = batch;
    }

    co_return validate_batch(
      batch,
      maybe_decompressed_batch_ref,
      validation_mode,
      args.timestamp_type,
      args.message_timestamp_before_max_ms,
      args.message_timestamp_after_max_ms,
      args.probe,
      args.ntp,
      args.client_id);
}

} // namespace kafka
