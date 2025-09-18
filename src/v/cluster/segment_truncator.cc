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

#include "cluster/segment_truncator.h"

#include "cloud_storage/materialized_resources.h"
#include "cloud_storage/read_path_probes.h"
#include "cloud_storage/remote.h"
#include "storage/offset_translator_state.h"
#include "utils/stream_provider.h"

#include <seastar/core/timed_out_error.hh>

namespace cluster {

namespace {
struct remote_provider : public stream_provider {
    explicit remote_provider(ss::input_stream<char> strm)
      : _strm(std::move(strm)) {}

    ss::input_stream<char> take_stream() override {
        vassert(_strm.has_value(), "no stream to take");
        auto r = std::exchange(_strm, std::nullopt);
        return std::move(r).value();
    }

    ss::future<> close() override {
        if (!_strm.has_value()) {
            return ss::now();
        }
        return _strm.value().close().then([this] { _strm.reset(); });
    }

private:
    std::optional<ss::input_stream<char>> _strm;
};
ss::logger trunc_log{"suffix-truncate"};
} // namespace

// TODO(oren): separate error code
ss::future<std::expected<cloud_storage::segment_meta, cluster::errc>>
truncate_remote_segment(
  cloud_storage::segment_meta seg,
  kafka::offset last_included_ko,
  const model::ntp& ntp,
  const cloud_storage::partition_manifest& manifest,
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage::remote_path_provider& path_provider,
  cloud_storage::remote& remote,
  cloud_storage::cache& cache,
  retry_chain_node& parent_retry) {
    auto raw_path = path_provider.segment_path(manifest, seg);
    auto remote_path = cloud_storage::remote_segment_path{std::move(raw_path)};

    vlog(
      trunc_log.debug,
      "{}: Truncate segment at {} to {}",
      ntp,
      remote_path,
      last_included_ko);

    auto& materialized = remote.materialized();
    auto& rp_probe = materialized.get_read_path_probe();
    cloud_storage::partition_probe part_probe{ntp};

    auto remote_seg = ss::make_lw_shared<cloud_storage::remote_segment>(
      remote,
      cache,
      bucket,
      remote_path,
      ntp,
      seg,
      parent_retry,
      part_probe,
      rp_probe);

    co_await remote_seg->hydrate();

    cloud_storage::cloud_log_reader_config reader_conf{
      kafka::offset{0},
      last_included_ko,
      std::nullopt,
      std::nullopt,
      1 /* batches_per_read */};

    ssx::semaphore_units units{};

    cloud_storage::remote_segment_batch_reader rdr{
      remote_seg, reader_conf, part_probe, rp_probe, std::move(units)};

    vlog(
      trunc_log.debug,
      "{}: Make offset translator state w/ base {} & delta {}",
      ntp,
      remote_seg->get_base_rp_offset(),
      remote_seg->get_base_offset_delta());

    storage::offset_translator_state ots{ntp};
    cloud_storage::segment_meta new_seg = {
      .is_compacted = seg.is_compacted,
      .size_bytes = 0 /* TODO(oren): overestimate - is ok? can't recall */,
      .base_offset = seg.base_offset,
      .committed_offset = model::offset{0} /* TODO(oren): compute*/,
      .base_timestamp = seg.base_timestamp,
      .max_timestamp = model::timestamp{0} /* TODO(oren): compute */,
      .delta_offset = seg.delta_offset,
      .ntp_revision = seg.ntp_revision,
      .archiver_term = seg.archiver_term,
      .segment_term = seg.segment_term,
      .delta_offset_end = model::offset_delta{0} /* TODO(oren): compute */,
      .sname_format = cloud_storage::segment_name_format::v3,
      .metadata_size_hint = 0,
    };

    bool done = false;
    while (!done) {
        using namespace std::chrono_literals;
        try {
            auto read_res = co_await rdr.read_some(
              model::timeout_clock::now() + 10s, ots);

            if (read_res.has_error()) {
                vlog(
                  trunc_log.debug,
                  "{}: Done... read failed with {}, max_ko: {}",
                  ntp,
                  read_res.error(),
                  new_seg.last_kafka_offset());
                break;
            } else if (read_res.value().empty()) {
                vlog(
                  trunc_log.debug,
                  "{}: Done... read returned no batches, max_ko: {}",
                  ntp,
                  new_seg.last_kafka_offset());
                break;
            }
            vlog(
              trunc_log.debug,
              "{}: Got some batches N={}",
              ntp,
              read_res.value().size());
            for (const auto& batch : read_res.value()) {
                const auto& hdr = batch.header();
                // NOTE(oren): when reading through remote_segment_batch_reader,
                // consume_batch_end does the offset translation into kafka
                // space on our behalf. for more direct access to the batch
                // header, use a lower level reader I guess
                [[maybe_unused]] auto batch_last_kafka = model::offset_cast(
                  hdr.last_offset());
                if (batch_last_kafka > last_included_ko) {
                    vlog(
                      trunc_log.debug,
                      "{}: Done... reached end, max_ko: {}, batch last ko: "
                      "{} batch size : {} ",
                      ntp,
                      new_seg.last_kafka_offset(),
                      batch_last_kafka,
                      hdr.size_bytes);
                    done = true;
                    break;
                }
                vlog(
                  trunc_log.debug,
                  "{}: BATCH [{}-{}](records size: {}) - {}",
                  ntp,
                  hdr.base_offset,
                  hdr.last_offset(),
                  hdr.size_bytes - model::packed_record_batch_header_size,
                  hdr);
                new_seg.size_bytes = rdr.bytes_consumed() + rdr.bytes_skipped();
                new_seg.committed_offset = model::prev_offset(
                  rdr.current_rp_offset());
                new_seg.max_timestamp = hdr.max_timestamp;
                new_seg.delta_offset_end = rdr.current_delta();
            }
        } catch (const ss::timed_out_error&) {
            vlog(trunc_log.debug, "{}: Read timed out", ntp);
            break;
        } catch (...) {
            auto ex = std::current_exception();
            vlog(trunc_log.debug, "{}: Unknown error during read: {}", ntp, ex);
            break;
        }
    }

    co_await rdr.stop();

    if (new_seg.committed_offset == model::offset{0}) {
        vlog(trunc_log.debug, "{}: Didn't process any more batches", ntp);
        co_await remote_seg->stop();
        co_return std::unexpected(cluster::errc::topic_operation_error);
    } else if (new_seg.committed_offset == seg.committed_offset) {
        vlog(
          trunc_log.debug,
          "{}: No change to committed offset (nothing truncated)",
          ntp);
        co_await remote_seg->stop();
        co_return seg;
    }

    vlog(trunc_log.debug, "{}: NEW SEG: {} ORIG SEG: {}", ntp, new_seg, seg);

    // otherwise reupload to the new path and we'll add that to the manifest

    vassert(
      new_seg.last_kafka_offset() <= last_included_ko,
      "OOPS: {} > {}",
      new_seg.last_kafka_offset(),
      last_included_ko);

    cloud_storage::remote::reset_input_stream reset_stream = [remote_seg,
                                                              &new_seg] {
        using provider_t = std::unique_ptr<stream_provider>;
        return remote_seg
          ->offset_data_stream(
            remote_seg->get_base_kafka_offset(),
            new_seg.last_kafka_offset(),
            std::nullopt,
            std::nullopt)
          .then(
            [](cloud_storage::remote_segment::input_stream_with_offsets strm)
              -> provider_t {
                return std::make_unique<remote_provider>(
                  std::move(strm.stream));
            });
    };

    lazy_abort_source as{[]() { return std::nullopt; }};

    auto new_raw_path = path_provider.segment_path(manifest, new_seg);
    auto new_remote_path = cloud_storage::remote_segment_path{
      std::move(new_raw_path)};

    auto upl_result = co_await remote.upload_segment(
      bucket,
      new_remote_path,
      new_seg.size_bytes,
      reset_stream,
      parent_retry,
      as,
      5);

    co_await remote_seg->stop();

    if (upl_result != cloud_storage::upload_result::success) {
        co_return std::unexpected(cluster::errc::topic_operation_error);
    }

    vlog(
      trunc_log.debug,
      "{}: SUCCESS {} -> {} - {}",
      ntp,
      seg.size_bytes,
      new_seg.size_bytes,
      new_seg);

    co_return new_seg;
}
} // namespace cluster
