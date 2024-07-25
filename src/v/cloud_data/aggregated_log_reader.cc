/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_data/aggregated_log_reader.h"

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cloud_data/dl_placeholder.h"
#include "cloud_data/logger.h"
#include "cloud_data/types.h"
#include "cloud_storage/cache_service.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/record_utils.h"
#include "serde/rw/rw.h"
#include "serde/rw/uuid.h"
#include "storage/log_reader.h"
#include "storage/parser.h"
#include "storage/record_batch_utils.h"
#include "storage/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>

using namespace std::chrono_literals;

namespace cloud_data {

//
struct hydrated_L0_object {
    object_id id;
    iobuf payload;
};

// Materialized placeholder extent
struct placeholder_extent {
    model::offset base_offset;
    dl_placeholder placeholder;
    ss::lw_shared_ptr<hydrated_L0_object> L0_object;
};

// Get dl_placeholder and the payload of the object and generate a record batch
inline model::record_batch
materialize_single_raft_data_batch(placeholder_extent& extent) {
    auto offset = extent.placeholder.offset;
    auto size = extent.placeholder.size_bytes;
    vassert(
      size() > model::packed_record_batch_header_size,
      "L0 object is smaller ({}) than the batch header",
      size());
    auto header_bytes = extent.L0_object->payload.share(
      offset(), model::packed_record_batch_header_size);
    auto records_bytes = extent.L0_object->payload.share(
      offset() + model::packed_record_batch_header_size,
      size() - model::packed_record_batch_header_size);
    auto header = storage::batch_header_from_disk_iobuf(
      std::move(header_bytes));
    // NOTE: the serialized raft_data batch doesn't have the offset set
    // so we need to populate it from the placeholder batch. We also need
    // to make sure that crc is correct.
    header.base_offset = extent.base_offset;
    header.crc = model::crc_record_batch(header, records_bytes);
    crc::crc32c crc;
    model::crc_record_batch_header(crc, header);
    header.header_crc = crc.value();
    model::record_batch batch(
      header,
      std::move(records_bytes),
      model::record_batch::tag_ctor_ng{}); // TODO: fix compression
    return batch;
}

/// Convert record batch to dl_placeholder value and return an empty
/// extent which later has to be hydrated.
inline placeholder_extent make_placeholder_extent(model::record_batch batch) {
    auto base_offset = batch.base_offset();
    iobuf payload = std::move(batch).release_data();
    iobuf_parser parser(std::move(payload));
    auto record = model::parse_one_record_from_buffer(parser);
    iobuf value = std::move(record).release_value();
    auto placeholder = serde::from_iobuf<dl_placeholder>(std::move(value));
    return placeholder_extent{
      .base_offset = base_offset,
      .placeholder = placeholder,
      .L0_object = ss::make_lw_shared<hydrated_L0_object>({
        .id = placeholder.id,
      }),
    };
}

inline size_t get_extent_size(const model::record_batch& placeholder) {
    vassert(
      placeholder.header().type == model::record_batch_type::dl_placeholder,
      "Unsupported batch type {}",
      placeholder.header());
    /// FIXME: This code makes unnecessary copy
    iobuf payload = placeholder.data().copy();
    iobuf_parser parser(std::move(payload));
    auto record = model::parse_one_record_from_buffer(parser);
    iobuf value = std::move(record).release_value();
    auto p = serde::from_iobuf<dl_placeholder>(std::move(value));
    return p.size_bytes;
}

/// Fetch data referenced by the placeholder batch and the content of the
/// dl_placeholder
inline ss::future<> hydrate_placeholder_extent( // TODO: fix error handling,
                                                // should return an error code
  placeholder_extent* extent,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<ss::lowres_clock>* api,
  cloud_storage::cloud_storage_cache_api* cache) {
    // This iobuf contains the record batch replaced by the placeholder. It
    // might potentially contain data that belongs to other placeholder batches
    // and in order to get the extent of the record batch placeholder we need
    // to use byte offset and size.
    iobuf L0_object_content;

    // 2. download object from S3
    auto cache_file_name = std::filesystem::path(
      ssx::sformat("{}", extent->placeholder.id()));
    if (
      cloud_storage::cache_element_status::available
      != co_await cache->is_cached(cache_file_name)) {
        // Populate the cache
        ss::abort_source as; // TODO: use proper abort source
        retry_chain_node tmp_rtc(
          as,
          10s,
          200ms,
          retry_strategy::disallow); // TODO: fix timeout/backoff

        cloud_io::download_request req{
          .transfer_details = {
            .bucket = bucket, 
            .key = cloud_storage_clients::object_key(cache_file_name), 
            .parent_rtc = tmp_rtc,
            },
          .display_str = "L0",
          .payload = L0_object_content};
        auto dl_result = co_await api->download_object(std::move(req));
        if (dl_result != cloud_storage::download_result::success) {
            throw "failed to download"; // TODO: fix YOLO error handling
        }
        auto buf_str = make_iobuf_input_stream(L0_object_content.copy());
        auto sr_guard = co_await cache->reserve_space(0, 0);
        co_await cache->put(cache_file_name, buf_str, sr_guard);
    } else {
        // Read object from the cache
        auto file = co_await cache->get(cache_file_name);
        if (file.has_value() == false) {
            throw "cache race condition";
        }
        ss::file_input_stream_options options{};
        options.buffer_size
          = config::shard_local_cfg().storage_read_buffer_size();
        options.read_ahead
          = config::shard_local_cfg().storage_read_readahead_count();
        options.io_priority_class = ss::default_priority_class(); // FIXME
        auto source = ss::make_file_input_stream(
          std::move(file->body), 0, std::move(options));
        auto target = make_iobuf_ref_output_stream(L0_object_content);
        co_await ss::copy(source, target);
        co_await source.close();
    }

    extent->L0_object->payload = std::move(L0_object_content);
}

inline ss::future<ss::circular_buffer<placeholder_extent>>
materialize_sorted_run(
  ss::circular_buffer<model::record_batch> placeholders,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<ss::lowres_clock>* api,
  cloud_storage::cloud_storage_cache_api* cache) {
    absl::node_hash_map<uuid_t, ss::lw_shared_ptr<hydrated_L0_object>> hydrated;
    ss::circular_buffer<placeholder_extent> extents;
    for (auto&& p : placeholders) {
        auto extent = make_placeholder_extent(std::move(p));
        extents.push_back(extent);
        // reuse hydrated objects if possible
        auto it = hydrated.find(extent.placeholder.id());
        if (it != hydrated.end()) {
            auto& payload = it->second->payload;
            // TODO: check that id of the payload matches
            extent.L0_object->payload = payload.share(0, payload.size_bytes());
        } else {
            co_await hydrate_placeholder_extent(&extent, bucket, api, cache);
            // TODO: error handling and logging
            hydrated.insert(
              std::make_pair(extent.placeholder.id, extent.L0_object));
        }
    }
    co_return std::move(extents);
}

class joining_record_batch_reader_impl
  : public model::record_batch_reader::impl {
public:
    joining_record_batch_reader_impl(
      model::record_batch_reader rdr,
      storage::log_reader_config cfg,
      cloud_storage_clients::bucket_name bucket,
      cloud_io::remote_api<ss::lowres_clock>& api,
      cloud_storage::cloud_storage_cache_api& cache)
      : _underlying(std::move(rdr))
      , _config(std::move(cfg))
      , _bucket(std::move(bucket))
      , _api(api)
      , _cache(cache) {}

    bool is_end_of_stream() const override {
        bool is_eos = _config.start_offset >= _config.max_offset
                      || _underlying.is_end_of_stream();
        /*TODO: remove*/ vlog(
          cd_log.debug,
          "NEEDLER: JRBRI, is end of stream: {}, underlying: {}, config: {}",
          is_eos,
          _underlying.is_end_of_stream(),
          _config);
        return is_eos;
    }

    ss::future<model::record_batch_reader::storage_t>
    do_load_slice(model::timeout_clock::time_point tm) override {
        /*TODO: remove*/ vlog(
          cd_log.debug, "NEEDLER: JRBRI, do_load_slice invoked");
        struct consumer {
            consumer(
              storage::log_reader_config& config,
              ss::circular_buffer<model::record_batch>& rb)
              : _config(config)
              , _read_buffer(rb) {}

            ss::future<ss::stop_iteration> operator()(model::record_batch rb) {
                /*TODO: remove*/ vlog(
                  cd_log.debug,
                  "NEEDLER: JRBRI.consumer () {} vs {}",
                  rb.header(),
                  _config);
                if (rb.header().base_offset < _config.start_offset) {
                    /*TODO: remove*/ vlog(
                      cd_log.debug, "NEEDLER: JRBRI.consumer skip");
                    // Skip data which was already consumed
                    co_return ss::stop_iteration::no;
                }
                if (rb.header().last_offset() > _config.max_offset) {
                    /*TODO: remove*/ vlog(
                      cd_log.debug, "NEEDLER: JRBRI.consumer stop");
                    // Stop on EOS
                    co_return ss::stop_iteration::yes;
                }
                // Account bytes based on placeholder content, not header
                _num_bytes += get_extent_size(rb);
                auto last_offset = rb.header().last_offset();
                _read_buffer.push_back(std::move(rb));
                _config.start_offset = model::next_offset(last_offset);
                bool done = _config.start_offset >= _config.max_offset
                            || _num_bytes > _config.max_bytes;
                auto res = done ? ss::stop_iteration::yes
                                : ss::stop_iteration::no;
                /*TODO: remove*/ vlog(
                  cd_log.debug, "NEEDLER: JRBRI.consumer return {}", res);
                co_return res;
            }

            void end_of_stream() {}
            storage::log_reader_config& _config;
            ss::circular_buffer<model::record_batch>& _read_buffer;
            size_t _num_bytes{0};
        };

        // We need to rewrite the storage::log_reader. The storage::log_reader
        // takes max_bytes into account by tracking total size of all consumed
        // batches. In case of dl_placeholder batches the size should actually
        // come from the dl_placeholder.size_bytes field.

        /*TODO: remove*/ vlog(
          cd_log.debug,
          "NEEDLER: JRBRI consume from the underlying reader {}",
          _config);
        ss::circular_buffer<model::record_batch> read_buffer;
        consumer cons(_config, read_buffer);
        co_await _underlying.consume(cons, tm);

        /*TODO: remove*/ vlog(
          cd_log.debug, "NEEDLER: JRBRI materialize sorted run");
        // TODO: cache extents in the reader
        auto extents = co_await materialize_sorted_run(
          std::move(read_buffer), _bucket, &_api, &_cache);
        ss::circular_buffer<model::record_batch> slice;
        for (auto& e : extents) {
            auto b = materialize_single_raft_data_batch(e);
            /*TODO: remove*/ vlog(
              cd_log.debug, "NEEDLER: JRBRI materialize batch {}", b.header());
            slice.push_back(std::move(b));
        }

        /*TODO: remove*/ vlog(
          cd_log.debug, "NEEDLER: JRBRI return {} batches", slice.size());
        co_return std::move(slice);
    }

    void print(std::ostream& o) override { o << "joining_record_batch_reader"; }

    ss::future<> finally() noexcept override { return ss::now(); }

private:
    // Underlying storage partition reader
    model::record_batch_reader _underlying;
    storage::log_reader_config _config;
    cloud_storage_clients::bucket_name _bucket;
    // TODO: cache extent so it can be reused with the reader
    // cloud storage apis
    cloud_io::remote_api<ss::lowres_clock>& _api;
    cloud_storage::cloud_storage_cache_api& _cache;
};

// TODO: implement caching of readers
model::record_batch_reader make_aggregated_log_reader(
  storage::log_reader_config cfg,
  cloud_storage_clients::bucket_name bucket,
  model::record_batch_reader underlying,
  cloud_io::remote_api<ss::lowres_clock>& api,
  cloud_storage::cloud_storage_cache_api& cache) {
    auto impl = std::make_unique<joining_record_batch_reader_impl>(
      std::move(underlying), cfg, std::move(bucket), api, cache);
    return model::record_batch_reader(std::move(impl));
}

} // namespace cloud_data
