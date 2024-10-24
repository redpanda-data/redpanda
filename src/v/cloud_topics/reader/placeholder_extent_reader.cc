/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/reader/placeholder_extent_reader.h"

#include "cloud_io/basic_cache_service_api.h"
#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_topics/dl_placeholder.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/reader/placeholder_extent.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/coroutine/as_future.hh>

#include <chrono>
#include <exception>

using namespace std::chrono_literals;

namespace experimental::cloud_topics {

/// Convert record batch to dl_placeholder value and return an empty
/// extent which later has to be hydrated.
size_t get_extent_size(const model::record_batch& placeholder) {
    /// FIXME: This code makes unnecessary copy
    vassert(
      placeholder.header().type == model::record_batch_type::dl_placeholder,
      "Unsupported batch type {}",
      placeholder.header());
    iobuf payload = placeholder.data().copy();
    iobuf_parser parser(std::move(payload));
    auto record = model::parse_one_record_from_buffer(parser);
    iobuf value = std::move(record).release_value();
    auto p = serde::from_iobuf<dl_placeholder>(std::move(value));
    return p.size_bytes;
}

ss::future<result<ss::circular_buffer<placeholder_extent>>>
materialize_sorted_run(
  ss::circular_buffer<model::record_batch> placeholders,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* api,
  cloud_io::basic_cache_service_api<>* cache,
  retry_chain_node* rtc) {
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
            auto res = co_await materialize(&extent, bucket, api, cache, rtc);
            if (res.has_error()) {
                co_return res.error();
            }
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
      const storage::log_reader_config& cfg,
      cloud_storage_clients::bucket_name bucket,
      cloud_io::remote_api<>& api,
      cloud_io::basic_cache_service_api<>& cache,
      retry_chain_node* rtc)
      : _underlying(std::move(rdr))
      , _config(cfg)
      , _bucket(std::move(bucket))
      , _api(api)
      , _cache(cache)
      , _rtc(rtc) {}

    bool is_end_of_stream() const override {
        bool is_eos = _config.start_offset >= _config.max_offset
                      || _underlying.is_end_of_stream();
        return is_eos;
    }

    ss::future<model::record_batch_reader::storage_t>
    do_load_slice(model::timeout_clock::time_point tm) override {
        struct consumer {
            consumer(
              storage::log_reader_config* config,
              ss::circular_buffer<model::record_batch>* rb)
              : _config(config)
              , _read_buffer(rb) {}

            ss::future<ss::stop_iteration> operator()(model::record_batch rb) {
                if (rb.header().base_offset < _config->start_offset) {
                    // Skip data which was already consumed
                    co_return ss::stop_iteration::no;
                }
                if (rb.header().last_offset() > _config->max_offset) {
                    // Stop on EOS
                    co_return ss::stop_iteration::yes;
                }
                // Account bytes based on placeholder content, not header
                _num_bytes += get_extent_size(rb);
                auto last_offset = rb.header().last_offset();
                _read_buffer->push_back(std::move(rb));
                _config->start_offset = model::next_offset(last_offset);
                bool done = _config->start_offset >= _config->max_offset
                            || _num_bytes > _config->max_bytes;
                auto res = done ? ss::stop_iteration::yes
                                : ss::stop_iteration::no;
                co_return res;
            }

            void end_of_stream() {}

        private:
            storage::log_reader_config* _config;
            ss::circular_buffer<model::record_batch>* _read_buffer;
            size_t _num_bytes{0};
        };

        ss::circular_buffer<model::record_batch> read_buffer;
        consumer cons(&_config, &read_buffer);
        co_await _underlying.consume(cons, tm);

        auto extents = co_await materialize_sorted_run(
          std::move(read_buffer), _bucket, &_api, &_cache, &_rtc);
        if (extents.has_error()) {
            vlog(
              cd_log.error,
              "Failed to materialize sorted run: {}",
              extents.error().message());
            throw std::system_error(extents.error());
        }

        ss::circular_buffer<model::record_batch> slice;
        for (auto& e : extents.value()) {
            slice.push_back(make_raft_data_batch(std::move(e)));
        }

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
    cloud_io::remote_api<>& _api;
    cloud_io::basic_cache_service_api<>& _cache;
    retry_chain_node _rtc;
};

model::record_batch_reader make_placeholder_extent_reader(
  storage::log_reader_config cfg,
  cloud_storage_clients::bucket_name bucket,
  model::record_batch_reader underlying,
  cloud_io::remote_api<ss::lowres_clock>& api,
  cloud_io::basic_cache_service_api<ss::lowres_clock>& cache,
  retry_chain_node& rtc) {
    auto impl = std::make_unique<joining_record_batch_reader_impl>(
      std::move(underlying), cfg, std::move(bucket), api, cache, &rtc);
    return model::record_batch_reader(std::move(impl));
}

} // namespace experimental::cloud_topics
