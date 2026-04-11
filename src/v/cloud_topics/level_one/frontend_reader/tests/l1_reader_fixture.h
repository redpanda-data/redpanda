/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "bytes/iostream.h"
#include "cloud_topics/level_one/common/fake_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/frontend_reader/l1_reader_cache.h"
#include "cloud_topics/level_one/frontend_reader/level_one_reader.h"
#include "cloud_topics/level_one/frontend_reader/level_one_reader_probe.h"
#include "cloud_topics/level_one/metastore/simple_metastore.h"
#include "cloud_topics/log_reader_config.h"
#include "container/chunked_circular_buffer.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <chrono>
#include <map>
#include <optional>

using namespace cloud_topics;
using namespace std::chrono_literals;

namespace cloud_topics::l1 {

class l1_reader_fixture : public seastar_test {
protected:
    using tidp_batches_t = std::pair<
      model::topic_id_partition,
      chunked_circular_buffer<model::record_batch>>;

    std::pair<model::ntp, model::topic_id_partition>
    make_ntidp(std::string_view topic_name) {
        static constexpr auto test_namespace = "test_ns";
        static constexpr model::partition_id test_partition_id{0};

        auto ntp = model::ntp{
          model::ns{test_namespace},
          model::topic{topic_name},
          test_partition_id};
        auto tidp = model::topic_id_partition{
          model::topic_id{uuid_t::create()}, test_partition_id};
        return std::make_pair(ntp, tidp);
    }

    ss::future<> make_l1_objects(std::vector<tidp_batches_t> batches_by_tidp) {
        auto meta_builder = (co_await _metastore.object_builder()).value();

        // First record the object ID for each tidp,
        std::map<model::topic_id_partition, l1::object_id> oid_by_tidp;
        for (auto& [tidp, unused] : batches_by_tidp) {
            oid_by_tidp[tidp]
              = (co_await meta_builder->get_or_create_object_for(tidp)).value();
        }

        // Then create output streams and builders for each object.
        std::map<l1::object_id, iobuf> bufs_by_oid;
        std::map<l1::object_id, std::unique_ptr<l1::object_builder>>
          builders_by_oid;
        for (auto& [unused, oid] : oid_by_tidp) {
            if (!builders_by_oid.contains(oid)) {
                bufs_by_oid[oid] = iobuf{};
                builders_by_oid[oid] = l1::object_builder::create(
                  make_iobuf_ref_output_stream(bufs_by_oid[oid]), {});
            }
        }

        // Create the term offset map first before consuming batches.
        l1::metastore::term_offset_map_t term_map;
        for (auto& [tidp, batches] : batches_by_tidp) {
            term_map[tidp].push_back(
              l1::metastore::term_offset{
                .term = model::term_id{1},
                .first_offset = model::offset_cast(
                  batches.front().base_offset()),
              });
        }

        // Load each ntp's batches into the object.
        for (auto& [tidp, batches] : batches_by_tidp) {
            auto& oid = oid_by_tidp[tidp];
            auto& builder = builders_by_oid[oid];
            co_await builder->start_partition(tidp);
            for (auto& batch : batches) {
                co_await builder->add_batch(std::move(batch));
            }
        }

        // Finish all the objects, upload them, and use the metadata
        // to prepare the metastore registration.
        for (auto& [oid, builder] : builders_by_oid) {
            auto obj_info = co_await builder->finish();
            co_await builder->close();

            _io.put_object(oid, std::move(bufs_by_oid[oid]));

            for (auto& [tidp, partition] : obj_info.index.partitions) {
                meta_builder
                  ->add(
                    oid,
                    l1::metastore::object_metadata::ntp_metadata{
                      .tidp = tidp,
                      .base_offset = partition.first_offset,
                      .last_offset = partition.last_offset,
                      .max_timestamp = partition.max_timestamp,
                      .pos = partition.file_position,
                      .size = partition.length,
                    })
                  .value();
            }

            meta_builder
              ->finish(oid, obj_info.footer_offset, obj_info.size_bytes)
              .value();
        }

        auto res = co_await _metastore.add_objects(*meta_builder, term_map);
    }

    model::record_batch_reader make_reader(
      const model::ntp& ntp,
      const model::topic_id_partition& tidp,
      kafka::offset start_offset = kafka::offset{0},
      kafka::offset max_offset = kafka::offset::max(),
      size_t max_bytes = std::numeric_limits<size_t>::max(),
      bool strict_max_bytes = false,
      size_t lookahead_objects = 0) {
        cloud_topic_log_reader_config config(
          start_offset,
          max_offset,
          /*min_bytes=*/0, // min_bytes
          max_bytes,
          /*type_filter=*/std::nullopt,
          /*first_timestamp=*/std::nullopt,
          /*abort_source=*/std::nullopt,
          /*client_addr=*/std::nullopt,
          /*strict_max_bytes=*/strict_max_bytes);
        config.lookahead_objects = lookahead_objects;
        return model::record_batch_reader(
          std::make_unique<level_one_log_reader_impl>(
            config, ntp, tidp, &_metastore, &_io, nullptr, _cache_ptr));
    }

    chunked_circular_buffer<model::record_batch>
    read_all(model::record_batch_reader reader) {
        auto data = model::consume_reader_to_memory(
                      std::move(reader), model::no_timeout)
                      .get();
        chunked_circular_buffer<model::record_batch> result;
        std::move(data.begin(), data.end(), std::back_inserter(result));
        return result;
    }

    ss::future<> TearDownAsync() override { co_await _cache.stop(); }

    l1::simple_metastore _metastore{};
    l1::fake_io _io{};
    l1_reader_cache _cache{};
    l1_reader_cache* _cache_ptr = &_cache;
};

} // namespace cloud_topics::l1
