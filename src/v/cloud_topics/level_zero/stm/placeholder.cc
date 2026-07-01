/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_zero/stm/placeholder.h"

#include "model/record_batch_types.h"
#include "storage/record_batch_builder.h"

namespace cloud_topics {

model::record_batch encode_placeholder_batch(
  model::record_batch_header header, extent_meta extent) {
    vassert(
      header.record_count > 0, "Empty record batch not allowed {}", header);

    cloud_topics::ctp_placeholder placeholder{
      .id = extent.id,
      .offset = extent.first_byte_offset,
      .size_bytes = extent.byte_range_size,
    };

    storage::record_batch_builder builder(
      model::record_batch_type::ctp_placeholder, header.base_offset);

    builder.set_producer_identity(header.producer_id, header.producer_epoch);
    if (header.attrs.is_control()) {
        builder.set_control_type();
    }
    if (header.attrs.is_transactional()) {
        builder.set_transactional_type();
    }

    // In case of a placeholder batch the first record contains the
    // actual placeholder and the remaining records are empty. The remaining
    // records are added to avoid confusing any other code that may expect
    // that the number of records in the batch is equal to the number of
    // offsets in the header.
    auto first_value = serde::to_iobuf(placeholder);
    builder.add_raw_kv(std::nullopt, std::move(first_value));

    for (int i = 1; i < header.record_count; ++i) {
        builder.add_raw_kv(std::nullopt, std::nullopt);
    }

    auto ph = std::move(builder).build(
      storage::record_batch_builder::reset_size_checksum::no);
    // In order for timequeries to work correctly, we need to ensure we never
    // look inside the batch to answer the query. If the time is append time we
    // don't need to unpack the batch, but instead all records have the
    // max_timestamp as their timestamp. Since we're not mirroring the
    // timestamps from the data batch into this placeholder, we use append time
    // and will in the cloud topics frontend read the data batch to properly
    // determine the offset.
    ph.header().first_timestamp = header.max_timestamp;
    ph.header().max_timestamp = header.max_timestamp;
    ph.header().attrs.set_timestamp_type(model::timestamp_type::append_time);
    ph.header().base_sequence = header.base_sequence;
    ph.header().reset_size_checksum_metadata(ph.data());
    return ph;
}

ctp_placeholder parse_placeholder_batch(model::record_batch batch) {
    iobuf payload = std::move(batch).release_data();
    iobuf_parser parser(std::move(payload));
    auto record = model::parse_one_record_from_buffer(parser);
    iobuf value = std::move(record).release_value();
    auto placeholder = serde::from_iobuf<cloud_topics::ctp_placeholder>(
      std::move(value));
    return placeholder;
}

model::record_batch apply_placeholder_to_batch(
  const model::record_batch_header& placeholder_batch_header,
  model::record_batch uploaded_batch) {
    // crcs and sizes are set later in reset_size_checksum_metadata
    model::record_batch_header merged_header{
      .header_crc = 0,
      .size_bytes = 0,
      .base_offset = placeholder_batch_header.base_offset,
      .type = uploaded_batch.header().type,
      .crc = 0,
      // We need to use the same attributes for compression and timestamp types
      // which are changed in the placeholder batch
      .attrs = uploaded_batch.header().attrs,
      // Last offset delta should be the same in both headers
      .last_offset_delta = placeholder_batch_header.last_offset_delta,
      .first_timestamp = uploaded_batch.header().first_timestamp,
      .max_timestamp = uploaded_batch.header().max_timestamp,
      // producer_id, producer_epoch and base_sequence should be the same
      // in both headers as it's required for rm_stm
      .producer_id = placeholder_batch_header.producer_id,
      .producer_epoch = placeholder_batch_header.producer_epoch,
      .base_sequence = placeholder_batch_header.base_sequence,
      // This should be the same in both headers
      .record_count = placeholder_batch_header.record_count,
      .ctx = placeholder_batch_header.ctx,
    };
    merged_header.reset_size_checksum_metadata(uploaded_batch.data());
    return model::record_batch(
      merged_header,
      std::move(uploaded_batch).release_data(),
      model::record_batch::tag_ctor_ng{});
}

} // namespace cloud_topics
