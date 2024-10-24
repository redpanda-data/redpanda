/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "model/transform.h"

#include "bytes/iobuf_parser.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "serde/rw/bool_class.h"
#include "serde/rw/map.h"
#include "serde/rw/rw.h"
#include "serde/rw/uuid.h"
#include "serde/rw/vector.h"
#include "utils/vint.h"

#include <seastar/core/print.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/variant_utils.hh>

#include <algorithm>
#include <optional>
#include <stdexcept>

namespace model {

namespace {

bool validate_kv(iobuf_const_parser* parser) {
    auto [key_length, kl] = parser->read_varlong();
    // -1 is valid size of a `null` record, and is used to distinguish between
    // `null` and empty bytes.
    if (key_length < -1) {
        return false;
    }
    if (key_length > 0) {
        parser->skip(key_length);
    }
    auto [value_length, vl] = parser->read_varlong();
    if (value_length < -1) {
        return false;
    }
    if (value_length > 0) {
        parser->skip(value_length);
    }
    return true;
}

bool validate_record_payload(const iobuf& buf) {
    iobuf_const_parser parser(buf);
    if (!validate_kv(&parser)) {
        return false;
    }
    auto [header_count, hc] = parser.read_varlong();
    if (header_count < 0) {
        return false;
    }
    for (int64_t i = 0; i < header_count; ++i) {
        if (!validate_kv(&parser)) {
            return false;
        }
    }
    return parser.bytes_left() == 0;
}

void append_vint_to_iobuf(iobuf& b, int64_t v) {
    auto vb = vint::to_bytes(v);
    b.append(vb.data(), vb.size());
}

} // namespace

std::ostream& operator<<(std::ostream& os, const transform_from_start& o) {
    fmt::print(os, "{{ start + {} }}", o.delta);
    return os;
}

std::ostream& operator<<(std::ostream& os, const transform_from_end& o) {
    fmt::print(os, "{{ end - {} }}", o.delta);
    return os;
}

std::ostream&
operator<<(std::ostream& os, const transform_offset_options& opts) {
    ss::visit(
      opts.position,
      [&os](transform_offset_options::latest_offset) {
          fmt::print(os, "{{ latest_offset }}");
      },
      [&os](model::timestamp ts) {
          fmt::print(os, "{{ timequery: {} }}", ts.value());
      },
      [&os](model::transform_from_start off) {
          fmt::print(os, "{{ offset: {} }}", off);
      },
      [&os](model::transform_from_end off) {
          fmt::print(os, "{{ offset: {} }}", off);
      });
    return os;
}

/**
 * Legacy version of transform_offset_options
 *
 * When writing out transform metadata, we write this version if and
 * only if the position variant contains one of the legacy alternatives.
 * This allows us to feature-gate the use of the new alternatives
 * (see model/transform.h) without completely blocking transform deploys
 * during an upgrade.
 */
struct legacy_transform_offset_options
  : serde::envelope<
      legacy_transform_offset_options,
      serde::version<0>,
      serde::compat_version<0>> {
    serde::variant<transform_offset_options::latest_offset, model::timestamp>
      position;
    bool operator==(const legacy_transform_offset_options&) const = default;

    void serde_write(iobuf& out) const { serde::write(out, position); }
};

bool transform_offset_options::is_legacy_compat() const {
    return std::holds_alternative<transform_offset_options::latest_offset>(
             position)
           || std::holds_alternative<model::timestamp>(position);
}

void transform_offset_options::serde_read(
  iobuf_parser& in, const serde::header& h) {
    using serde::read_nested;

    if (h._version == 0) {
        auto p = read_nested<serde::variant<latest_offset, model::timestamp>>(
          in, h._bytes_left_limit);
        ss::visit(p, [this](auto v) { position = v; });
    } else {
        position = read_nested<decltype(position)>(in, h._bytes_left_limit);
    }

    if (in.bytes_left() > h._bytes_left_limit) {
        in.skip(in.bytes_left() - h._bytes_left_limit);
    }
}

void transform_offset_options::serde_write(iobuf& out) const {
    serde::write(out, position);
}

std::ostream& operator<<(std::ostream& os, const transform_metadata& meta) {
    fmt::print(
      os,
      "{{name: \"{}\", input: {}, outputs: {}, "
      "env: <redacted>, uuid: {}, source_ptr: {}, is_paused: {} }}",
      meta.name,
      meta.input_topic,
      meta.output_topics,
      // skip env becuase of pii
      meta.uuid,
      meta.source_ptr,
      meta.paused);
    return os;
}

void transform_metadata::serde_write(iobuf& out) const {
    serde::write(out, name);
    write(out, input_topic);
    serde::write(out, output_topics);
    serde::write(out, environment);
    serde::write(out, uuid);
    serde::write(out, source_ptr);
    ss::visit(
      offset_options.position,
      [&out](transform_offset_options::latest_offset v) {
          serde::write(out, legacy_transform_offset_options{.position = v});
      },
      [&out](model::timestamp v) {
          serde::write(out, legacy_transform_offset_options{.position = v});
      },
      [this, &out](auto) { serde::write(out, offset_options); });
    serde::write(out, paused);
    serde::write(out, compression_mode);
}

void transform_metadata::serde_read(iobuf_parser& in, const serde::header& h) {
    using serde::read_nested;

    name = read_nested<decltype(name)>(in, h._bytes_left_limit);
    input_topic = read_nested<decltype(input_topic)>(in, h._bytes_left_limit);
    output_topics = read_nested<decltype(output_topics)>(
      in, h._bytes_left_limit);
    environment = read_nested<decltype(environment)>(in, h._bytes_left_limit);
    uuid = read_nested<decltype(uuid)>(in, h._bytes_left_limit);
    source_ptr = read_nested<decltype(source_ptr)>(in, h._bytes_left_limit);

    if (h._version >= 1) {
        offset_options = read_nested<decltype(offset_options)>(
          in, h._bytes_left_limit);
    }
    if (h._version >= 2) {
        paused = read_nested<decltype(paused)>(in, h._bytes_left_limit);
        compression_mode = read_nested<decltype(compression_mode)>(
          in, h._bytes_left_limit);
    }
}

bool transform_metadata_patch::empty() const noexcept {
    return !env.has_value() && !paused.has_value()
           && !compression_mode.has_value();
}

std::ostream& operator<<(std::ostream& os, const transform_offsets_key& key) {
    fmt::print(
      os,
      "{{ transform id: {}, partition: {}, output_topic: {} }}",
      key.id,
      key.partition,
      key.output_topic);
    return os;
}

void transform_offsets_key::serde_read(
  iobuf_parser& in, const serde::header& h) {
    using serde::read_nested;
    id = read_nested<model::transform_id>(in, h._bytes_left_limit);
    partition = read_nested<model::partition_id>(in, h._bytes_left_limit);
    if (h._version == 0) {
        // Only supported a single output topic
        output_topic = output_topic_index(0);
    } else {
        output_topic = read_nested<output_topic_index>(in, h._bytes_left_limit);
    }
    // Skip unknown future fields.
    if (in.bytes_left() > h._bytes_left_limit) {
        in.skip(in.bytes_left() - h._bytes_left_limit);
    }
}

void transform_offsets_key::serde_write(iobuf& out) const {
    serde::write(out, id);
    serde::write(out, partition);
    serde::write(out, output_topic);
}

std::ostream&
operator<<(std::ostream& os, const transform_offsets_value& value) {
    fmt::print(os, "{{ offset: {} }}", value.offset);
    return os;
}

std::ostream&
operator<<(std::ostream& os, const transform_report::processor& p) {
    fmt::print(
      os,
      "{{id: {}, status: {}, node: {}, lag: {}}}",
      p.id,
      p.status,
      p.node,
      p.lag);
    return os;
}

transform_report::transform_report(transform_metadata meta)
  : metadata(std::move(meta))
  , processors() {}

transform_report::transform_report(
  transform_metadata meta, absl::btree_map<model::partition_id, processor> map)
  : metadata(std::move(meta))
  , processors(std::move(map)) {};

void transform_report::add(processor processor) {
    processors.insert_or_assign(processor.id, processor);
}

void cluster_transform_report::add(
  transform_id id,
  const transform_metadata& meta,
  transform_report::processor processor) {
    auto [it, _] = transforms.try_emplace(id, meta);
    it->second.add(processor);
}

void cluster_transform_report::merge(const cluster_transform_report& other) {
    for (const auto& [tid, treport] : other.transforms) {
        for (const auto& [pid, preport] : treport.processors) {
            add(tid, treport.metadata, preport);
        }
    }
}

std::ostream&
operator<<(std::ostream& os, transform_report::processor::state s) {
    return os << processor_state_to_string(s);
}

std::string_view
processor_state_to_string(transform_report::processor::state state) {
    switch (state) {
    case transform_report::processor::state::inactive:
        return "inactive";
    case transform_report::processor::state::running:
        return "running";
    case transform_report::processor::state::errored:
        return "errored";
    case transform_report::processor::state::unknown:
        break;
    }
    return "unknown";
}

transformed_data::transformed_data(iobuf d)
  : _data(std::move(d)) {}

std::optional<transformed_data> transformed_data::create_validated(iobuf buf) {
    try {
        if (!validate_record_payload(buf)) {
            return std::nullopt;
        }
    } catch (const std::out_of_range&) {
        return std::nullopt;
    }
    return transformed_data(std::move(buf));
}

model::record_batch transformed_data::make_batch(
  model::timestamp timestamp, ss::chunked_fifo<transformed_data> records) {
    model::record_batch::compressed_records serialized_records;
    int32_t i = 0;
    for (model::transformed_data& r : records) {
        serialized_records.append_fragments(std::move(r).to_serialized_record(
          model::record_attributes(),
          /*timestamp_delta=*/0,
          /*offset_delta=*/i++));
    }

    model::record_batch_header header;
    header.type = record_batch_type::raft_data;
    // mark the batch as created with broker time
    header.attrs.set_timestamp_type(model::timestamp_type::append_time);
    header.first_timestamp = timestamp;
    header.max_timestamp = timestamp;
    // disable idempotent producing, we don't currently use that within
    // transforms.
    header.producer_id = -1;

    header.last_offset_delta = i - 1;
    header.record_count = i;
    header.size_bytes = int32_t(
      model::packed_record_batch_header_size + serialized_records.size_bytes());

    auto batch = model::record_batch(
      header,
      std::move(serialized_records),
      model::record_batch::tag_ctor_ng{});

    // Recompute the crc
    batch.header().crc = model::crc_record_batch(batch);
    batch.header().header_crc = model::internal_header_only_crc(batch.header());

    return batch;
}

transformed_data transformed_data::from_record(record r) {
    iobuf payload;
    append_vint_to_iobuf(payload, r.key_size());
    payload.append(r.release_key());
    append_vint_to_iobuf(payload, r.value_size());
    payload.append(r.release_value());
    append_vint_to_iobuf(payload, int64_t(r.headers().size()));
    for (auto& header : r.headers()) {
        append_vint_to_iobuf(payload, header.key_size());
        payload.append(header.release_key());
        append_vint_to_iobuf(payload, header.value_size());
        payload.append(header.release_value());
    }
    return transformed_data(std::move(payload));
}

iobuf transformed_data::to_serialized_record(
  record_attributes attrs, int64_t timestamp_delta, int32_t offset_delta) && {
    iobuf out;
    out.reserve_memory(sizeof(attrs) + vint::max_length * 3);
    // placeholder for the final length.
    auto placeholder = out.reserve(vint::max_length);

    const auto attr = ss::cpu_to_be(attrs.value());
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    out.append(reinterpret_cast<const char*>(&attr), sizeof(attr));

    bytes td = vint::to_bytes(timestamp_delta);
    out.append(td.data(), td.size());

    bytes od = vint::to_bytes(offset_delta);
    out.append(od.data(), od.size());

    out.append_fragments(std::move(_data));

    bytes encoded_size = vint::to_bytes(
      int64_t(out.size_bytes() - vint::max_length));

    // Write out the size at the end of the reserved space we took at the
    // beginning.
    placeholder.write_end(encoded_size.data(), encoded_size.size());
    // drop the bytes we reserved, but didn't use.
    out.trim_front(vint::max_length - encoded_size.size());

    return out;
}

size_t transformed_data::memory_usage() const {
    return sizeof(*this) + _data.size_bytes();
}

transformed_data transformed_data::copy() const {
    return transformed_data(_data.copy());
}

wasm_binary_iobuf share_wasm_binary(const wasm_binary_iobuf& b) {
    vassert(
      b().get_owner_shard() == ss::this_shard_id(),
      "cannot share a wasm binary from another shard");
    const auto& underlying = b();
    return wasm_binary_iobuf(
      std::make_unique<iobuf>(underlying->share(0, underlying->size_bytes())));
}

void tag_invoke(
  serde::tag_t<serde::read_tag>,
  iobuf_parser& in,
  wasm_binary_iobuf& t,
  const std::size_t bytes_left_limit) {
    auto size = serde::read_nested<serde::serde_size_t>(in, bytes_left_limit);
    t = wasm_binary_iobuf(std::make_unique<iobuf>(in.share(size)));
}

void tag_invoke(
  serde::tag_t<serde::write_tag>, iobuf& out, wasm_binary_iobuf t) {
    if (!t()) {
        serde::write<serde::serde_size_t>(out, 0);
        return;
    }
    serde::write<serde::serde_size_t>(out, t()->size_bytes());
    // If we're on the same shard we can enable move fragment optimizations
    if (t().get_owner_shard() == ss::this_shard_id()) {
        out.append(std::move(*t()));
    } else {
        // Otherwise we can't touch the memory and we need to do a full copy to
        // this core.
        for (const auto& fragment : *t()) {
            out.append(fragment.get(), fragment.size());
        }
    }
}

} // namespace model
