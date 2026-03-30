/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/lsm/debug_reader.h"

#include "cloud_topics/level_one/metastore/lsm/values.h"
#include "cloud_topics/level_one/metastore/state.h"
#include "serde/rw/rw.h"
#include "strings/string_switch.h"
#include "utils/uuid.h"

namespace cloud_topics::l1 {

using error = debug_reader::error;
using errc = debug_reader::errc;

namespace {

std::optional<row_type> parse_row_type(std::string_view raw_key) {
    // First 2 hex chars encode the row_type byte.
    if (raw_key.size() < 2) {
        return std::nullopt;
    }
    return string_switch<std::optional<row_type>>(raw_key.substr(0, 2))
      .match("00", row_type::metadata)
      .match("01", row_type::extent)
      .match("02", row_type::term_start)
      .match("03", row_type::compaction)
      .match("04", row_type::object)
      .default_match(std::nullopt);
}

ss::sstring uuid_to_string(const uuid_t& u) { return ss::sstring(u); }

proto::admin::metastore::row_key
make_metadata_proto_key(const metadata_row_key& k) {
    proto::admin::metastore::row_key pk;
    proto::admin::metastore::metadata_key mk;
    mk.set_topic_id(uuid_to_string(k.tidp.topic_id()));
    mk.set_partition_id(k.tidp.partition());
    pk.set_metadata(std::move(mk));
    return pk;
}

proto::admin::metastore::row_key
make_extent_proto_key(const extent_row_key& k) {
    proto::admin::metastore::row_key pk;
    proto::admin::metastore::extent_key ek;
    ek.set_topic_id(uuid_to_string(k.tidp.topic_id()));
    ek.set_partition_id(k.tidp.partition());
    ek.set_base_offset(k.base_offset());
    pk.set_extent(std::move(ek));
    return pk;
}

proto::admin::metastore::row_key make_term_proto_key(const term_row_key& k) {
    proto::admin::metastore::row_key pk;
    proto::admin::metastore::term_key tk;
    tk.set_topic_id(uuid_to_string(k.tidp.topic_id()));
    tk.set_partition_id(k.tidp.partition());
    tk.set_term_id(k.term());
    pk.set_term(std::move(tk));
    return pk;
}

proto::admin::metastore::row_key
make_compaction_proto_key(const compaction_row_key& k) {
    proto::admin::metastore::row_key pk;
    proto::admin::metastore::compaction_key ck;
    ck.set_topic_id(uuid_to_string(k.tidp.topic_id()));
    ck.set_partition_id(k.tidp.partition());
    pk.set_compaction(std::move(ck));
    return pk;
}

proto::admin::metastore::row_key
make_object_proto_key(const object_row_key& k) {
    proto::admin::metastore::row_key pk;
    proto::admin::metastore::object_key ok;
    ok.set_object_id(uuid_to_string(k.oid()));
    pk.set_object(std::move(ok));
    return pk;
}

} // namespace

std::expected<debug_reader::decoded_key, error>
debug_reader::decode_key(std::string_view raw_key) {
    auto type_opt = parse_row_type(raw_key);
    if (!type_opt) {
        return std::unexpected(
          error(errc::unknown_row_type, "key: {}", raw_key));
    }
    auto type = *type_opt;

    switch (type) {
    case row_type::metadata: {
        auto dk = metadata_row_key::decode(raw_key);
        if (!dk) {
            return std::unexpected(
              error(errc::decode_failure, "metadata key: {}", raw_key));
        }
        return decoded_key{type, make_metadata_proto_key(*dk)};
    }
    case row_type::extent: {
        auto dk = extent_row_key::decode(raw_key);
        if (!dk) {
            return std::unexpected(
              error(errc::decode_failure, "extent key: {}", raw_key));
        }
        return decoded_key{type, make_extent_proto_key(*dk)};
    }
    case row_type::term_start: {
        auto dk = term_row_key::decode(raw_key);
        if (!dk) {
            return std::unexpected(
              error(errc::decode_failure, "term key: {}", raw_key));
        }
        return decoded_key{type, make_term_proto_key(*dk)};
    }
    case row_type::compaction: {
        auto dk = compaction_row_key::decode(raw_key);
        if (!dk) {
            return std::unexpected(
              error(errc::decode_failure, "compaction key: {}", raw_key));
        }
        return decoded_key{type, make_compaction_proto_key(*dk)};
    }
    case row_type::object: {
        auto dk = object_row_key::decode(raw_key);
        if (!dk) {
            return std::unexpected(
              error(errc::decode_failure, "object key: {}", raw_key));
        }
        return decoded_key{type, make_object_proto_key(*dk)};
    }
    }
    return std::unexpected(error(errc::unknown_row_type, "key: {}", raw_key));
}

std::expected<proto::admin::metastore::row_value, error>
debug_reader::decode_value(row_type type, iobuf value) {
    proto::admin::metastore::row_value pv;
    try {
        switch (type) {
        case row_type::metadata: {
            auto rv = serde::from_iobuf<metadata_row_value>(std::move(value));
            proto::admin::metastore::metadata_value mv;
            mv.set_start_offset(rv.start_offset());
            mv.set_next_offset(rv.next_offset());
            mv.set_compaction_epoch(rv.compaction_epoch());
            mv.set_size(rv.size);
            mv.set_num_extents(rv.num_extents);
            pv.set_metadata(std::move(mv));
            return pv;
        }
        case row_type::extent: {
            auto rv = serde::from_iobuf<extent_row_value>(std::move(value));
            proto::admin::metastore::extent_value ev;
            ev.set_last_offset(rv.last_offset());
            ev.set_max_timestamp(rv.max_timestamp());
            ev.set_filepos(rv.filepos);
            ev.set_len(rv.len);
            ev.set_object_id(uuid_to_string(rv.oid()));
            pv.set_extent(std::move(ev));
            return pv;
        }
        case row_type::term_start: {
            auto rv = serde::from_iobuf<term_row_value>(std::move(value));
            proto::admin::metastore::term_value tv;
            tv.set_term_start_offset(rv.term_start_offset());
            pv.set_term(std::move(tv));
            return pv;
        }
        case row_type::compaction: {
            auto rv = serde::from_iobuf<compaction_row_value>(std::move(value));
            proto::admin::metastore::compaction_value cv;
            auto stream = rv.state.cleaned_ranges.make_stream();
            while (stream.has_next()) {
                auto interval = stream.next();
                proto::admin::metastore::offset_range r;
                r.set_base_offset(interval.base_offset());
                r.set_last_offset(interval.last_offset());
                cv.get_cleaned_ranges().push_back(std::move(r));
            }
            for (const auto& crt : rv.state.cleaned_ranges_with_tombstones) {
                proto::admin::metastore::cleaned_range_with_tombstones tr;
                tr.set_base_offset(crt.base_offset());
                tr.set_last_offset(crt.last_offset());
                tr.set_cleaned_with_tombstones_at(
                  crt.cleaned_with_tombstones_at());
                cv.get_cleaned_ranges_with_tombstones().push_back(
                  std::move(tr));
            }
            pv.set_compaction(std::move(cv));
            return pv;
        }
        case row_type::object: {
            auto rv = serde::from_iobuf<object_row_value>(std::move(value));
            proto::admin::metastore::object_value ov;
            ov.set_total_data_size(rv.object.total_data_size);
            ov.set_removed_data_size(rv.object.removed_data_size);
            ov.set_footer_pos(rv.object.footer_pos);
            ov.set_object_size(rv.object.object_size);
            ov.set_last_updated(rv.object.last_updated());
            ov.set_is_preregistration(rv.object.is_preregistration);
            pv.set_object(std::move(ov));
            return pv;
        }
        }
    } catch (const std::exception& ex) {
        return std::unexpected(
          error(errc::decode_failure, "value decode: {}", ex.what()));
    }
    return std::unexpected(error(errc::unknown_row_type, "unknown row type"));
}

std::expected<proto::admin::metastore::read_rows_response, error>
debug_reader::build_response(
  const chunked_vector<write_batch_row>& rows,
  std::optional<ss::sstring> next_key) {
    proto::admin::metastore::read_rows_response resp;
    for (const auto& row : rows) {
        auto dk_res = decode_key(row.key);
        if (!dk_res) {
            return std::unexpected(std::move(dk_res.error()));
        }
        proto::admin::metastore::read_row rr;
        rr.set_key(std::move(dk_res->key));

        if (row.value.size_bytes() > 0) {
            auto val_res = decode_value(dk_res->type, row.value.copy());
            if (!val_res) {
                return std::unexpected(std::move(val_res.error()));
            }
            rr.set_value(std::move(*val_res));
        }

        resp.get_rows().push_back(std::move(rr));
    }
    if (next_key.has_value()) {
        resp.set_next_key(std::move(*next_key));
    }
    return resp;
}

} // namespace cloud_topics::l1
