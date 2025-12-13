/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/lsm/state_reader.h"

#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "lsm/core/exceptions.h"
#include "ssx/future-util.h"

#include <exception>

namespace cloud_topics::l1 {

namespace {

state_reader::errc to_errc(std::exception_ptr e) {
    if (ssx::is_shutdown_exception(e)) {
        return state_reader::errc::shutting_down;
    }
    try {
        std::rethrow_exception(e);
    } catch (lsm::abort_requested_exception&) {
        return state_reader::errc::shutting_down;
    } catch (lsm::corruption_exception&) {
        return state_reader::errc::corruption;
    } catch (...) {
        return state_reader::errc::io_error;
    }
}

model::topic_id_partition next_partition(const model::topic_id_partition& tp) {
    return model::topic_id_partition(
      tp.topic_id, model::partition_id(tp.partition() + 1));
}

bool is_at_extent(
  lsm::iterator& iter,
  const model::topic_id_partition& tidp,
  extent_row_key* out = nullptr) {
    if (!iter.valid()) {
        return false;
    }
    auto key = extent_row_key::decode(iter.key());
    if (!key.has_value() || key->tidp != tidp) {
        return false;
    }
    if (out) {
        *out = key.value();
    }
    return true;
}

} // namespace

ss::coroutine::experimental::generator<
  std::expected<state_reader::extent_row, state_reader::errc>>
state_reader::extent_key_range::get_rows() {
    auto fut = co_await ss::coroutine::as_future(_iter.seek(_base_key));
    if (fut.failed()) {
        auto ex = fut.get_exception();
        co_yield std::unexpected(to_errc(ex));
        co_return;
    }
    if (!_iter.valid() || _iter.key() != _base_key) {
        co_yield std::unexpected(errc::corruption);
        co_return;
    }
    while (_iter.valid()) {
        std::exception_ptr ex;
        try {
            auto val = serde::from_iobuf<extent_row_value>(_iter.value());
            co_yield extent_row{
              .key = ss::sstring(_iter.key()),
              .val = val,
            };
            if (_iter.key() == _last_key) {
                co_return;
            }
            if (_iter.key() > _last_key) {
                co_yield std::unexpected(errc::corruption);
                co_return;
            }
            co_await _iter.next();
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            co_yield std::unexpected(to_errc(ex));
            co_return;
        }
    }
}

ss::future<
  chunked_vector<std::expected<state_reader::extent_row, state_reader::errc>>>
state_reader::extent_key_range::materialize_rows() {
    auto gen = get_rows();
    chunked_vector<std::expected<extent_row, errc>> ret;
    while (auto res = co_await gen()) {
        if (!res.has_value()) {
            break;
        }
        ret.emplace_back(*res);
    }
    co_return ret;
}

ss::future<std::expected<std::optional<metadata_row_value>, state_reader::errc>>
state_reader::get_metadata(const model::topic_id_partition& tidp) {
    return get_val<metadata_row_key, metadata_row_value>(tidp);
}

ss::future<std::expected<std::optional<compaction_state>, state_reader::errc>>
state_reader::get_compaction_metadata(const model::topic_id_partition& tidp) {
    auto opt_val_res
      = co_await get_val<compaction_row_key, compaction_row_value>(tidp);
    if (!opt_val_res.has_value()) {
        co_return std::unexpected(opt_val_res.error());
    }
    if (!opt_val_res.value().has_value()) {
        co_return std::nullopt;
    }
    co_return opt_val_res.value()->state;
}

ss::future<std::expected<std::optional<object_entry>, state_reader::errc>>
state_reader::get_object(object_id oid) {
    auto opt_val_res = co_await get_val<object_row_key, object_row_value>(oid);
    if (!opt_val_res.has_value()) {
        co_return std::unexpected(opt_val_res.error());
    }
    if (!opt_val_res.value().has_value()) {
        co_return std::nullopt;
    }
    co_return opt_val_res.value()->object;
}

ss::future<std::expected<std::optional<term_start>, state_reader::errc>>
state_reader::get_max_term(const model::topic_id_partition& tidp) {
    iobuf val_buf;
    model::term_id term;
    try {
        // Seek to the first term of the next partition and then iterate
        // backwards to find the highest term of this partition.
        auto iter = co_await snap_.create_iterator();
        co_await iter.seek(
          term_row_key::encode(next_partition(tidp), model::term_id(0)));
        if (!iter.valid()) {
            co_await iter.seek_to_last();
        } else {
            co_await iter.prev();
        }
        if (!iter.valid()) {
            co_return std::nullopt;
        }
        auto key = term_row_key::decode(iter.key());
        if (!key.has_value() || key->tidp != tidp) {
            co_return std::nullopt;
        }
        term = key->term;
        val_buf = iter.value();
    } catch (...) {
        co_return std::unexpected(to_errc(std::current_exception()));
    }
    try {
        auto val = serde::from_iobuf<term_row_value>(std::move(val_buf));
        co_return term_start{
          .term_id = term, .start_offset = val.term_start_offset};
    } catch (...) {
        co_return std::unexpected(errc::corruption);
    }
}

ss::future<std::expected<std::optional<extent>, state_reader::errc>>
state_reader::get_extent_ge(
  const model::topic_id_partition& tidp, kafka::offset o) {
    iobuf val_buf;
    kafka::offset base_offset;
    try {
        auto iter = co_await snap_.create_iterator();
        // Seek to o+1 and iterate backwards to find the extent that may
        // contain o.
        auto next_key_str = extent_row_key::encode(tidp, kafka::next_offset(o));
        co_await iter.seek(next_key_str);
        if (iter.valid()) {
            co_await iter.prev();
            if (!is_at_extent(iter, tidp)) {
                // Either there are no rows below the seek result for extent
                // o+1, or the rows below aren't extents for this partition,
                // implying that no extents exist below o+1. The o+1 seek
                // result would be the next highest extent.
                co_await iter.seek(next_key_str);
            }
        } else {
            // All rows are below the key for extent o+1. If an extent for o
            // does exist, it will be the last row.
            co_await iter.seek_to_last();
        }

        if (!iter.valid()) {
            co_return std::nullopt;
        }

        auto key = extent_row_key::decode(iter.key());
        if (!key.has_value() || key->tidp != tidp) {
            co_return std::nullopt;
        }
        base_offset = key->base_offset;
        val_buf = iter.value();
    } catch (...) {
        co_return std::unexpected(to_errc(std::current_exception()));
    }
    try {
        auto val = serde::from_iobuf<extent_row_value>(std::move(val_buf));
        // Check if this extent contains or comes after the query offset
        if (o > val.last_offset) {
            // The offset is beyond this extent, no extent contains it
            co_return std::nullopt;
        }
        co_return extent{
          .base_offset = base_offset,
          .last_offset = val.last_offset,
          .max_timestamp = val.max_timestamp,
          .filepos = val.filepos,
          .len = val.len,
          .oid = val.oid,
        };
    } catch (...) {
        co_return std::unexpected(errc::corruption);
    }
}

ss::future<std::expected<
  std::optional<state_reader::extent_key_range>,
  state_reader::errc>>
state_reader::get_extent_range(
  const model::topic_id_partition& tidp,
  kafka::offset base,
  kafka::offset last) {
    ss::sstring base_key;
    ss::sstring last_key;
    iobuf last_val_buf;
    auto iter = co_await snap_.create_iterator();
    try {
        co_await iter.seek(extent_row_key::encode(tidp, base));
        extent_row_key key;
        if (!is_at_extent(iter, tidp, &key) || key.base_offset != base) {
            co_return std::nullopt;
        }
        base_key = ss::sstring(iter.key());

        // Start looking for the extent that ends with `last`. Seek just past
        // where we expect the extent to be and call prev() so if it exists, we
        // will be pointing at it.
        co_await iter.seek(
          extent_row_key::encode(tidp, kafka::next_offset(last)));
        if (!iter.valid()) {
            co_await iter.seek_to_last();
        } else {
            co_await iter.prev();
        }
        if (!is_at_extent(iter, tidp)) {
            co_return std::nullopt;
        }
        last_key = ss::sstring(iter.key());
        last_val_buf = iter.value();
    } catch (...) {
        co_return std::unexpected(to_errc(std::current_exception()));
    }
    try {
        auto val = serde::from_iobuf<extent_row_value>(std::move(last_val_buf));
        if (val.last_offset != last) {
            co_return std::nullopt;
        }
    } catch (...) {
        co_return std::unexpected(errc::corruption);
    }
    // Position iterator back at base_key to return to the caller.
    try {
        co_await iter.seek(base_key);
        dassert(iter.valid(), "Iterator became invalid for key: {}", base_key);
    } catch (...) {
        co_return std::unexpected(to_errc(std::current_exception()));
    }
    co_return extent_key_range(
      std::move(base_key), std::move(last_key), std::move(iter));
}

template<typename KeyT, typename ValT, typename... KeyEncodeArgs>
ss::future<std::expected<std::optional<ValT>, state_reader::errc>>
state_reader::get_val(KeyEncodeArgs... args) {
    auto fut = co_await ss::coroutine::as_future(
      snap_.get(KeyT::encode(args...)));
    if (fut.failed()) {
        co_return std::unexpected(to_errc(fut.get_exception()));
    }
    auto opt_buf = fut.get();
    if (!opt_buf.has_value()) {
        co_return std::nullopt;
    }
    try {
        auto val = serde::from_iobuf<ValT>(std::move(*opt_buf));
        co_return val;
    } catch (...) {
        co_return std::unexpected(errc::corruption);
    }
}

} // namespace cloud_topics::l1
