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
#include "cloud_topics/logger.h"
#include "lsm/core/exceptions.h"
#include "ssx/future-util.h"

#include <exception>

namespace cloud_topics::l1 {

using errc = state_reader::errc;
using error = state_reader::error;

namespace {

error to_error(std::exception_ptr e, std::string_view prefix = "") {
    if (ssx::is_shutdown_exception(e)) {
        return {errc::shutting_down, "{}{}", prefix, e};
    }
    errc ec{};
    ss::sstring msg;
    try {
        std::rethrow_exception(e);
    } catch (lsm::abort_requested_exception& ex) {
        ec = errc::shutting_down;
        msg = fmt::format("{}{}", prefix, ex.what());
    } catch (lsm::corruption_exception& ex) {
        ec = errc::corruption;
        msg = fmt::format("{}{}", prefix, ex.what());
    } catch (std::exception& ex) {
        ec = errc::io_error;
        msg = fmt::format("{}{}", prefix, ex.what());
        vlog(cd_log.error, "Unexpected exception: {}", msg);
    } catch (...) {
        ec = errc::io_error;
        msg = fmt::format("{}{}", prefix, e);
        vlog(cd_log.error, "Unexpected exception_ptr: {}", msg);
    }
    return {ec, std::move(msg)};
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

ss::sstring state_reader::extent_key_range::to_string() {
    return fmt::format(
      "extent_key_range: [{}, {}], {} iter: {}",
      _base_key,
      _last_key,
      _direction == direction::forward ? "forward" : "backward",
      _iter.valid() ? _iter.key() : "{invalid iterator}");
}

ss::coroutine::experimental::generator<
  std::expected<state_reader::extent_row, state_reader::error>>
state_reader::extent_key_range::get_rows() {
    const bool forward = _direction == direction::forward;
    const auto& start_key = forward ? _base_key : _last_key;
    const auto& end_key = forward ? _last_key : _base_key;

    auto fut = co_await ss::coroutine::as_future(_iter.seek(start_key));
    if (fut.failed()) {
        auto ex = fut.get_exception();
        co_yield std::unexpected(to_error(ex));
        co_return;
    }
    if (!_iter.valid() || _iter.key() != start_key) {
        co_yield std::unexpected(
          error(errc::corruption, "Expected base key: {}", to_string()));
        co_return;
    }
    while (_iter.valid()) {
        std::exception_ptr ex;
        try {
            if (forward ? (_iter.key() > end_key) : (_iter.key() < end_key)) {
                co_yield std::unexpected(error(
                  errc::corruption,
                  "Unexpected key past last: {}",
                  to_string()));
                co_return;
            }
            auto val = serde::from_iobuf<extent_row_value>(_iter.value());
            co_yield extent_row{
              .key = ss::sstring(_iter.key()),
              .val = val,
            };
            if (_iter.key() == end_key) {
                co_return;
            }
            if (forward) {
                co_await _iter.next();
            } else {
                co_await _iter.prev();
            }
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            co_yield std::unexpected(to_error(
              ex, fmt::format("Exception iterating {}: ", to_string())));
            co_return;
        }
    }
}

ss::future<
  chunked_vector<std::expected<state_reader::extent_row, state_reader::error>>>
state_reader::extent_key_range::materialize_rows() {
    auto gen = get_rows();
    chunked_vector<std::expected<extent_row, error>> ret;
    while (auto res = co_await gen()) {
        if (!res.has_value()) {
            break;
        }
        ret.emplace_back(*res);
    }
    co_return ret;
}

namespace {

bool is_at_object(lsm::iterator& iter) {
    if (!iter.valid()) {
        return false;
    }
    auto key = object_row_key::decode(iter.key());
    return key.has_value();
}

} // namespace

ss::sstring state_reader::object_key_range::to_string() {
    return fmt::format(
      "object_key_range: inclusive_base_key: {}, iter: {}",
      _inclusive_base_key,
      _iter.valid() ? _iter.key() : "{invalid iterator}");
}

ss::coroutine::experimental::generator<
  std::expected<state_reader::object_row, state_reader::error>>
state_reader::object_key_range::get_rows() {
    auto fut = co_await ss::coroutine::as_future(
      _iter.seek(_inclusive_base_key));
    if (fut.failed()) {
        auto ex = fut.get_exception();
        co_yield std::unexpected(to_error(ex));
        co_return;
    }
    while (_iter.valid()) {
        std::exception_ptr ex;
        if (!is_at_object(_iter)) {
            co_return;
        }
        try {
            auto val = serde::from_iobuf<object_row_value>(_iter.value());
            co_yield object_row{
              .key = ss::sstring(_iter.key()),
              .val = val,
              .seqno = _iter.seqno(),
            };

            co_await _iter.next();
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            co_yield std::unexpected(to_error(
              ex, fmt::format("Exception iterating {}: ", to_string())));
            co_return;
        }
    }
}

ss::future<
  std::expected<std::optional<metadata_row_value>, state_reader::error>>
state_reader::get_metadata(const model::topic_id_partition& tidp) {
    return get_val<metadata_row_key, metadata_row_value>(tidp);
}

ss::future<std::expected<std::optional<compaction_state>, state_reader::error>>
state_reader::get_compaction_metadata(const model::topic_id_partition& tidp) {
    auto opt_val_res
      = co_await get_val<compaction_row_key, compaction_row_value>(tidp);
    if (!opt_val_res.has_value()) {
        co_return std::unexpected(std::move(opt_val_res.error()));
    }
    if (!opt_val_res.value().has_value()) {
        co_return std::nullopt;
    }
    co_return opt_val_res.value()->state;
}

ss::future<std::expected<std::optional<object_entry>, state_reader::error>>
state_reader::get_object(object_id oid) {
    auto opt_val_res = co_await get_val<object_row_key, object_row_value>(oid);
    if (!opt_val_res.has_value()) {
        co_return std::unexpected(std::move(opt_val_res.error()));
    }
    if (!opt_val_res.value().has_value()) {
        co_return std::nullopt;
    }
    co_return opt_val_res.value()->object;
}

ss::future<std::expected<std::optional<term_start>, state_reader::error>>
state_reader::get_max_term(const model::topic_id_partition& tidp) {
    iobuf val_buf;
    model::term_id term;
    ss::sstring base_key_str;
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
        base_key_str = ss::sstring(iter.key());
        auto key = term_row_key::decode(base_key_str);
        if (!key.has_value() || key->tidp != tidp) {
            co_return std::nullopt;
        }
        term = key->term;
        val_buf = iter.value();
    } catch (...) {
        co_return std::unexpected(to_error(std::current_exception()));
    }
    try {
        auto val = serde::from_iobuf<term_row_value>(std::move(val_buf));
        co_return term_start{
          .term_id = term, .start_offset = val.term_start_offset};
    } catch (...) {
        co_return std::unexpected(error(
          errc::corruption,
          "Failed to decode term row value for {} term {} key {}",
          tidp,
          term,
          base_key_str));
    }
}

ss::future<std::expected<std::optional<extent>, state_reader::error>>
state_reader::get_extent_ge(
  const model::topic_id_partition& tidp, kafka::offset o) {
    iobuf val_buf;
    kafka::offset base_offset;
    ss::sstring base_key_str;
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

        base_key_str = ss::sstring(iter.key());
        auto key = extent_row_key::decode(base_key_str);
        if (!key.has_value() || key->tidp != tidp) {
            co_return std::nullopt;
        }
        base_offset = key->base_offset;
        val_buf = iter.value();
    } catch (...) {
        co_return std::unexpected(to_error(std::current_exception()));
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
        co_return std::unexpected(error(
          errc::corruption,
          "Failed to decode extent row value for {} offset {} key {}",
          tidp,
          base_offset,
          base_key_str));
    }
}

ss::future<std::expected<
  std::optional<state_reader::extent_key_range>,
  state_reader::error>>
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
        co_return std::unexpected(to_error(std::current_exception()));
    }
    try {
        auto val = serde::from_iobuf<extent_row_value>(std::move(last_val_buf));
        if (val.last_offset != last) {
            co_return std::nullopt;
        }
    } catch (...) {
        co_return std::unexpected(error(
          errc::corruption,
          "Failed to decode extent row value for {} offset {} key {}",
          tidp,
          base,
          base_key));
    }
    // Position iterator back at base_key to return to the caller.
    try {
        co_await iter.seek(base_key);
        dassert(iter.valid(), "Iterator became invalid for key: {}", base_key);
    } catch (...) {
        co_return std::unexpected(to_error(std::current_exception()));
    }
    co_return extent_key_range(
      std::move(base_key),
      std::move(last_key),
      std::move(iter),
      direction::forward);
}

ss::future<std::expected<
  std::optional<std::pair<ss::sstring, ss::sstring>>,
  state_reader::error>>
state_reader::find_inclusive_extent_keys(
  const model::topic_id_partition& tidp,
  std::optional<kafka::offset> min_offset,
  std::optional<kafka::offset> max_offset) {
    ss::sstring base_key;
    ss::sstring last_key;
    try {
        auto iter = co_await snap_.create_iterator();
        if (min_offset.has_value()) {
            // Find the first extent where last_offset >= min_offset.
            // Seek to min_offset+1, then prev to find extent that may contain
            // min_offset.
            auto seek_key = extent_row_key::encode(
              tidp, kafka::next_offset(min_offset.value()));
            co_await iter.seek(seek_key);
            if (iter.valid()) {
                co_await iter.prev();
                if (!is_at_extent(iter, tidp)) {
                    // If there were no extents below the seek_key result, seek
                    // back to that key in case it's an extent -- if it is, its
                    // last_offset will be >= min_offset.
                    co_await iter.seek(seek_key);
                }
            } else {
                co_await iter.seek_to_last();
            }

            if (!is_at_extent(iter, tidp)) {
                co_return std::nullopt;
            }

            // Validate extent's last_offset >= min_offset.
            auto val = serde::from_iobuf<extent_row_value>(iter.value());
            if (val.last_offset < min_offset.value()) {
                co_await iter.next();
                if (!is_at_extent(iter, tidp)) {
                    co_return std::nullopt;
                }
            }
        } else {
            // No lower bound, start from first extent.
            co_await iter.seek(extent_row_key::encode(tidp, kafka::offset(0)));
            if (!is_at_extent(iter, tidp)) {
                co_return std::nullopt;
            }
        }
        base_key = ss::sstring(iter.key());

        if (max_offset.has_value()) {
            // Find the last extent whose base_offset <= max_offset.
            // Seek to max_offset+1, then prev to find extent with base <= max.
            auto seek_key = extent_row_key::encode(
              tidp, kafka::next_offset(max_offset.value()));
            co_await iter.seek(seek_key);
            if (iter.valid()) {
                co_await iter.prev();
            } else {
                co_await iter.seek_to_last();
            }

            if (!is_at_extent(iter, tidp)) {
                co_return std::nullopt;
            }
        } else {
            // No upper bound, end at last extent in this partition. Seek to
            // next partition, then prev.
            co_await iter.seek(
              extent_row_key::encode(next_partition(tidp), kafka::offset(0)));
            if (iter.valid()) {
                co_await iter.prev();
            } else {
                co_await iter.seek_to_last();
            }
            if (!is_at_extent(iter, tidp)) {
                co_return std::nullopt;
            }
        }
        last_key = ss::sstring(iter.key());

        // Sanity check: base_key <= last_key.
        if (base_key > last_key) {
            co_return std::nullopt;
        }
    } catch (...) {
        co_return std::unexpected(to_error(std::current_exception()));
    }

    co_return std::make_optional(
      std::make_pair(std::move(base_key), std::move(last_key)));
}

ss::future<std::expected<
  std::optional<state_reader::extent_key_range>,
  state_reader::error>>
state_reader::get_inclusive_extents(
  const model::topic_id_partition& tidp,
  std::optional<kafka::offset> min_offset,
  std::optional<kafka::offset> max_offset) {
    auto keys_res = co_await find_inclusive_extent_keys(
      tidp, min_offset, max_offset);
    if (!keys_res.has_value()) {
        co_return std::unexpected(std::move(keys_res.error()));
    }
    if (!keys_res.value().has_value()) {
        co_return std::nullopt;
    }

    auto iter = co_await snap_.create_iterator();
    co_return extent_key_range(
      std::move(keys_res.value()->first),
      std::move(keys_res.value()->second),
      std::move(iter),
      direction::forward);
}

ss::future<std::expected<
  std::optional<state_reader::extent_key_range>,
  state_reader::error>>
state_reader::get_inclusive_extents_backward(
  const model::topic_id_partition& tidp,
  std::optional<kafka::offset> min_offset,
  std::optional<kafka::offset> max_offset) {
    auto keys_res = co_await find_inclusive_extent_keys(
      tidp, min_offset, max_offset);
    if (!keys_res.has_value()) {
        co_return std::unexpected(std::move(keys_res.error()));
    }
    if (!keys_res.value().has_value()) {
        co_return std::nullopt;
    }

    auto iter = co_await snap_.create_iterator();
    co_return extent_key_range(
      std::move(keys_res.value()->first),
      std::move(keys_res.value()->second),
      std::move(iter),
      direction::backward);
}

namespace {

bool is_at_term(
  lsm::iterator& iter,
  const model::topic_id_partition& tidp,
  term_row_key* out = nullptr) {
    if (!iter.valid()) {
        return false;
    }
    auto key = term_row_key::decode(iter.key());
    if (!key.has_value() || key->tidp != tidp) {
        return false;
    }
    if (out) {
        *out = key.value();
    }
    return true;
}

} // namespace

ss::future<std::expected<std::optional<term_start>, state_reader::error>>
state_reader::get_term_le(
  const model::topic_id_partition& tidp, kafka::offset offset) {
    try {
        auto iter = co_await snap_.create_iterator();

        // Seek to the first term of the next partition and iterate backwards.
        co_await iter.seek(
          term_row_key::encode(next_partition(tidp), model::term_id(0)));
        if (!iter.valid()) {
            co_await iter.seek_to_last();
        } else {
            co_await iter.prev();
        }

        // Iterate backwards through terms, find first with start_offset <=
        // offset.
        while (is_at_term(iter, tidp)) {
            auto val = serde::from_iobuf<term_row_value>(iter.value());
            if (val.term_start_offset <= offset) {
                auto key = term_row_key::decode(iter.key());
                co_return term_start{
                  .term_id = key->term,
                  .start_offset = val.term_start_offset,
                };
            }
            co_await iter.prev();
        }

        // No term found with start_offset <= offset.
        co_return std::nullopt;
    } catch (...) {
        co_return std::unexpected(to_error(std::current_exception()));
    }
}

ss::future<std::expected<std::optional<kafka::offset>, state_reader::error>>
state_reader::get_term_end(
  const model::topic_id_partition& tidp, model::term_id term) {
    try {
        auto iter = co_await snap_.create_iterator();

        // Seek to the term key.
        co_await iter.seek(term_row_key::encode(tidp, term));
        if (!is_at_term(iter, tidp)) {
            // All terms are below the requested term.
            co_return std::nullopt;
        }

        auto key = term_row_key::decode(iter.key());
        if (key->term > term) {
            // Found a higher term; return its start offset as the exclusive
            // end of the requested term.
            auto val = serde::from_iobuf<term_row_value>(iter.value());
            co_return val.term_start_offset;
        }

        // Otherwise, we found the exact term. Look for the next term so we can
        // use it to get this term's end.
        co_await iter.next();
        if (is_at_term(iter, tidp)) {
            auto val = serde::from_iobuf<term_row_value>(iter.value());
            co_return val.term_start_offset;
        }

        // If there was no term above the requested term, return the
        // partition's next offset.
        auto metadata_res
          = co_await get_val<metadata_row_key, metadata_row_value>(tidp);
        if (!metadata_res.has_value()) {
            co_return std::unexpected(std::move(metadata_res.error()));
        }
        if (!metadata_res.value().has_value()) {
            co_return std::unexpected(error(
              errc::corruption, "Missing partition metadata for {}", tidp));
        }
        co_return metadata_res.value()->next_offset;
    } catch (...) {
        co_return std::unexpected(to_error(std::current_exception()));
    }
}

template<typename KeyT, typename ValT, typename... KeyEncodeArgs>
ss::future<std::expected<std::optional<ValT>, state_reader::error>>
state_reader::get_val(KeyEncodeArgs... args) {
    auto key_str = KeyT::encode(args...);
    auto fut = co_await ss::coroutine::as_future(snap_.get(key_str));
    if (fut.failed()) {
        co_return std::unexpected(to_error(fut.get_exception()));
    }
    auto opt_buf = fut.get();
    if (!opt_buf.has_value()) {
        co_return std::nullopt;
    }
    try {
        auto val = serde::from_iobuf<ValT>(std::move(*opt_buf));
        co_return val;
    } catch (...) {
        co_return std::unexpected(
          error(errc::corruption, "Failed to decode value at key {}", key_str));
    }
}

ss::future<std::expected<chunked_vector<ss::sstring>, state_reader::error>>
state_reader::get_term_keys(
  const model::topic_id_partition& tidp,
  std::optional<kafka::offset> upper_bound) {
    chunked_vector<ss::sstring> keys;
    try {
        auto iter = co_await snap_.create_iterator();

        // Seek to the first term row for this partition.
        co_await iter.seek(term_row_key::encode(tidp, model::term_id(0)));

        // Iterate through term rows, collecting keys.
        while (is_at_term(iter, tidp)) {
            if (upper_bound.has_value()) {
                auto val = serde::from_iobuf<term_row_value>(iter.value());
                if (val.term_start_offset > *upper_bound) {
                    // Stop once we find a term with start_offset > upper_bound.
                    break;
                }
            }
            keys.push_back(ss::sstring(iter.key()));
            co_await iter.next();
        }
    } catch (...) {
        co_return std::unexpected(to_error(std::current_exception()));
    }
    co_return keys;
}

ss::future<std::expected<chunked_vector<term_start>, state_reader::error>>
state_reader::get_all_terms(const model::topic_id_partition& tidp) {
    chunked_vector<term_start> terms;
    try {
        auto iter = co_await snap_.create_iterator();
        co_await iter.seek(term_row_key::encode(tidp, model::term_id(0)));

        term_row_key decoded;
        while (is_at_term(iter, tidp, &decoded)) {
            auto val = serde::from_iobuf<term_row_value>(iter.value());
            terms.push_back(
              term_start{
                .term_id = decoded.term,
                .start_offset = val.term_start_offset,
              });
            co_await iter.next();
        }
    } catch (...) {
        co_return std::unexpected(to_error(std::current_exception()));
    }
    co_return terms;
}

namespace {

bool is_at_metadata(
  lsm::iterator& iter,
  const model::topic_id& tid,
  metadata_row_key* out = nullptr) {
    if (!iter.valid()) {
        return false;
    }
    auto key = metadata_row_key::decode(iter.key());
    if (!key.has_value() || key->tidp.topic_id != tid) {
        return false;
    }
    if (out) {
        *out = key.value();
    }
    return true;
}

} // namespace

ss::future<
  std::expected<chunked_vector<model::partition_id>, state_reader::error>>
state_reader::get_partitions_for_topic(const model::topic_id& tid) {
    chunked_vector<model::partition_id> partitions;
    try {
        auto iter = co_await snap_.create_iterator();

        // Seek to the first metadata row for this topic.
        auto tidp = model::topic_id_partition(tid, model::partition_id(0));
        co_await iter.seek(metadata_row_key::encode(tidp));

        // Iterate through all metadata rows for this topic.
        while (is_at_metadata(iter, tid)) {
            auto key = metadata_row_key::decode(iter.key());
            partitions.push_back(key->tidp.partition);
            co_await iter.next();
        }
    } catch (...) {
        co_return std::unexpected(to_error(std::current_exception()));
    }
    co_return partitions;
}

std::expected<state_reader::object_key_range, state_reader::error>
make_object_range(lsm::iterator iter, std::optional<object_id> start_oid) {
    auto base_key = start_oid ? object_row_key::encode(*start_oid)
                              : object_row_key::encode(object_id(uuid_t{}));
    return state_reader::object_key_range(std::move(base_key), std::move(iter));
}

ss::future<std::expected<state_reader::object_key_range, state_reader::error>>
state_reader::get_object_range(std::optional<object_id> start_oid) {
    auto base_key = start_oid ? object_row_key::encode(*start_oid)
                              : object_row_key::encode(object_id(uuid_t{}));
    try {
        auto iter = co_await snap_.create_iterator();
        co_return object_key_range(std::move(base_key), std::move(iter));
    } catch (...) {
        co_return std::unexpected(to_error(std::current_exception()));
    }
}

} // namespace cloud_topics::l1
