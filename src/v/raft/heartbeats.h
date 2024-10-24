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

#pragma once

#include "bytes/iobuf.h"
#include "model/metadata.h"
#include "raft/fundamental.h"
#include "serde/async.h"
#include "serde/envelope.h"
#include "serde/rw/enum.h"
#include "serde/rw/rw.h"
#include "utils/delta_for.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/coroutine.hh>

#include <cstddef>
#include <cstdint>

namespace raft {

template<typename ValueT, typename DeltaT, typename FieldT = uint64_t>
class deltafor_column {
private:
    using field_t = FieldT;
    using encoder_t = deltafor_encoder<field_t, DeltaT, true>;
    using decoder_t = deltafor_decoder<field_t, DeltaT>;
    using row_t = encoder_t::row_t;

    static constexpr auto value_sz = sizeof(ValueT);
    static constexpr auto value_bits = sizeof(ValueT) * 8;
    static constexpr auto row_sz = 16;
    static constexpr auto values_per_field = sizeof(field_t) / value_sz;
    static constexpr auto values_per_row = row_sz * values_per_field;
    static constexpr field_t mask = std::numeric_limits<ValueT>::max();

public:
    struct cursor {
        explicit cursor(const deltafor_column<ValueT, DeltaT, FieldT>& column)
          : _column(column)
          , _decoder(
              column._data.get_initial_value(),
              column._data.get_row_count(),
              column._data.share(),
              DeltaT{}) {
            if (_column.size() < _column.values_per_row) {
                _buffer = _column._remainder_buffer;
                _in_reminder = true;
            } else {
                _decoder.read(_buffer);
            }
        }

        bool advance() {
            const auto idx = _current % _column.values_per_row;
            const auto shift = (_current % _column.values_per_field)
                               * value_bits;

            if (
              (_current + 1) / _column.values_per_row
              < _column._data.get_row_count()) {
                if (
                  idx == (_column.values_per_row - 1)
                  && shift
                       == (_column.values_per_field - 1) * _column.value_bits) {
                    _buffer = {};
                    _decoder.read(_buffer);
                }
            } else if (!_in_reminder) {
                _buffer = _column._remainder_buffer;
                _in_reminder = true;
            }

            if (++_current >= _column.size()) {
                return false;
            }
            return true;
        }

        ValueT operator()() const {
            if (_current >= _column.size()) {
                throw std::out_of_range("");
            }
            const auto idx = (_current % _column.values_per_row)
                             / _column.values_per_field;
            if constexpr (values_per_field == 1) {
                return _buffer[idx];
            }

            const auto shift = (_current % _column.values_per_field)
                               * value_bits;
            return static_cast<ValueT>(_buffer[idx] >> shift) & mask;
        }

        size_t size() const { return _column.size(); }

    private:
        const deltafor_column<ValueT, DeltaT, FieldT>& _column;
        decoder_t _decoder;
        decoder_t::row_t _buffer{};

        size_t _current{0};
        bool _in_reminder{false};
    };

    void add(ValueT value) {
        const uint8_t shift = (_cnt % values_per_field) * value_bits;
        const auto idx = (_cnt++ % values_per_row) / values_per_field;

        if constexpr (values_per_field == 1) {
            _remainder_buffer[idx] = value;
        } else {
            _remainder_buffer[idx] |= (static_cast<field_t>(value) << shift);
        }

        if (
          idx == (row_sz - 1) && shift == (values_per_field - 1) * value_bits) {
            _data.add(_remainder_buffer);
            _remainder_buffer = {};
        }
    }

    cursor make_cursor() const { return cursor(*this); }

    size_t size() const { return _cnt; }

    deltafor_column<ValueT, DeltaT, FieldT> copy() const {
        deltafor_column<ValueT, DeltaT, FieldT> ret;
        ret._data = encoder_t(
          _data.get_initial_value(),
          _data.get_row_count(),
          _data.get_last_value(),
          _data.copy());
        ret._cnt = _cnt;
        ret._remainder_buffer = _remainder_buffer;
        return ret;
    }

    friend bool operator==(
      const deltafor_column<ValueT, DeltaT, FieldT>& lhs,
      const deltafor_column<ValueT, DeltaT, FieldT>& rhs) {
        return lhs._data == rhs._data && lhs._cnt == rhs._cnt
               && lhs._remainder_buffer == rhs._remainder_buffer;
    }

    friend inline void read_nested(
      iobuf_parser& in,
      deltafor_column<ValueT, DeltaT, FieldT>& c,
      const size_t bytes_left_limit) {
        using serde::read_async_nested;
        using serde::read_nested;
        c._remainder_buffer = read_nested<row_t>(in, bytes_left_limit);
        auto initial = read_nested<field_t>(in, bytes_left_limit);
        auto cnt = read_nested<uint32_t>(in, bytes_left_limit);
        auto last = read_nested<field_t>(in, bytes_left_limit);
        auto buffer = read_nested<iobuf>(in, bytes_left_limit);
        // copy buffer content not share the buffer cross shards
        c._data = encoder_t(initial, cnt, last, buffer.copy());
        c._cnt = read_nested<size_t>(in, bytes_left_limit);
    }

    friend inline void
    write(iobuf& out, deltafor_column<ValueT, DeltaT, FieldT> state) {
        using serde::write;
        write(out, state._remainder_buffer);
        write(out, state._data.get_initial_value());
        write(out, state._data.get_row_count());
        write(out, state._data.get_last_value());
        // copy buffer content not share the buffer cross shards
        write(out, state._data.copy());
        write(out, state._cnt);
    }

private:
    row_t _remainder_buffer{};
    encoder_t _data;
    size_t _cnt{0};
};

template<typename Func, typename... C>
void for_each_column(Func f, const C&... values) {
    auto cursors = std::make_tuple(values.make_cursor()...);

    for (size_t i = 0; i < std::get<0>(cursors).size(); ++i) {
        auto values = std::apply(
          [](auto&... c) { return std::make_tuple(c()...); }, cursors);
        std::apply(f, values);
        std::apply(
          [](auto&... c) { return std::make_tuple(c.advance()...); }, cursors);
    }
}

struct heartbeat_request_data
  : serde::envelope<
      heartbeat_request_data,
      serde::version<0>,
      serde::compat_version<0>> {
    model::revision_id source_revision;
    model::revision_id target_revision;
    model::offset commit_index;
    model::term_id term;
    model::offset prev_log_index;
    model::term_id prev_log_term;
    model::offset last_visible_index;

    auto serde_fields() {
        return std::tie(
          source_revision,
          target_revision,
          commit_index,
          term,
          prev_log_index,
          prev_log_term,
          last_visible_index);
    }

    friend bool
    operator==(const heartbeat_request_data&, const heartbeat_request_data&)
      = default;
};

struct heartbeat_reply_data
  : serde::envelope<
      heartbeat_reply_data,
      serde::version<0>,
      serde::compat_version<0>> {
    model::revision_id source_revision;
    model::revision_id target_revision;
    model::term_id term;

    model::offset last_flushed_log_index;
    model::offset last_dirty_log_index;
    model::offset last_term_base_offset;

    bool may_recover = false;

    auto serde_fields() {
        return std::tie(
          source_revision,
          target_revision,
          term,
          last_flushed_log_index,
          last_dirty_log_index,
          last_term_base_offset,
          may_recover);
    }

    friend bool
    operator==(const heartbeat_reply_data&, const heartbeat_reply_data&)
      = default;
};

struct group_heartbeat {
    raft::group_id group;
    std::optional<heartbeat_request_data> data;

    friend bool operator==(const group_heartbeat&, const group_heartbeat&)
      = default;
};

struct full_heartbeat_reply {
    raft::group_id group;
    reply_result result = reply_result::failure;
    heartbeat_reply_data data;

    friend inline void read_nested(
      iobuf_parser& in,
      full_heartbeat_reply& req,
      const size_t bytes_left_limit) {
        using serde::read_nested;
        read_nested(in, req.group, bytes_left_limit);
        read_nested(in, req.result, bytes_left_limit);
        read_nested(in, req.data, bytes_left_limit);
    }

    friend inline void write(iobuf& out, full_heartbeat_reply req) {
        using serde::write;
        write(out, req.group);
        write(out, req.result);
        write(out, req.data);
    }

    friend bool
    operator==(const full_heartbeat_reply&, const full_heartbeat_reply&)
      = default;
};

struct full_heartbeat {
    raft::group_id group;
    heartbeat_request_data data;

    friend bool operator==(const full_heartbeat&, const full_heartbeat&)
      = default;

    friend inline void read_nested(
      iobuf_parser& in, full_heartbeat& fh, const size_t bytes_left_limit) {
        using serde::read_nested;
        read_nested(in, fh.group, bytes_left_limit);
        read_nested(in, fh.data, bytes_left_limit);
    }
    friend inline void write(iobuf& out, full_heartbeat fh) {
        using serde::write;
        write(out, fh.group);
        write(out, fh.data);
    }
};

class heartbeat_request_v2
  : public serde::envelope<
      heartbeat_request_v2,
      serde::version<0>,
      serde::compat_version<0>> {
public:
    using rpc_adl_exempt = std::true_type;

    using lw_column_t
      = deltafor_column<int64_t, ::details::delta_delta<int64_t>, int64_t>;

    using full_heartbeats_t = ss::chunked_fifo<full_heartbeat>;

    heartbeat_request_v2() noexcept = default;

    heartbeat_request_v2(model::node_id source_node, model::node_id target_node)
      : _source_node(source_node)
      , _target_node(target_node) {}

    void add(const group_heartbeat&);

    friend bool operator==(
      const heartbeat_request_v2& lhs, const heartbeat_request_v2& rhs) {
        return lhs._source_node == rhs._source_node
               && lhs._target_node == rhs._target_node
               && lhs._lw_cnt == rhs._lw_cnt
               && lhs._lw_heartbeats == rhs._lw_heartbeats
               && lhs._full_heartbeats.size() == rhs._full_heartbeats.size()
               && std::equal(
                 lhs._full_heartbeats.begin(),
                 lhs._full_heartbeats.end(),
                 rhs._full_heartbeats.begin());
    };

    ss::future<> serde_async_write(iobuf& out);
    ss::future<> serde_async_read(iobuf_parser&, const serde::header);

    heartbeat_request_v2 copy() const;

    const full_heartbeats_t& full_heartbeats() const {
        return _full_heartbeats;
    }

    template<typename Func>
    void for_each_lw_heartbeat(Func&& f) const {
        for_each_column(
          [f = std::forward<Func>(f)](int64_t g) mutable {
              f(raft::group_id(g));
          },
          _lw_heartbeats);
    }

    model::node_id source() const { return _source_node; }
    model::node_id target() const { return _target_node; }
    uint64_t lw_heartbeats_count() const { return _lw_cnt; }

private:
    friend std::ostream&
    operator<<(std::ostream& o, const heartbeat_request_v2& r);

    uint64_t _lw_cnt{0};
    model::node_id _source_node;
    model::node_id _target_node;
    /**
     * Heartbeat requests stores lightweight heartbeats (only group_id) in a
     * deltafor encoded form. The lw_heartbeat column is delta_delta encoded as
     * we leverage the fact that consensus instances are sorted by group_id in
     * heartbeat manager. This way the group ids added to heartbeat request are
     * monotonically increasing.
     *
     * Since during the active phase most of the state between raft groups is
     * exchanged in append_entries request containing data we encoded
     * full_heartbeats as a plain chunked_fifo
     */
    lw_column_t _lw_heartbeats;
    full_heartbeats_t _full_heartbeats;
};

class heartbeat_reply_v2
  : public serde::envelope<
      heartbeat_reply_v2,
      serde::version<0>,
      serde::compat_version<0>> {
public:
    using rpc_adl_exempt = std::true_type;

    using lw_column_t = deltafor_column<int64_t, ::details::delta_xor, int64_t>;
    using result_column_t = deltafor_column<int8_t, ::details::delta_xor>;

    heartbeat_reply_v2() noexcept = default;

    heartbeat_reply_v2(model::node_id source_node, model::node_id target_node)
      : _source_node(source_node)
      , _target_node(target_node) {}

    model::node_id source() const { return _source_node; };
    model::node_id target() const { return _target_node; };

    const ss::chunked_fifo<full_heartbeat_reply>& full_replies() const {
        return _full_replies;
    };

    friend std::ostream&
    operator<<(std::ostream& o, const heartbeat_reply_v2& r);

    friend bool
    operator==(const heartbeat_reply_v2& lhs, const heartbeat_reply_v2& rhs) {
        return lhs._source_node == rhs._source_node
               && lhs._target_node == rhs._target_node
               && lhs._lw_replies == rhs._lw_replies
               && lhs._results == rhs._results
               && lhs._full_replies.size() == rhs._full_replies.size()
               && std::equal(
                 lhs._full_replies.begin(),
                 lhs._full_replies.end(),
                 rhs._full_replies.begin());
    };

    ss::future<> serde_async_write(iobuf& out);
    ss::future<> serde_async_read(iobuf_parser&, const serde::header);

    heartbeat_reply_v2 copy() const;

    void add(group_id, reply_result);
    void add(group_id, reply_result, const heartbeat_reply_data&);

    template<typename Func>
    void for_each_lw_reply(Func&& f) const {
        for_each_column(
          [f = std::forward<Func>(f)](int64_t group, uint8_t result) {
              return f(raft::group_id(group), reply_result(result));
          },
          _lw_replies,
          _results);
    }

private:
    model::node_id _source_node;
    model::node_id _target_node;
    /**
     * Lightweight heartbeat reply is a tuple containing group_id and
     * reply_result. The tuples are encoded in a columnar format where group_id
     * and results are stored in a separate column.
     * Here both the results and replies are encoded using deltafor with XOR
     * based delta encoding since there are no ordering guarantees on values
     * stored in replies (heartbeats are processed in per shard chunks).
     *
     * The same way as in the request, full replies are encoded as a plain
     * chunked fifo.
     */
    lw_column_t _lw_replies;
    result_column_t _results;

    ss::chunked_fifo<full_heartbeat_reply> _full_replies;
};
} // namespace raft
