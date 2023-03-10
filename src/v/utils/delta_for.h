/*
 * Copyright 2022 Redpanda Data, Inc.
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
#include "bytes/iobuf_parser.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/util/log.hh>

#include <variant>

namespace details {
static constexpr uint32_t FOR_buffer_depth = 16;

/*
 * The delta encoder for deltafor_encoder and deltafor_decoder.
 *
 * The encoder can work with any integer sequence with 64-bit values.
 * It uses bitwise XOR operation to compute delta values.
 */
struct delta_xor {
    template<class value_t, size_t row_width>
    constexpr uint8_t encode(
      value_t last,
      const std::array<value_t, row_width>& row,
      std::array<value_t, row_width>& buf) const {
        auto p = last;
        uint64_t agg = 0;
        for (uint32_t i = 0; i < row_width; ++i) {
            buf[i] = row[i] ^ p;
            agg |= buf[i];
            p = row[i];
        }
        uint8_t nbits = std::bit_width(agg);
        return nbits;
    }

    template<class value_t, size_t row_width>
    constexpr value_t
    decode(value_t initial, std::array<value_t, row_width>& row) const {
        auto p = initial;
        for (unsigned i = 0; i < row_width; i++) {
            row[i] = row[i] ^ p;
            p = row[i];
        }
        return p;
    }

    bool operator==(delta_xor const&) const = default;
};

/*
 * The delta encoder for deltafor_encoder and deltafor_decoder.
 *
 * The encoder can work only with non-decreasing integer sequences
 * with 64-bit values. It uses delta-delta algorithm to compute delta values.
 * The alg. computes delta values by subtacting consequtive values from
 * each other and then it subtracts the pre-defined step value out of every
 * delta. The step value is a minimal possible delta between two consequitive
 * elements.
 */
template<class ValueT>
struct delta_delta {
    explicit constexpr delta_delta(ValueT step = {0})
      : _step_size(step) {}

    template<class value_t, size_t row_width>
    constexpr uint8_t encode(
      value_t last,
      const std::array<value_t, row_width>& row,
      std::array<value_t, row_width>& buf) const {
        auto p = last;
        uint64_t agg = 0;
        for (uint32_t i = 0; i < row_width; ++i) {
            vassert(
              row[i] >= p,
              "Value {} can't be larger than the previous one {}",
              row[i],
              p);
            auto delta = row[i] - p;
            vassert(
              delta >= _step_size,
              "Delta {} can't be larger than step size {}",
              delta,
              _step_size);
            buf[i] = (row[i] - p) - _step_size;
            agg |= buf[i];
            p = row[i];
        }
        uint8_t nbits = std::bit_width(agg);
        return nbits;
    }

    template<class value_t, size_t row_width>
    constexpr value_t
    decode(value_t initial, std::array<value_t, row_width>& row) const {
        auto p = initial;
        for (unsigned i = 0; i < row_width; i++) {
            row[i] = row[i] + p + _step_size;
            p = row[i];
        }
        return p;
    }

    ValueT _step_size;

    bool operator==(delta_delta const&) const = default;
};

} // namespace details

/// Position in the delta_for encoded data stream
template<class T>
struct deltafor_stream_pos_t
  : public serde::envelope<
      deltafor_stream_pos_t<T>,
      serde::version<0>,
      serde::compat_version<0>> {
    /// Initial value for the next row
    T initial;
    /// Offset of the next row
    uint32_t offset;
    /// Number of rows before the next row
    uint32_t num_rows;

    auto serde_fields() { return std::tie(initial, offset, num_rows); }
};

/** \brief Delta-FOR encoder
 *
 * The algorithm uses differential encoding followed by the
 * frame of reference (FoR) encoding step. The differential step
 * is parametrized (two options are available: XOR and DeltaDelta).
 *
 * The encoder works with 16-element rows. This is needed to
 * enable easy loop unrolling (e.g. 4-bit values can be packed into
 * single uint64_t variable, 3-bit values can be packed into uint32_t
 * + uint16_t, etc). It also simplifies the loop unrolling for the
 * compiler since the loops that process the single row can be
 * easily unrolled or vectorized since the number of iterations is
 * known at compile time. Most bit-packing required by FoR can be
 * done as a series of simple operations (shifts/loads/stores) in a
 * loop that doesn't require any branching.
 *
 * Also, because bit-packing routines work with rows of 16 elements
 * they always start and stop on a byte boundary. This simplifies the
 * code quite a lot.
 *
 * The FoR encoding memory layout is not conventional. It's not
 * placing values one after another. It uses the following schema
 * instead. Consider the following size classes: 64, 32, 16, and
 * 8-bits. Each 64-bit value can be represented as a series of those + some
 * remainder. For instance, 18-bit value can be represented as 16-bit
 * value + 2-bit remainder, 47-bit value can be represented as 32-bit
 * value + 8-bit value + 7-bit remainder, etc.
 *
 * The bit-packing algorithm works in the following way. First, it
 * calculates minimal number of bits that can be used to store any
 * value in a row. Then the number of bits is factored into one or
 * several size classes + remainder. After that the algorithm writes
 * the number of bits that corresponds to the largest size class, then
 * the next one, etc. After that it writes the remainder of every element.
 * This means that the data from all 16 values is interleaving. The number
 * of bits used to represent the row is stored using 8 bits.
 *
 * Example: let's say that we have a row that requires us to use 59
 * bits per element. Bit-packing will go like this:
 * - write first 32-bits from every value in a row (64 bytes total);
 * - write next 16-bits from every value in a row (32 bytes total);
 * - write next 8-bits from every value in a row (16 bytes total);
 * - write the remaining 3-bits from every value (6 bytes total);
 * The result is represented using 118 bytes.
 *
 * One advantage of this approach is that it requires less code to
 * implement. It needs bit-packing functions that can pack 1-7 bits
 * and also 8, 16, 32, and 64 bits. All possible bit-packing arrangements
 * from 0 to 64 bits can be produced using this functions.
 * The alternative to this is to implement 63 bit-packing functions that
 * can pack all possible values. The approach used here requires only
 * 7 custom bit-packing functions + 4 bit-packing functions for different
 * size classes (8, 16, 32, 64) which is much easier to impelment and
 * test.
 *
 * It's also beneficial for further compression using general purpose
 * compression algorithms (e.g. zstd or lz4). If the values have some
 * common substructure (e.g. the lowest bits are zeroed) this common bits
 * will be stored together and the compression alg. could take advantage
 * of that.
 *
 * The compressed data is stored internally using an iobuf. This iobuf
 * can be copied and pushed to the decoder to decompress the values.
 *
 * The 'TVal' type parameter is a type of the encoded values.
 * The 'DeltaStep' type parameter is a type of the delta encoding
 * step (two implementations are provided, delta XOR and delta-delta).
 */
template<
  class TVal,
  class DeltaStep = details::delta_xor,
  bool use_nttp_deltastep = false,
  DeltaStep delta_alg = DeltaStep{}>
class deltafor_encoder
  : public serde::envelope<
      deltafor_encoder<TVal, DeltaStep, use_nttp_deltastep, delta_alg>,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr uint32_t row_width = details::FOR_buffer_depth;

public:
    explicit deltafor_encoder(
      TVal initial_value, DeltaStep delta = {}) requires(!use_nttp_deltastep)
      : _initial(initial_value)
      , _last(initial_value)
      , _cnt{0}
      , _delta(delta) {}

    constexpr explicit deltafor_encoder(TVal initial_value) requires
      use_nttp_deltastep
      : _initial(initial_value)
      , _last(initial_value)
      , _cnt{0}
      , _delta(delta_alg) {}

    constexpr explicit deltafor_encoder() = default;

    deltafor_encoder(
      TVal initial_value,
      uint32_t cnt,
      TVal last_value,
      iobuf data,
      DeltaStep delta = {}) requires(!use_nttp_deltastep)
      : _initial(initial_value)
      , _last(last_value)
      , _data(std::move(data))
      , _cnt(cnt)
      , _delta(delta) {}

    deltafor_encoder(
      TVal initial_value,
      uint32_t cnt,
      TVal last_value,
      iobuf data) requires use_nttp_deltastep
      : _initial(initial_value)
      , _last(last_value)
      , _data(std::move(data))
      , _cnt(cnt)
      , _delta(delta_alg) {}

    // This c-tor creates shallow copy of the encoder.
    //
    // The underlying iobuf is shared which makes the operation
    // relatively lightweiht. The signature is different from
    // copy c-tor on purpose. The 'other' object is modified
    // and not just copied. If the c-tor throws the 'other' is
    // not affected.
    explicit deltafor_encoder(deltafor_encoder* other)
      : _initial(other->_initial)
      , _last(other->_last)
      , _data(other->_data.share(0, other->_data.size_bytes()))
      , _cnt(other->_cnt)
      , _delta(other->_delta) {}

    using row_t = std::array<TVal, row_width>;

    /// Encode single row
    void add(const row_t& row) {
        row_t buf;
        uint8_t nbits = _delta.encode(_last, row, buf);
        _last = row.back();
        _data.append(&nbits, 1);
        pack(buf, nbits);
        _cnt++;
    }

    /// Return ppsition inside the stream
    deltafor_stream_pos_t<TVal> get_position() const {
        return {
          .initial = _last,
          .offset = uint32_t(_data.size_bytes()),
          .num_rows = _cnt,
        };
    }

    // State of the transaction
    // The state can be used to append multiple rows
    // and then commit or rollback.
    struct tx_state {
        using self_t
          = deltafor_encoder<TVal, DeltaStep, use_nttp_deltastep, delta_alg>;
        self_t uncommitted;

        void add(const row_t& row) { uncommitted.add(row); }
    };

    // Create tx-state object and start transaction
    //
    // Only one transaction at a time is supported but this
    // is not enforced. Abandoning tx_state object is ok (this
    // is equivalent for aborting the transaction).
    tx_state tx_start() { return tx_state{deltafor_encoder{this}}; }

    // Commit changes done to tx_state.
    // This operation does not throw.
    void tx_commit(tx_state tx) noexcept {
        _last = tx.uncommitted._last;
        _data = std::move(tx.uncommitted._data);
        _cnt = tx.uncommitted._cnt;
        _delta = tx.uncommitted._delta;
    }

    /// Copy the underlying iobuf
    iobuf copy() const { return _data.copy(); }

    /// Share the underlying iobuf
    iobuf share() const { return _data.share(0, _data.size_bytes()); }

    /// Return number of rows stored in the underlying iobuf instance
    uint32_t get_row_count() const noexcept { return _cnt; }

    /// Get initial value used to create the encoder
    TVal get_initial_value() const noexcept { return _initial; }

    /// Get last value used to create the encoder
    TVal get_last_value() const noexcept { return _last; }

    size_t mem_use() const { return _data.size_bytes(); }

    auto serde_fields() {
        if constexpr (use_nttp_deltastep) {
            return std::tie(_initial, _last, _data, _cnt);
        } else {
            return std::tie(_initial, _last, _data, _cnt, _delta);
        }
    }

private:
    template<typename T>
    void _pack_as(const row_t& input) {
        static_assert(
          std::is_unsigned<T>::value,
          "Only unsigned integer can be used as a type parameter");
        for (unsigned i = 0; i < row_width; i++) {
            T bits = static_cast<T>(input[i]);
            uint8_t buf[sizeof(bits)];
            std::memcpy(&buf, &bits, sizeof(bits));
            _data.append(buf, sizeof(bits));
        }
    }

    template<typename T>
    void _shift_as(row_t& input) {
        for (uint32_t i = 0; i < row_width; i++) {
            input[i] >>= 8 * sizeof(T);
        }
    }

    template<typename T>
    void _pack_shift(row_t& input) {
        _pack_as<T>(input);
        _shift_as<T>(input);
    }

    template<typename... Ts>
    void _pack_shift_all(row_t& input) {
        (_pack_shift<Ts>(input), ...);
    }

    void _pack1(const row_t& input) {
        uint16_t bits = 0;
        for (uint32_t i = 0; i < row_width; i++) {
            bits |= static_cast<uint16_t>((input[i] & 1) << i);
        }
        _data.append(reinterpret_cast<uint8_t*>(&bits), sizeof(bits));
    }

    void _pack2(const row_t& input) {
        uint32_t bits = 0;
        for (uint32_t i = 0; i < row_width; i++) {
            bits |= static_cast<uint32_t>((input[i] & 3) << (2 * i));
        }
        _data.append(reinterpret_cast<uint8_t*>(&bits), sizeof(bits));
    }

    void _pack3(const row_t& input) {
        uint32_t bits0 = 0;
        uint16_t bits1 = 0;
        bits0 |= static_cast<uint32_t>((input[0] & 7));
        bits0 |= static_cast<uint32_t>((input[1] & 7) << 3);
        bits0 |= static_cast<uint32_t>((input[2] & 7) << 6);
        bits0 |= static_cast<uint32_t>((input[3] & 7) << 9);
        bits0 |= static_cast<uint32_t>((input[4] & 7) << 12);
        bits0 |= static_cast<uint32_t>((input[5] & 7) << 15);
        bits0 |= static_cast<uint32_t>((input[6] & 7) << 18);
        bits0 |= static_cast<uint32_t>((input[7] & 7) << 21);
        bits0 |= static_cast<uint32_t>((input[8] & 7) << 24);
        bits0 |= static_cast<uint32_t>((input[9] & 7) << 27);
        bits0 |= static_cast<uint32_t>((input[10] & 3) << 30);
        bits1 |= static_cast<uint32_t>((input[10] & 4) >> 2);
        bits1 |= static_cast<uint32_t>((input[11] & 7) << 1);
        bits1 |= static_cast<uint32_t>((input[12] & 7) << 4);
        bits1 |= static_cast<uint32_t>((input[13] & 7) << 7);
        bits1 |= static_cast<uint32_t>((input[14] & 7) << 10);
        bits1 |= static_cast<uint32_t>((input[15] & 7) << 13);
        _data.append(reinterpret_cast<uint8_t*>(&bits0), sizeof(bits0));
        _data.append(reinterpret_cast<uint8_t*>(&bits1), sizeof(bits1));
    }

    void _pack4(const row_t& input) {
        uint64_t bits0 = 0;
        bits0 |= static_cast<uint64_t>((input[0] & 0xF));
        bits0 |= static_cast<uint64_t>((input[1] & 0xF) << 4);
        bits0 |= static_cast<uint64_t>((input[2] & 0xF) << 8);
        bits0 |= static_cast<uint64_t>((input[3] & 0xF) << 12);
        bits0 |= static_cast<uint64_t>((input[4] & 0xF) << 16);
        bits0 |= static_cast<uint64_t>((input[5] & 0xF) << 20);
        bits0 |= static_cast<uint64_t>((input[6] & 0xF) << 24);
        bits0 |= static_cast<uint64_t>((input[7] & 0xF) << 28);
        bits0 |= static_cast<uint64_t>((input[8] & 0xF) << 32);
        bits0 |= static_cast<uint64_t>((input[9] & 0xF) << 36);
        bits0 |= static_cast<uint64_t>((input[10] & 0xF) << 40);
        bits0 |= static_cast<uint64_t>((input[11] & 0xF) << 44);
        bits0 |= static_cast<uint64_t>((input[12] & 0xF) << 48);
        bits0 |= static_cast<uint64_t>((input[13] & 0xF) << 52);
        bits0 |= static_cast<uint64_t>((input[14] & 0xF) << 56);
        bits0 |= static_cast<uint64_t>((input[15] & 0xF) << 60);
        _data.append(reinterpret_cast<uint8_t*>(&bits0), sizeof(bits0));
    }

    void _pack5(const row_t& input) {
        uint64_t bits0 = 0;
        uint16_t bits1 = 0;
        bits0 |= static_cast<uint64_t>((input[0] & 0x1F));
        bits0 |= static_cast<uint64_t>((input[1] & 0x1F) << 5);
        bits0 |= static_cast<uint64_t>((input[2] & 0x1F) << 10);
        bits0 |= static_cast<uint64_t>((input[3] & 0x1F) << 15);
        bits0 |= static_cast<uint64_t>((input[4] & 0x1F) << 20);
        bits0 |= static_cast<uint64_t>((input[5] & 0x1F) << 25);
        bits0 |= static_cast<uint64_t>((input[6] & 0x1F) << 30);
        bits0 |= static_cast<uint64_t>((input[7] & 0x1F) << 35);
        bits0 |= static_cast<uint64_t>((input[8] & 0x1F) << 40);
        bits0 |= static_cast<uint64_t>((input[9] & 0x1F) << 45);
        bits0 |= static_cast<uint64_t>((input[10] & 0x1F) << 50);
        bits0 |= static_cast<uint64_t>((input[11] & 0x1F) << 55);
        bits0 |= static_cast<uint64_t>((input[12] & 0x0F) << 60);
        bits1 |= static_cast<uint32_t>((input[12] & 0x10) >> 4);
        bits1 |= static_cast<uint32_t>((input[13] & 0x1F) << 1);
        bits1 |= static_cast<uint32_t>((input[14] & 0x1F) << 6);
        bits1 |= static_cast<uint32_t>((input[15] & 0x1F) << 11);
        _data.append(reinterpret_cast<uint8_t*>(&bits0), sizeof(bits0));
        _data.append(reinterpret_cast<uint8_t*>(&bits1), sizeof(bits1));
    }

    void _pack6(const row_t& input) {
        uint64_t bits0 = 0;
        uint32_t bits1 = 0;
        bits0 |= static_cast<uint64_t>((input[0] & 0x3F));
        bits0 |= static_cast<uint64_t>((input[1] & 0x3F) << 6);
        bits0 |= static_cast<uint64_t>((input[2] & 0x3F) << 12);
        bits0 |= static_cast<uint64_t>((input[3] & 0x3F) << 18);
        bits0 |= static_cast<uint64_t>((input[4] & 0x3F) << 24);
        bits0 |= static_cast<uint64_t>((input[5] & 0x3F) << 30);
        bits0 |= static_cast<uint64_t>((input[6] & 0x3F) << 36);
        bits0 |= static_cast<uint64_t>((input[7] & 0x3F) << 42);
        bits0 |= static_cast<uint64_t>((input[8] & 0x3F) << 48);
        bits0 |= static_cast<uint64_t>((input[9] & 0x3F) << 54);
        bits0 |= static_cast<uint64_t>((input[10] & 0x0F) << 60);
        bits1 |= static_cast<uint32_t>((input[10] & 0x30) >> 4);
        bits1 |= static_cast<uint32_t>((input[11] & 0x3F) << 2);
        bits1 |= static_cast<uint32_t>((input[12] & 0x3F) << 8);
        bits1 |= static_cast<uint32_t>((input[13] & 0x3F) << 14);
        bits1 |= static_cast<uint32_t>((input[14] & 0x3F) << 20);
        bits1 |= static_cast<uint32_t>((input[15] & 0x3F) << 26);
        _data.append(reinterpret_cast<uint8_t*>(&bits0), sizeof(bits0));
        _data.append(reinterpret_cast<uint8_t*>(&bits1), sizeof(bits1));
    }

    void _pack7(const row_t& input) {
        uint64_t bits0 = 0;
        uint32_t bits1 = 0;
        uint16_t bits2 = 0;
        bits0 |= static_cast<uint64_t>((input[0] & 0x7F));
        bits0 |= static_cast<uint64_t>((input[1] & 0x7F) << 7);
        bits0 |= static_cast<uint64_t>((input[2] & 0x7F) << 14);
        bits0 |= static_cast<uint64_t>((input[3] & 0x7F) << 21);
        bits0 |= static_cast<uint64_t>((input[4] & 0x7F) << 28);
        bits0 |= static_cast<uint64_t>((input[5] & 0x7F) << 35);
        bits0 |= static_cast<uint64_t>((input[6] & 0x7F) << 42);
        bits0 |= static_cast<uint64_t>((input[7] & 0x7F) << 49);
        bits0 |= static_cast<uint64_t>((input[8] & 0x7F) << 56);
        bits0 |= static_cast<uint64_t>((input[9] & 0x01) << 63);
        bits1 |= static_cast<uint32_t>((input[9] & 0x7E) >> 1);
        bits1 |= static_cast<uint32_t>((input[10] & 0x7F) << 6);
        bits1 |= static_cast<uint32_t>((input[11] & 0x7F) << 13);
        bits1 |= static_cast<uint32_t>((input[12] & 0x7F) << 20);
        bits1 |= static_cast<uint32_t>((input[13] & 0x1F) << 27);
        bits2 |= static_cast<uint16_t>((input[13] & 0x60) >> 5);
        bits2 |= static_cast<uint16_t>((input[14] & 0x7F) << 2);
        bits2 |= static_cast<uint16_t>((input[15] & 0x7F) << 9);
        _data.append(reinterpret_cast<uint8_t*>(&bits0), sizeof(bits0));
        _data.append(reinterpret_cast<uint8_t*>(&bits1), sizeof(bits1));
        _data.append(reinterpret_cast<uint8_t*>(&bits2), sizeof(bits2));
    }

    void pack(row_t& input, int n) {
        switch (n) {
        case 0:
            break;
        case 1:
            _pack1(input);
            break;
        case 2:
            _pack2(input);
            break;
        case 3:
            _pack3(input);
            break;
        case 4:
            _pack4(input);
            break;
        case 5:
            _pack5(input);
            break;
        case 6:
            _pack6(input);
            break;
        case 7:
            _pack7(input);
            break;
        case 8:
            _pack_as<uint8_t>(input);
            break;
        case 9:
            _pack_shift<uint8_t>(input);
            _pack1(input);
            break;
        case 10:
            _pack_shift<uint8_t>(input);
            _pack2(input);
            break;
        case 11:
            _pack_shift<uint8_t>(input);
            _pack3(input);
            break;
        case 12:
            _pack_shift<uint8_t>(input);
            _pack4(input);
            break;
        case 13:
            _pack_shift<uint8_t>(input);
            _pack5(input);
            break;
        case 14:
            _pack_shift<uint8_t>(input);
            _pack6(input);
            break;
        case 15:
            _pack_shift<uint8_t>(input);
            _pack7(input);
            break;
        case 16:
            _pack_as<uint16_t>(input);
            break;
        case 17:
            _pack_shift<uint16_t>(input);
            _pack1(input);
            break;
        case 18:
            _pack_shift<uint16_t>(input);
            _pack2(input);
            break;
        case 19:
            _pack_shift<uint16_t>(input);
            _pack3(input);
            break;
        case 20:
            _pack_shift<uint16_t>(input);
            _pack4(input);
            break;
        case 21:
            _pack_shift<uint16_t>(input);
            _pack5(input);
            break;
        case 22:
            _pack_shift<uint16_t>(input);
            _pack6(input);
            break;
        case 23:
            _pack_shift<uint16_t>(input);
            _pack7(input);
            break;
        case 24:
            _pack_shift<uint16_t>(input);
            _pack_as<uint8_t>(input);
            break;
        case 25:
            _pack_shift_all<uint16_t, uint8_t>(input);
            _pack1(input);
            break;
        case 26:
            _pack_shift_all<uint16_t, uint8_t>(input);
            _pack2(input);
            break;
        case 27:
            _pack_shift_all<uint16_t, uint8_t>(input);
            _pack3(input);
            break;
        case 28:
            _pack_shift_all<uint16_t, uint8_t>(input);
            _pack4(input);
            break;
        case 29:
            _pack_shift_all<uint16_t, uint8_t>(input);
            _pack5(input);
            break;
        case 30:
            _pack_shift_all<uint16_t, uint8_t>(input);
            _pack6(input);
            break;
        case 31:
            _pack_shift_all<uint16_t, uint8_t>(input);
            _pack7(input);
            break;
        case 32:
            _pack_as<uint32_t>(input);
            break;
        case 33:
            _pack_shift<uint32_t>(input);
            _pack1(input);
            break;
        case 34:
            _pack_shift<uint32_t>(input);
            _pack2(input);
            break;
        case 35:
            _pack_shift<uint32_t>(input);
            _pack3(input);
            break;
        case 36:
            _pack_shift<uint32_t>(input);
            _pack4(input);
            break;
        case 37:
            _pack_shift<uint32_t>(input);
            _pack5(input);
            break;
        case 38:
            _pack_shift<uint32_t>(input);
            _pack6(input);
            break;
        case 39:
            _pack_shift<uint32_t>(input);
            _pack7(input);
            break;
        case 40:
            _pack_shift<uint32_t>(input);
            _pack_as<uint8_t>(input);
            break;
        case 41:
            _pack_shift_all<uint32_t, uint8_t>(input);
            _pack1(input);
            break;
        case 42:
            _pack_shift_all<uint32_t, uint8_t>(input);
            _pack2(input);
            break;
        case 43:
            _pack_shift_all<uint32_t, uint8_t>(input);
            _pack3(input);
            break;
        case 44:
            _pack_shift_all<uint32_t, uint8_t>(input);
            _pack4(input);
            break;
        case 45:
            _pack_shift_all<uint32_t, uint8_t>(input);
            _pack5(input);
            break;
        case 46:
            _pack_shift_all<uint32_t, uint8_t>(input);
            _pack6(input);
            break;
        case 47:
            _pack_shift_all<uint32_t, uint8_t>(input);
            _pack7(input);
            break;
        case 48:
            _pack_shift<uint32_t>(input);
            _pack_as<uint16_t>(input);
            break;
        case 49:
            _pack_shift_all<uint32_t, uint16_t>(input);
            _pack1(input);
            break;
        case 50:
            _pack_shift_all<uint32_t, uint16_t>(input);
            _pack2(input);
            break;
        case 51:
            _pack_shift_all<uint32_t, uint16_t>(input);
            _pack3(input);
            break;
        case 52:
            _pack_shift_all<uint32_t, uint16_t>(input);
            _pack4(input);
            break;
        case 53:
            _pack_shift_all<uint32_t, uint16_t>(input);
            _pack5(input);
            break;
        case 54:
            _pack_shift_all<uint32_t, uint16_t>(input);
            _pack6(input);
            break;
        case 55:
            _pack_shift_all<uint32_t, uint16_t>(input);
            _pack7(input);
            break;
        case 56:
            _pack_shift_all<uint32_t, uint16_t>(input);
            _pack_as<uint8_t>(input);
            break;
        case 57:
            _pack_shift_all<uint32_t, uint16_t, uint8_t>(input);
            _pack1(input);
            break;
        case 58:
            _pack_shift_all<uint32_t, uint16_t, uint8_t>(input);
            _pack2(input);
            break;
        case 59:
            _pack_shift_all<uint32_t, uint16_t, uint8_t>(input);
            _pack3(input);
            break;
        case 60:
            _pack_shift_all<uint32_t, uint16_t, uint8_t>(input);
            _pack4(input);
            break;
        case 61:
            _pack_shift_all<uint32_t, uint16_t, uint8_t>(input);
            _pack5(input);
            break;
        case 62:
            _pack_shift_all<uint32_t, uint16_t, uint8_t>(input);
            _pack6(input);
            break;
        case 63:
            _pack_shift_all<uint32_t, uint16_t, uint8_t>(input);
            _pack7(input);
            break;
        case 64:
            return _pack_as<uint64_t>(input);
        }
    }

protected:
    TVal _initial{};
    TVal _last{};
    mutable iobuf _data{};
    uint32_t _cnt{};
    DeltaStep _delta{delta_alg};
};

/** \brief Delta-FOR decoder
 *
 * The object can be used to decode the iobuf copied from the encoder.
 * It can only read the whole sequence once. Once it's done reading
 * it can't be reset to read the sequence again.
 *
 * The initial_value and number of rows should match the corresponding
 * encoder which was used to compress the data.
 */
template<class TVal, class DeltaStep = details::delta_xor>
class deltafor_decoder {
    static constexpr uint32_t row_width = details::FOR_buffer_depth;

public:
    explicit deltafor_decoder(
      TVal initial_value, uint32_t cnt, iobuf data, DeltaStep delta = {})
      : _initial(initial_value)
      , _total{cnt}
      , _pos{0}
      , _data(std::move(data))
      , _delta(delta) {}

    using row_t = std::array<TVal, row_width>;

    /// Decode single row
    bool read(row_t& row) {
        if (_pos == _total) {
            return false;
        }
        auto bytes = _data.read_bytes(1);
        uint8_t nbits = *bytes.data();
        unpack(row, nbits);
        _initial = _delta.decode(_initial, row);
        _pos++;
        return true;
    }

    /// Skip rows
    void skip(const deltafor_stream_pos_t<TVal>& st) {
        _data.skip(st.offset);
        _initial = st.initial;
        _pos = st.num_rows;
    }

private:
    template<typename T>
    void _unpack_as(row_t& input, unsigned shift) {
        static_assert(
          std::is_unsigned<T>::value,
          "Only unsigned integer can be used as a type parameter");
        for (uint32_t i = 0; i < row_width; i++) {
            static_assert(sizeof(T) <= sizeof(uint64_t));
            T val{};
            auto bytes = _data.read_bytes(sizeof(T));
            std::memcpy(&val, bytes.data(), bytes.size());
            input[i] |= static_cast<uint64_t>(val) << shift;
        }
    }

    void _unpack1(row_t& output, unsigned shift) {
        uint16_t bits{};
        auto bytes = _data.read_bytes(sizeof(bits));
        std::memcpy(&bits, bytes.data(), bytes.size());
        for (uint32_t i = 0; i < row_width; i++) {
            output[i] |= static_cast<uint64_t>((bits & (1U << i)) >> i)
                         << shift;
        }
    }

    void _unpack2(row_t& output, unsigned shift) {
        uint32_t bits{};
        auto bytes = _data.read_bytes(sizeof(bits));
        std::memcpy(&bits, bytes.data(), bytes.size());
        for (uint32_t i = 0; i < row_width; i++) {
            output[i] |= static_cast<uint64_t>((bits & (3U << 2 * i)) >> 2 * i)
                         << shift;
        }
    }

    void _unpack3(row_t& output, unsigned shift) {
        uint32_t tmp32{};
        auto bytes = _data.read_bytes(sizeof(tmp32));
        std::memcpy(&tmp32, bytes.data(), bytes.size());
        uint64_t bits0 = tmp32;
        uint16_t tmp16{};
        bytes = _data.read_bytes(sizeof(tmp16));
        std::memcpy(&tmp16, bytes.data(), bytes.size());
        uint64_t bits1 = tmp16;
        output[0] |= ((bits0 & 7U)) << shift;
        output[1] |= ((bits0 & (7U << 3U)) >> 3U) << shift;
        output[2] |= ((bits0 & (7U << 6U)) >> 6U) << shift;
        output[3] |= ((bits0 & (7U << 9U)) >> 9U) << shift;
        output[4] |= ((bits0 & (7U << 12U)) >> 12U) << shift;
        output[5] |= ((bits0 & (7U << 15U)) >> 15U) << shift;
        output[6] |= ((bits0 & (7U << 18U)) >> 18U) << shift;
        output[7] |= ((bits0 & (7U << 21U)) >> 21U) << shift;
        output[8] |= ((bits0 & (7U << 24U)) >> 24U) << shift;
        output[9] |= ((bits0 & (7U << 27U)) >> 27U) << shift;
        output[10] |= (((bits0 & (3U << 30U)) >> 30U) | ((bits1 & 1U) << 2U))
                      << shift;
        output[11] |= ((bits1 & (7U << 1U)) >> 1U) << shift;
        output[12] |= ((bits1 & (7U << 4U)) >> 4U) << shift;
        output[13] |= ((bits1 & (7U << 7U)) >> 7U) << shift;
        output[14] |= ((bits1 & (7U << 10U)) >> 10U) << shift;
        output[15] |= ((bits1 & (7U << 13U)) >> 13U) << shift;
    }

    void _unpack4(row_t& output, unsigned shift) {
        uint64_t bits0{};
        auto bytes = _data.read_bytes(sizeof(bits0));
        std::memcpy(&bits0, bytes.data(), bytes.size());
        output[0] |= ((bits0 & 15U)) << shift;
        output[1] |= ((bits0 & (15ULL << 4U)) >> 4U) << shift;
        output[2] |= ((bits0 & (15ULL << 8U)) >> 8U) << shift;
        output[3] |= ((bits0 & (15ULL << 12U)) >> 12U) << shift;
        output[4] |= ((bits0 & (15ULL << 16U)) >> 16U) << shift;
        output[5] |= ((bits0 & (15ULL << 20U)) >> 20U) << shift;
        output[6] |= ((bits0 & (15ULL << 24U)) >> 24U) << shift;
        output[7] |= ((bits0 & (15ULL << 28U)) >> 28U) << shift;
        output[8] |= ((bits0 & (15ULL << 32U)) >> 32U) << shift;
        output[9] |= ((bits0 & (15ULL << 36U)) >> 36U) << shift;
        output[10] |= ((bits0 & (15ULL << 40U)) >> 40U) << shift;
        output[11] |= ((bits0 & (15ULL << 44U)) >> 44U) << shift;
        output[12] |= ((bits0 & (15ULL << 48U)) >> 48U) << shift;
        output[13] |= ((bits0 & (15ULL << 52U)) >> 52U) << shift;
        output[14] |= ((bits0 & (15ULL << 56U)) >> 56U) << shift;
        output[15] |= ((bits0 & (15ULL << 60U)) >> 60U) << shift;
    }

    void _unpack5(row_t& output, unsigned shift) {
        uint64_t bits0{};
        auto bytes = _data.read_bytes(sizeof(bits0));
        std::memcpy(&bits0, bytes.data(), bytes.size());
        uint16_t tmp16{};
        bytes = _data.read_bytes(sizeof(tmp16));
        std::memcpy(&tmp16, bytes.data(), bytes.size());
        uint64_t bits1 = tmp16;
        output[0] |= ((bits0 & 0x1FU)) << shift;
        output[1] |= ((bits0 & (0x1FULL << 5U)) >> 5U) << shift;
        output[2] |= ((bits0 & (0x1FULL << 10U)) >> 10U) << shift;
        output[3] |= ((bits0 & (0x1FULL << 15U)) >> 15U) << shift;
        output[4] |= ((bits0 & (0x1FULL << 20U)) >> 20U) << shift;
        output[5] |= ((bits0 & (0x1FULL << 25U)) >> 25U) << shift;
        output[6] |= ((bits0 & (0x1FULL << 30U)) >> 30U) << shift;
        output[7] |= ((bits0 & (0x1FULL << 35U)) >> 35U) << shift;
        output[8] |= ((bits0 & (0x1FULL << 40U)) >> 40U) << shift;
        output[9] |= ((bits0 & (0x1FULL << 45U)) >> 45U) << shift;
        output[10] |= ((bits0 & (0x1FULL << 50U)) >> 50U) << shift;
        output[11] |= ((bits0 & (0x1FULL << 55U)) >> 55U) << shift;
        output[12]
          |= (((bits0 & (0x0FULL << 60U)) >> 60U) | ((bits1 & 1U) << 4U))
             << shift;
        output[13] |= ((bits1 & (0x1FULL << 1U)) >> 1U) << shift;
        output[14] |= ((bits1 & (0x1FULL << 6U)) >> 6U) << shift;
        output[15] |= ((bits1 & (0x1FULL << 11U)) >> 11U) << shift;
    }

    void _unpack6(row_t& output, unsigned shift) {
        uint64_t bits0{};
        auto bytes = _data.read_bytes(sizeof(bits0));
        std::memcpy(&bits0, bytes.data(), bytes.size());
        uint32_t tmp32{};
        bytes = _data.read_bytes(sizeof(tmp32));
        std::memcpy(&tmp32, bytes.data(), bytes.size());
        uint64_t bits1 = tmp32;
        output[0] |= ((bits0 & 0x3FU)) << shift;
        output[1] |= ((bits0 & (0x3FULL << 6U)) >> 6U) << shift;
        output[2] |= ((bits0 & (0x3FULL << 12U)) >> 12U) << shift;
        output[3] |= ((bits0 & (0x3FULL << 18U)) >> 18U) << shift;
        output[4] |= ((bits0 & (0x3FULL << 24U)) >> 24U) << shift;
        output[5] |= ((bits0 & (0x3FULL << 30U)) >> 30U) << shift;
        output[6] |= ((bits0 & (0x3FULL << 36U)) >> 36U) << shift;
        output[7] |= ((bits0 & (0x3FULL << 42U)) >> 42U) << shift;
        output[8] |= ((bits0 & (0x3FULL << 48U)) >> 48U) << shift;
        output[9] |= ((bits0 & (0x3FULL << 54U)) >> 54U) << shift;
        output[10]
          |= (((bits0 & (0xFULL << 60U)) >> 60U) | (bits1 & 0x3U) << 4U)
             << shift;
        output[11] |= ((bits1 & (0x3FULL << 2U)) >> 2U) << shift;
        output[12] |= ((bits1 & (0x3FULL << 8U)) >> 8U) << shift;
        output[13] |= ((bits1 & (0x3FULL << 14U)) >> 14U) << shift;
        output[14] |= ((bits1 & (0x3FULL << 20U)) >> 20U) << shift;
        output[15] |= ((bits1 & (0x3FULL << 26U)) >> 26U) << shift;
    }

    void _unpack7(row_t& output, unsigned shift) {
        uint64_t bits0{};
        auto bytes = _data.read_bytes(sizeof(bits0));
        std::memcpy(&bits0, bytes.data(), bytes.size());
        uint32_t tmp32{};
        bytes = _data.read_bytes(sizeof(tmp32));
        std::memcpy(&tmp32, bytes.data(), bytes.size());
        uint64_t bits1 = tmp32;
        uint16_t tmp16{};
        bytes = _data.read_bytes(sizeof(tmp16));
        std::memcpy(&tmp16, bytes.data(), bytes.size());
        uint64_t bits2 = tmp16;
        output[0] |= ((bits0 & 0x7FU)) << shift;
        output[1] |= ((bits0 & (0x7FULL << 7U)) >> 7U) << shift;
        output[2] |= ((bits0 & (0x7FULL << 14U)) >> 14U) << shift;
        output[3] |= ((bits0 & (0x7FULL << 21U)) >> 21U) << shift;
        output[4] |= ((bits0 & (0x7FULL << 28U)) >> 28U) << shift;
        output[5] |= ((bits0 & (0x7FULL << 35U)) >> 35U) << shift;
        output[6] |= ((bits0 & (0x7FULL << 42U)) >> 42U) << shift;
        output[7] |= ((bits0 & (0x7FULL << 49U)) >> 49U) << shift;
        output[8] |= ((bits0 & (0x7FULL << 56U)) >> 56U) << shift;
        output[9]
          |= (((bits0 & (0x01ULL << 63U)) >> 63U) | ((bits1 & 0x3FU) << 1U))
             << shift;
        output[10] |= ((bits1 & (0x7FULL << 6U)) >> 6U) << shift;
        output[11] |= ((bits1 & (0x7FULL << 13U)) >> 13U) << shift;
        output[12] |= ((bits1 & (0x7FULL << 20U)) >> 20U) << shift;
        output[13]
          |= (((bits1 & (0x1FULL << 27U)) >> 27U) | ((bits2 & 0x03U) << 5U))
             << shift;
        output[14] |= ((bits2 & (0x7FULL << 2U)) >> 2U) << shift;
        output[15] |= ((bits2 & (0x7FULL << 9U)) >> 9U) << shift;
    }

    void unpack(row_t& output, int n) {
        switch (n) {
        case 0:
            break;
        case 1:
            _unpack1(output, 0);
            break;
        case 2:
            _unpack2(output, 0);
            break;
        case 3:
            _unpack3(output, 0);
            break;
        case 4:
            _unpack4(output, 0);
            break;
        case 5:
            _unpack5(output, 0);
            break;
        case 6:
            _unpack6(output, 0);
            break;
        case 7:
            _unpack7(output, 0);
            break;
        case 8:
            _unpack_as<uint8_t>(output, 0);
            break;
        case 9:
            _unpack_as<uint8_t>(output, 0);
            _unpack1(output, 8);
            break;
        case 10:
            _unpack_as<uint8_t>(output, 0);
            _unpack2(output, 8);
            break;
        case 11:
            _unpack_as<uint8_t>(output, 0);
            _unpack3(output, 8);
            break;
        case 12:
            _unpack_as<uint8_t>(output, 0);
            _unpack4(output, 8);
            break;
        case 13:
            _unpack_as<uint8_t>(output, 0);
            _unpack5(output, 8);
            break;
        case 14:
            _unpack_as<uint8_t>(output, 0);
            _unpack6(output, 8);
            break;
        case 15:
            _unpack_as<uint8_t>(output, 0);
            _unpack7(output, 8);
            break;
        case 16:
            _unpack_as<uint16_t>(output, 0);
            break;
        case 17:
            _unpack_as<uint16_t>(output, 0);
            _unpack1(output, 16);
            break;
        case 18:
            _unpack_as<uint16_t>(output, 0);
            _unpack2(output, 16);
            break;
        case 19:
            _unpack_as<uint16_t>(output, 0);
            _unpack3(output, 16);
            break;
        case 20:
            _unpack_as<uint16_t>(output, 0);
            _unpack4(output, 16);
            break;
        case 21:
            _unpack_as<uint16_t>(output, 0);
            _unpack5(output, 16);
            break;
        case 22:
            _unpack_as<uint16_t>(output, 0);
            _unpack6(output, 16);
            break;
        case 23:
            _unpack_as<uint16_t>(output, 0);
            _unpack7(output, 16);
            break;
        case 24:
            _unpack_as<uint16_t>(output, 0);
            _unpack_as<uint8_t>(output, 16);
            break;
        case 25:
            _unpack_as<uint16_t>(output, 0);
            _unpack_as<uint8_t>(output, 16);
            _unpack1(output, 24);
            break;
        case 26:
            _unpack_as<uint16_t>(output, 0);
            _unpack_as<uint8_t>(output, 16);
            _unpack2(output, 24);
            break;
        case 27:
            _unpack_as<uint16_t>(output, 0);
            _unpack_as<uint8_t>(output, 16);
            _unpack3(output, 24);
            break;
        case 28:
            _unpack_as<uint16_t>(output, 0);
            _unpack_as<uint8_t>(output, 16);
            _unpack4(output, 24);
            break;
        case 29:
            _unpack_as<uint16_t>(output, 0);
            _unpack_as<uint8_t>(output, 16);
            _unpack5(output, 24);
            break;
        case 30:
            _unpack_as<uint16_t>(output, 0);
            _unpack_as<uint8_t>(output, 16);
            _unpack6(output, 24);
            break;
        case 31:
            _unpack_as<uint16_t>(output, 0);
            _unpack_as<uint8_t>(output, 16);
            _unpack7(output, 24);
            break;
        case 32:
            _unpack_as<uint32_t>(output, 0);
            break;
        case 33:
            _unpack_as<uint32_t>(output, 0);
            _unpack1(output, 32);
            break;
        case 34:
            _unpack_as<uint32_t>(output, 0);
            _unpack2(output, 32);
            break;
        case 35:
            _unpack_as<uint32_t>(output, 0);
            _unpack3(output, 32);
            break;
        case 36:
            _unpack_as<uint32_t>(output, 0);
            _unpack4(output, 32);
            break;
        case 37:
            _unpack_as<uint32_t>(output, 0);
            _unpack5(output, 32);
            break;
        case 38:
            _unpack_as<uint32_t>(output, 0);
            _unpack6(output, 32);
            break;
        case 39:
            _unpack_as<uint32_t>(output, 0);
            _unpack7(output, 32);
            break;
        case 40:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint8_t>(output, 32);
            break;
        case 41:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint8_t>(output, 32);
            _unpack1(output, 40);
            break;
        case 42:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint8_t>(output, 32);
            _unpack2(output, 40);
            break;
        case 43:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint8_t>(output, 32);
            _unpack3(output, 40);
            break;
        case 44:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint8_t>(output, 32);
            _unpack4(output, 40);
            break;
        case 45:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint8_t>(output, 32);
            _unpack5(output, 40);
            break;
        case 46:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint8_t>(output, 32);
            _unpack6(output, 40);
            break;
        case 47:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint8_t>(output, 32);
            _unpack7(output, 40);
            break;
        case 48:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint16_t>(output, 32);
            break;
        case 49:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint16_t>(output, 32);
            _unpack1(output, 48);
            break;
        case 50:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint16_t>(output, 32);
            _unpack2(output, 48);
            break;
        case 51:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint16_t>(output, 32);
            _unpack3(output, 48);
            break;
        case 52:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint16_t>(output, 32);
            _unpack4(output, 48);
            break;
        case 53:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint16_t>(output, 32);
            _unpack5(output, 48);
            break;
        case 54:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint16_t>(output, 32);
            _unpack6(output, 48);
            break;
        case 55:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint16_t>(output, 32);
            _unpack7(output, 48);
            break;
        case 56:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint16_t>(output, 32);
            _unpack_as<uint8_t>(output, 48);
            break;
        case 57:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint16_t>(output, 32);
            _unpack_as<uint8_t>(output, 48);
            _unpack1(output, 56);
            break;
        case 58:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint16_t>(output, 32);
            _unpack_as<uint8_t>(output, 48);
            _unpack2(output, 56);
            break;
        case 59:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint16_t>(output, 32);
            _unpack_as<uint8_t>(output, 48);
            _unpack3(output, 56);
            break;
        case 60:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint16_t>(output, 32);
            _unpack_as<uint8_t>(output, 48);
            _unpack4(output, 56);
            break;
        case 61:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint16_t>(output, 32);
            _unpack_as<uint8_t>(output, 48);
            _unpack5(output, 56);
            break;
        case 62:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint16_t>(output, 32);
            _unpack_as<uint8_t>(output, 48);
            _unpack6(output, 56);
            break;
        case 63:
            _unpack_as<uint32_t>(output, 0);
            _unpack_as<uint16_t>(output, 32);
            _unpack_as<uint8_t>(output, 48);
            _unpack7(output, 56);
            break;
        case 64:
            _unpack_as<uint64_t>(output, 0);
            break;
        }
    }

    TVal _initial;
    uint32_t _total;
    uint32_t _pos;
    iobuf_parser _data;
    DeltaStep _delta;
};
