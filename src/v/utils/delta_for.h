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

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "serde/envelope.h"
#include "serde/rw/array.h"
#include "serde/rw/envelope.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/optional.h"
#include "serde/rw/rw.h"
#include "serde/rw/scalar.h"
#include "serde/rw/tags.h"
#include "ssx/sformat.h"

#include <seastar/util/log.hh>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <list>
#include <optional>
#include <span>
#include <type_traits>
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
      std::span<const value_t, row_width> row,
      std::span<value_t, row_width> buf) const {
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
    decode(value_t initial, std::span<value_t, row_width> row) const {
        auto p = initial;
        for (unsigned i = 0; i < row_width; i++) {
            row[i] = row[i] ^ p;
            p = row[i];
        }
        return p;
    }

    bool operator==(const delta_xor&) const = default;
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
      std::span<const value_t, row_width> row,
      std::span<value_t, row_width> buf) const {
        auto p = last;
        uint64_t agg = 0;
        for (uint32_t i = 0; i < row_width; ++i) {
            vassert(
              row[i] >= p,
              "Value {} can't be smaller than the previous one {}",
              row[i],
              p);
            auto delta = row[i] - p;
            vassert(
              delta >= _step_size,
              "Delta {} can't be smaller than step size {}",
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
    decode(value_t initial, std::span<value_t, row_width> row) const {
        auto p = initial;
        for (unsigned i = 0; i < row_width; i++) {
            row[i] = row[i] + p + _step_size;
            p = row[i];
        }
        return p;
    }

    ValueT _step_size;

    bool operator==(const delta_delta&) const = default;
};

namespace decomp {
// this namespace contains instruction on how to decompose N_BITS (<=64) into a
// collection of unsigned types (uint64,32,16,8) and a bunch of residual bits it
// is used to serialize a collection of values reducing the total space used.
// everything is done in alittle endian fashion. example: N_BITS=41, get
// decomposed as a uint32_t for [bits[0], bits[32]), uint8_t for [bits[32],
// bits[40]) and one residual bit for bits[40]

// detail: an array of 4 (is-unsigned-type-used, number-of-bits-of-this-type)
// pairs, from uint64_t to uint8_t, that describes if N_BITS will be decomposed
// with a particular type or not.
template<size_t N_BITS>
constexpr auto detail_decomp = [] {
    auto decomp = std::to_array<std::pair<bool, size_t>>({
      {false, 8 * sizeof(uint64_t)},
      {false, 8 * sizeof(uint32_t)},
      {false, 8 * sizeof(uint16_t)},
      {false, 8 * sizeof(uint8_t)},
    });

    auto rem_bits = N_BITS;
    for (auto& [in_use, sz_bits] : decomp) {
        in_use = rem_bits >= sz_bits;
        if (in_use) {
            rem_bits -= sz_bits;
        }
    }
    return decomp;
}();

// an array of pairs (bytes-to-save, bit-index-of-this-type-in-decomposition)
// that describes how to decompose N_BITS and remains only with a residual part
// < 8 bits
template<size_t N_BITS>
constexpr auto unsigned_decomposition = [] {
    auto decomposition = std::array<
      std::pair<size_t, size_t>,
      std::ranges::count_if(
        detail_decomp<N_BITS>, [](auto& elem) { return elem.first; })>{};
    auto it = decomposition.begin();
    size_t acc = 0;
    for (auto [in_use, sz_bits] : detail_decomp<N_BITS>) {
        if (in_use) {
            *(it++) = std::pair{sz_bits / 8, acc};
            acc += sz_bits;
        }
    }
    if (acc > N_BITS) {
        throw std::runtime_error("unsigned_decomposition: internal error");
    }
    return decomposition;
}();

template<size_t N_BITS>
constexpr auto whole_bytes = N_BITS / 8;
template<size_t N_BITS>
constexpr auto residual_bits = N_BITS % 8;
template<size_t N_BITS>
constexpr auto residual_save_mask = N_BITS == 64
                                      ? uint64_t(-1)
                                      : (uint64_t(1) << (N_BITS)) - 1u;

template<size_t N_BITS, size_t NUM_ELEMENTS>
constexpr auto serialized_size = whole_bytes<N_BITS> * NUM_ELEMENTS
                                 + residual_bits<N_BITS> * NUM_ELEMENTS / 8;

static_assert(
  unsigned_decomposition<41>
  == std::to_array<std::pair<size_t, size_t>>({{4, 0}, {1, 32}}));
static_assert(
  unsigned_decomposition<0> == std::array<std::pair<size_t, size_t>, 0>{});
static_assert(
  unsigned_decomposition<63>
  == std::to_array<std::pair<size_t, size_t>>({{4, 0}, {2, 32}, {1, 48}}));
static_assert(
  unsigned_decomposition<64> == std::array{std::pair{size_t{8}, size_t{0}}});

static_assert(whole_bytes<41> == 5);
static_assert(residual_bits<41> == 1);
static_assert(residual_save_mask<41> == 0x1'ff'ffffffff);

// __uint128_t being non-standard needs some special treatment
template<typename T>
concept unsigned_serializable = std::unsigned_integral<T>
                                || std::same_as<T, __uint128_t>;

template<size_t bytes_to_save>
auto serialize_little_endian(unsigned_serializable auto bits, uint8_t* output) {
    if constexpr (bytes_to_save > 0) {
        static_assert(
          std::endian::native == std::endian::little,
          "to work on a big-endian machine, insert "
          "`bits=std::byteswap(bits);`");
        // little endian view of bits
        auto buff = std::bit_cast<std::array<uint8_t, sizeof(bits)>>(bits);

        return std::ranges::copy_n(buff.begin(), bytes_to_save, output).out;
    }

    return output;
}

template<size_t bytes_to_read>
auto deserialize_little_endian(
  const uint8_t* input, unsigned_serializable auto& output) {
    if constexpr (bytes_to_read > 0) {
        static_assert(
          std::endian::native == std::endian::little,
          "to work on a big-endian machine, insert "
          "`output=std::byteswap(output);`");

        auto buff = std::array<uint8_t, sizeof(output)>{};
        auto in = std::ranges::copy_n(input, bytes_to_read, buff.begin()).in;
        output = std::bit_cast<std::decay_t<decltype(output)>>(buff);
        return in;
    }
    return input;
}
} // namespace decomp
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
    explicit deltafor_encoder(TVal initial_value, DeltaStep delta = {})
    requires(!use_nttp_deltastep)
      : _initial(initial_value)
      , _last(initial_value)
      , _cnt{0}
      , _delta(delta) {}

    constexpr explicit deltafor_encoder(TVal initial_value)
    requires use_nttp_deltastep
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
      DeltaStep delta = {})
    requires(!use_nttp_deltastep)
      : _initial(initial_value)
      , _last(last_value)
      , _data(std::move(data))
      , _cnt(cnt)
      , _delta(delta) {}

    deltafor_encoder(
      TVal initial_value, uint32_t cnt, TVal last_value, iobuf data)
    requires use_nttp_deltastep
      : _initial(initial_value)
      , _last(last_value)
      , _data(std::move(data))
      , _cnt(cnt)
      , _delta(delta_alg) {}

    // This c-tor creates shallow copy of the encoder.
    //
    // The underlying iobuf is shared which makes the operation
    // relatively lightweight. The signature is different from
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
    void add(std::span<const TVal, row_width> row) {
        std::array<TVal, row_width> buf;
        uint8_t nbits = _delta.encode(_last, row, std::span{buf});
        _last = row.back();
        _data.append(&nbits, 1);
        pack(buf, nbits);
        _cnt++;
    }

    /// Return position inside the stream
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

        void add(std::span<const TVal, row_width> row) { uncommitted.add(row); }
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

    /// Returns a deltafor_encoder that shares the underlying iobuf
    /// The method itself is not unsafe, but the returned object can modify the
    /// original object, so users need to think about the use case
    auto unsafe_alias() const -> deltafor_encoder {
        auto tmp = deltafor_encoder{};
        tmp._initial = _initial;
        tmp._last = _last;
        tmp._data = share();
        tmp._cnt = _cnt;

        if constexpr (!use_nttp_deltastep) {
            tmp._delta = _delta;
        }
        return tmp;
    }

    friend bool operator==(
      const deltafor_encoder<TVal, DeltaStep, use_nttp_deltastep, delta_alg>&,
      const deltafor_encoder<TVal, DeltaStep, use_nttp_deltastep, delta_alg>&)
      = default;

private:
    static_assert(
      row_width == 16,
      "pack<N_BITS> assumes row_width = 16, to move N_BITS into a __uint128_t "
      "and to compute how may bytes of the buffer are non-zero");
    template<size_t N_BITS>
    void pack(std::span<const TVal, row_width> input) {
        if constexpr (N_BITS > 0) {
            using namespace details::decomp;
            std::array<uint8_t, serialized_size<N_BITS, row_width>> buff;

            auto end_iter = buff.begin();
            // step 1: extract and serialize whole bytes, following
            // decomposition in words
            constexpr static auto decom = unsigned_decomposition<N_BITS>;
            [&]<size_t... Is>(std::index_sequence<Is...>) {
                (
                  [&] {
                      constexpr auto bytes_to_save = decom[Is].first;
                      constexpr auto shift_of_to_save_section
                        = decom[Is].second;
                      for (auto& e : input) {
                          end_iter = serialize_little_endian<bytes_to_save>(
                            std::make_unsigned_t<TVal>(e)
                              >> shift_of_to_save_section,
                            end_iter);
                      }
                  }(),
                  ...);
            }(std::make_index_sequence<decom.size()>{});

            // step 2: pack leftover bits into a single __uint128_t, little
            // endian wise, and serialize it.
            // masking is not strictly necessary, but it ensures that the
            // elements to not clash once packed
            constexpr static auto residual = residual_bits<N_BITS>;

            if constexpr (residual > 0) {
                constexpr static auto prev_saved_bits = whole_bytes<N_BITS> * 8;
                constexpr static auto mask = residual_save_mask<N_BITS>;
                auto bits = [&]<size_t... Is>(std::index_sequence<Is...>) {
                    // select residual bits, shift them down to zero and then
                    // shift them to the appropriate position
                    return (
                      (__uint128_t{(input[Is] & mask) >> prev_saved_bits}
                       << (residual * Is))
                      | ...);
                }(std::make_index_sequence<input.size()>());

                // find out how many bytes will be non-zero, once the buffer is
                // packed with row_width*N_BITS (this assumes row_width is 16)
                constexpr static auto bytes_to_save = residual * input.size()
                                                      / 8;
                serialize_little_endian<bytes_to_save>(bits, end_iter);
            }
            _data.append(buff.data(), buff.size());
        }
    }

    void pack(std::span<TVal, row_width> input, size_t nbits) {
        // poor man runtime index to constexpr index conversion, a.k.a. switch
        // constexpr
        [&]<size_t... Is>(std::index_sequence<Is...>) {
            ((void)(nbits == Is ? (pack<Is>(input), true) : false), ...);
        }(std::make_index_sequence<sizeof(uint64_t) * 8 + 1>());
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
    bool read(std::span<TVal, row_width> row) {
        if (_pos == _total) {
            return false;
        }
        auto nbits = _data.consume_type<uint8_t>();
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
    template<size_t N_BITS>
    void unpack(std::span<TVal, row_width> output) {
        std::ranges::fill(output, TVal{0});
        if constexpr (N_BITS > 0) {
            using namespace details::decomp;

            std::array<uint8_t, serialized_size<N_BITS, row_width>> tmp_buffer;
            _data.consume_to(tmp_buffer.size(), tmp_buffer.begin());

            auto end_it = tmp_buffer.cbegin();
            // step 1: deserialize whole bytes and paste them in place,
            // following decomposition in words
            constexpr static auto decom = unsigned_decomposition<N_BITS>;
            [&]<size_t... Is>(std::index_sequence<Is...>) {
                (
                  [&] {
                      constexpr auto bytes_to_restore = decom[Is].first;
                      constexpr auto shift_of_restored = decom[Is].second;
                      for (auto& e : output) {
                          auto tmp = std::make_unsigned_t<TVal>{};
                          end_it = deserialize_little_endian<bytes_to_restore>(
                            end_it, tmp);
                          e |= tmp << shift_of_restored;
                      }
                  }(),
                  ...);
            }(std::make_index_sequence<decom.size()>{});

            // step 2: unpack leftover bits into from a single __uint128_t,
            // little endian wise. masking is not strictly necessary, but it
            // ensures that the elements to not clash once unpacked
            constexpr static auto residual = residual_bits<N_BITS>;
            if constexpr (residual > 0) {
                constexpr static auto prev_saved_bits = whole_bytes<N_BITS> * 8;
                constexpr static auto mask = residual_save_mask<N_BITS>;

                constexpr static auto bytes_to_restore = residual
                                                         * output.size() / 8;
                auto bits = __uint128_t{};
                deserialize_little_endian<bytes_to_restore>(end_it, bits);

                [&]<size_t... Is>(std::index_sequence<Is...>) {
                    // select residual bits, shift them down to zero and then
                    // shift them to the appropriate position
                    return (
                      (output[Is]
                       |= ((bits >> (residual * Is)) << prev_saved_bits) & mask)
                      | ...);
                }(std::make_index_sequence<output.size()>());
            }
        }
    }
    void unpack(std::span<TVal, row_width> output, uint8_t n) {
        [&]<size_t... Is>(std::index_sequence<Is...>) {
            ((void)(n == Is ? (unpack<Is>(output), true) : false), ...);
        }(std::make_index_sequence<sizeof(uint64_t) * 8 + 1>{});
    }

    TVal _initial;
    uint32_t _total;
    uint32_t _pos;
    iobuf_parser _data;
    DeltaStep _delta;
};

template<class value_t, class decoder_t>
class deltafor_frame_const_iterator
  : public boost::iterator_facade<
      deltafor_frame_const_iterator<value_t, decoder_t>,
      const value_t,
      boost::iterators::forward_traversal_tag> {
    constexpr static uint32_t buffer_depth = details::FOR_buffer_depth;
    constexpr static uint32_t index_mask = buffer_depth - 1;

    friend class boost::iterator_core_access;
    using self_t = deltafor_frame_const_iterator<value_t, decoder_t>;

public:
    /// Create iterator that points to the beginning
    explicit deltafor_frame_const_iterator(
      decoder_t decoder,
      const std::array<value_t, buffer_depth>& head,
      uint32_t size,
      uint32_t pos = 0,
      value_t frame_initial_value = {})
      : _head(head)
      , _decoder(std::move(decoder))
      , _pos(pos)
      , _size(size)
      , _frame_initial(frame_initial_value) {
        if (!_decoder->read(_read_buf)) {
            _read_buf = _head;
        }
    }

    /// Create iterator that points to the end
    deltafor_frame_const_iterator() = default;

    uint32_t index() const { return _pos; }

    value_t get_frame_initial_value() const noexcept { return _frame_initial; }

private:
    const value_t& dereference() const {
        auto ix = _pos & index_mask;
        return _read_buf.at(ix);
    }

    void increment() {
        _pos++;
        if ((_pos & index_mask) == 0) {
            // Read next buffer from the decoder
            _read_buf = {};
            if (!_decoder->read(_read_buf)) {
                // If the decoder is empty we need to continue reading data from
                // the head of the column.
                _read_buf = _head;
            }
        }
    }

    bool equal(const self_t& other) const {
        if (other._pos == other._size) {
            // All 'end' iterators are equal
            return _pos == _size;
        }
        return other._pos == _pos && other._size == _size;
    }

    std::array<value_t, buffer_depth> _read_buf{};
    std::array<value_t, buffer_depth> _head{};
    std::optional<decoder_t> _decoder{std::nullopt};
    uint32_t _pos{0};
    uint32_t _size{0};
    value_t _frame_initial;
};

struct share_frame_t {};
inline constexpr auto share_frame = share_frame_t{};

template<class value_t, auto delta_alg_instance = details::delta_xor{}>
class deltafor_frame
  : public serde::envelope<
      deltafor_frame<value_t, delta_alg_instance>,
      serde::version<0>,
      serde::compat_version<0>> {
public:
    using delta_alg = std::remove_cvref_t<decltype(delta_alg_instance)>;

private:
    constexpr static uint32_t buffer_depth = details::FOR_buffer_depth;
    constexpr static uint32_t index_mask = buffer_depth - 1;
    using encoder_t
      = deltafor_encoder<value_t, delta_alg, true, delta_alg_instance>;
    using decoder_t = deltafor_decoder<value_t, delta_alg>;

    struct index_value {
        size_t ix;
        value_t value;
    };

    using self_t = deltafor_frame<value_t, delta_alg_instance>;

public:
    deltafor_frame() = default;

    // constructor that will share the underlying buffer of src
    deltafor_frame(share_frame_t, deltafor_frame& src) noexcept
      : _head{src._head}
      , _tail{[&] {
          if (src._tail.has_value()) {
              // invoke share constructor by passing a pointer to encoder_t
              return std::optional<encoder_t>{
                std::in_place, &(src._tail.value())};
          }
          return std::optional<encoder_t>{};
      }()}
      , _size{src._size}
      , _last_row{src._last_row} {}

    using const_iterator = deltafor_frame_const_iterator<value_t, decoder_t>;

    using hint_t = deltafor_stream_pos_t<value_t>;

    void append(value_t value) {
        auto ix = index_mask & _size++;
        _head.at(ix) = value;
        if ((_size & index_mask) == 0) {
            if (!_tail.has_value()) {
                _tail.emplace(_head.at(0));
            }
            _last_row = _tail->get_position();
            _tail->add(_head);
        }
    }

    struct frame_tx_t {
        typename encoder_t::tx_state inner;
        self_t& self;

        frame_tx_t(typename encoder_t::tx_state&& s, self_t& self)
          : inner(std::move(s))
          , self(self) {}

        void add(const std::array<value_t, buffer_depth>& row) {
            inner.add(row);
        }
        void commit() && noexcept { self._tail->tx_commit(std::move(inner)); }
    };

    // Transactional append operation
    //
    // The method returns an optional with the state of the
    // transaction. If the append operation doesn't require
    // memory allocation the optional is null. Otherwise it
    // will contain a transaction which need to be commited.
    // The actual append operation can allocate memory and
    // can throw but 'commit' method of the transaction
    // doesn't allocate and doesn't throw.
    std::optional<frame_tx_t> append_tx(value_t value) {
        std::optional<frame_tx_t> tx;
        auto ix = index_mask & _size++;
        _head.at(ix) = value;
        if ((_size & index_mask) == 0) {
            if (!_tail.has_value()) {
                _tail.emplace(_head.at(0));
            }
            _last_row = _tail->get_position();
            tx.emplace(_tail->tx_start(), *this);
            tx->add(_head);
        }
        return tx;
    }

    /// Get stream position object which can be used as a "hint" to speed up
    /// index lookup operations.
    std::optional<hint_t> get_current_stream_pos() const { return _last_row; }

    const_iterator at_index(size_t index) const {
        if (index >= _size) {
            return end();
        }
        auto it = begin();
        std::advance(it, index);
        return it;
    }

    /// Get element by index, use 'hint' to speedup the operation.
    ///
    /// The 'hint' has to correspond to any index lower or equal to
    /// 'index'. The 'hint' values has to be stored externally.
    const_iterator at_index(size_t index, const hint_t& hint) const {
        if (index >= _size) {
            return end();
        }
        decoder_t decoder(
          _tail->get_initial_value(),
          _tail->get_row_count(),
          _tail->share(),
          delta_alg_instance);
        decoder.skip(hint);
        auto curr_ix = hint.num_rows * details::FOR_buffer_depth;
        auto it = const_iterator(
          std::move(decoder),
          _head,
          _size,
          curr_ix,
          _tail->get_initial_value());
        std::advance(it, index - curr_ix);
        return it;
    }

    bool is_applicable(const hint_t& hint) const {
        if (!_tail) {
            return false;
        }

        if constexpr (std::
                        is_same_v<delta_alg, details::delta_delta<value_t>>) {
            // check possible only with a monotonic frame
            if (!(_tail->get_initial_value() <= hint.initial
                  && hint.initial <= _tail->get_last_value())) {
                // ensure the value is in range
                return false;
            }
        }

        if (_tail->get_position().offset < hint.offset) {
            // ensure enough data in the buffer to apply the hint
            return false;
        }

        // has an encoded buffer, (if monotonic) data is in range, there are
        // enough bytes in the encoded buffer
        return true;
    }

    size_t size() const { return _size; }

    size_t mem_use() const {
        if (!_tail.has_value()) {
            return sizeof(*this);
        }
        return sizeof(*this) + _tail.value().mem_use();
    }

    /// Return first element of the frame or nullopt if frame is empty
    auto get_initial_value() const noexcept {
        if (_tail.has_value()) {
            return std::make_optional(_tail->get_initial_value());
        }
        return std::nullopt;
    }

    const_iterator begin() const {
        if (_tail.has_value()) {
            decoder_t decoder(
              _tail->get_initial_value(),
              _tail->get_row_count(),
              _tail->share(),
              delta_alg_instance);
            return const_iterator(
              std::move(decoder), _head, _size, 0, _tail->get_initial_value());
        } else if (_size != 0) {
            // special case, data is only stored in the buffer
            // not in the compressed column
            decoder_t decoder(0, 0, iobuf(), delta_alg_instance);
            return const_iterator(std::move(decoder), _head, _size);
        }
        return end();
    }

    const_iterator end() const { return const_iterator(); }

    const_iterator find(value_t value) const {
        return pred_search<std::equal_to<value_t>>(value);
    }

    const_iterator upper_bound(value_t value) const {
        return pred_search<std::greater<value_t>>(value);
    }

    const_iterator lower_bound(value_t value) const {
        return pred_search<std::greater_equal<value_t>>(value);
    }

    std::optional<value_t> last_value() const {
        if (_size == 0) {
            return std::nullopt;
        }
        auto ix = (_size - 1) & index_mask;
        return _head.at(ix);
    }

    bool contains(value_t value) const { return find(value) != end(); }

    /// Prefix truncate the frame. Index ix_exclusive is a new
    /// start of the frame.
    void prefix_truncate_ix(uint32_t ix_exclusive) {
        self_t tmp{};
        for (auto it = at_index(ix_exclusive); it != end(); ++it) {
            tmp.append(*it);
        }
        _head = tmp._head;
        _tail = std::move(tmp._tail);
        _size = tmp._size;
        _last_row = tmp._last_row;
    }

    auto serde_fields() { return std::tie(_head, _tail, _size, _last_row); }

    /// Returns a frame that shares the underlying iobuf
    /// The method itself is not unsafe, but the returned object can modify the
    /// original object, so users need to think about the use case
    auto unsafe_alias() const -> self_t {
        auto tmp = self_t{};
        tmp._head = _head;
        if (_tail.has_value()) {
            tmp._tail = _tail->unsafe_alias();
        }
        tmp._size = _size;
        tmp._last_row = _last_row;
        return tmp;
    }

private:
    template<class PredT>
    const_iterator pred_search(value_t value) const {
        PredT pred;
        for (auto it = begin(); it != end(); ++it) {
            if (pred(*it, value)) {
                return it;
            }
        }
        return end();
    }

    std::array<value_t, buffer_depth> _head{};
    std::optional<encoder_t> _tail{std::nullopt};
    size_t _size{0};
    std::optional<hint_t> _last_row{std::nullopt};
};

/// Column iterator
///
/// The core idea here is that the iterator stores an immutable
/// snapshot of the data so it's safe to use it in the asynchronous
/// and concurrent environment. The list of iterators to all frames
/// is stored in the vector. The iterators are referencing the immutable
/// copy of the column (the underlying iobuf is shared, the write buffer
/// is copied).
template<class value_t, auto delta_alg>
class deltafor_column_const_iterator
  : public boost::iterator_facade<
      deltafor_column_const_iterator<value_t, delta_alg>,
      const value_t,
      boost::iterators::forward_traversal_tag> {
    friend class boost::iterator_core_access;

public:
    using frame_t = deltafor_frame<value_t, delta_alg>;
    using frame_iter_t = typename frame_t::const_iterator;
    using hint_t = typename frame_t::hint_t;

private:
    using outer_iter_t = typename std::list<frame_iter_t>::iterator;
    using iter_list_t = std::list<frame_iter_t>;
    using self_t = deltafor_column_const_iterator<value_t, delta_alg>;

    template<class container_t>
    static iter_list_t make_snapshot(const container_t& src) {
        iter_list_t snap;
        for (const auto& f : src) {
            auto i = f.begin();
            snap.push_back(std::move(i));
        }
        return snap;
    }

    template<class iter_t>
    static iter_list_t make_snapshot_at(iter_t begin, iter_t end, size_t ix) {
        iter_list_t snap;
        auto intr = begin->at_index(ix);
        snap.push_back(std::move(intr));
        std::advance(begin, 1);
        for (auto it = begin; it != end; ++it) {
            auto i = it->begin();
            snap.push_back(std::move(i));
        }
        return snap;
    }

    template<class iter_t>
    static iter_list_t
    make_snapshot_at(iter_t begin, iter_t end, size_t ix, const hint_t& row) {
        iter_list_t snap;
        auto intr = begin->at_index(ix, row);
        snap.push_back(std::move(intr));
        std::advance(begin, 1);
        for (auto it = begin; it != end; ++it) {
            auto i = it->begin();
            snap.push_back(std::move(i));
        }
        return snap;
    }

    const value_t& dereference() const {
        vassert(!is_end(), "Can't dereference iterator");
        return *_inner_it;
    }

    void increment() {
#ifndef NDEBUG
        vassert(!is_end(), "can't increment iterator");
#endif
        ++_ix_column;
        ++_inner_it;
        if (_inner_it == _inner_end) {
            ++_outer_it;
            _inner_it = _outer_it == _snapshot.end() ? frame_iter_t()
                                                     : std::move(*_outer_it);
        }
    }

    bool equal(const auto& other) const {
        ssize_t idx = is_end() ? -1 : index();
        ssize_t oth_idx = other.is_end() ? -1 : other.index();
        return idx == oth_idx;
    }

public:
    /// Create iterator that points to the beginning of the column
    template<class container_t>
    explicit deltafor_column_const_iterator(const container_t& src)
      : _snapshot(make_snapshot(src))
      , _outer_it(_snapshot.begin())
      , _inner_it(
          _snapshot.empty() ? frame_iter_t{} : std::move(_snapshot.front())) {}

    /// Create iterator that points to the middle of the column
    template<class iterator_t>
    explicit deltafor_column_const_iterator(
      iterator_t begin,
      iterator_t end,
      uint32_t intra_frame_ix,
      uint32_t column_ix)
      : _snapshot(make_snapshot_at(begin, end, intra_frame_ix))
      , _outer_it(_snapshot.begin())
      , _inner_it(
          _snapshot.empty() ? frame_iter_t{} : std::move(_snapshot.front()))
      , _ix_column(column_ix) {}

    template<class iterator_t>
    explicit deltafor_column_const_iterator(
      iterator_t begin,
      iterator_t end,
      uint32_t intra_frame_ix,
      const hint_t& row,
      uint32_t column_ix)
      : _snapshot(make_snapshot_at(begin, end, intra_frame_ix, row))
      , _outer_it(_snapshot.begin())
      , _inner_it(
          _snapshot.empty() ? frame_iter_t{} : std::move(_snapshot.front()))
      , _ix_column(column_ix) {}

    /// Create iterator that points to the end of any column
    deltafor_column_const_iterator()
      : _outer_it(_snapshot.end()) {}

    // Current index
    size_t index() const { return _ix_column; }

    bool is_end() const {
        // Invariant: _inner_it is never equal to _inner_end
        // unless the iterator points to the end.
        return _inner_it == _inner_end;
    }

    auto get_frame_initial_value() const noexcept {
        return _outer_it->get_frame_initial_value();
    }

private:
    iter_list_t _snapshot;
    outer_iter_t _outer_it;
    frame_iter_t _inner_it{};
    // We don't need to store 'end' iterator for
    // every 'begin' iterator we store in the '_snapshot'
    // because all 'end' iterators are equal.
    frame_iter_t _inner_end{};
    // Current position inside the column
    size_t _ix_column{0};
};

/// Column that represents a single field
///
/// There are two specializations of this template. One for delta_xor
/// algorithm and another one for delta_delta. The latter one is guaranteed
/// to be used with monotonic sequences which makes some search
/// optimizations possible.
template<class value_t, class delta_t, size_t max_frame_size>
class deltafor_column;

namespace cloud_storage {
class column_store;
}

/// Base class for deltafor_column specializations
///
/// We have two specializations of the deltafor_column. One
/// for delta-xor alg. and another one for delta-delta. They're
/// different only by pred_serach implementation.
template<class value_t, auto delta_alg, class Derived, size_t max_frame_size>
class deltafor_column_impl
  : public serde::envelope<
      deltafor_column_impl<value_t, delta_alg, Derived, max_frame_size>,
      serde::version<0>,
      serde::compat_version<0>> {
    using delta_t = std::remove_cvref_t<decltype(delta_alg)>;
    using frame_t = deltafor_frame<value_t, delta_alg>;
    using decoder_t = deltafor_decoder<value_t, delta_t>;

    using self_t
      = deltafor_column_impl<value_t, delta_alg, Derived, max_frame_size>;

    // this friendship is used to access frame_t and _frames
    friend class cloud_storage::column_store;

    // constructor that will share all the frames from the source range
    deltafor_column_impl(
      share_frame_t, auto&& frame_iterator, auto&& frame_iterator_end) {
        for (; frame_iterator != frame_iterator_end; ++frame_iterator) {
            // this target the constructor that will share the underlying buffer
            _frames.emplace_back(share_frame, *frame_iterator);
        }
    }

    // crtp helper
    auto to_underlying() const -> decltype(auto) {
        return *static_cast<const Derived*>(this);
    }

public:
    /// Position in the column, can be used by
    /// index lookup operations
    using hint_t = typename frame_t::hint_t;

    using const_iterator = deltafor_column_const_iterator<value_t, delta_alg>;

    deltafor_column_impl() = default;

    deltafor_column_impl(deltafor_column_impl&&) noexcept = default;
    deltafor_column_impl& operator=(deltafor_column_impl&&) noexcept = default;

    void append(value_t value) {
        if (_frames.empty() || _frames.back().size() == max_frame_size) {
            _frames.push_back({});
        }
        _frames.back().append(value);
    }

    /// Return frame that contains value with index
    std::reference_wrapper<const frame_t>
    get_frame_by_element_index(size_t ix) {
        for (auto it = _frames.begin(); it != _frames.end(); ++it) {
            if (it->size() <= ix) {
                ix -= it->size();
            } else {
                return *it;
            }
        }
        vassert(false, "Invalid index {}", ix);
    }

    /// Get stream position object which can be used as a "hint" to speed up
    /// index lookup operations.
    std::optional<hint_t> get_current_stream_pos() const {
        if (_frames.empty()) {
            return std::nullopt;
        }
        return _frames.back().get_current_stream_pos();
    }

    struct column_tx_t {
        using frame_tx_t = typename frame_t::frame_tx_t;
        using frame_list_t = std::list<frame_t>;
        std::variant<frame_tx_t, frame_list_t> inner;
        self_t& self;

        column_tx_t(
          frame_tx_t inner,
          deltafor_column_impl<value_t, delta_alg, Derived, max_frame_size>&
            self)
          : inner(std::move(inner))
          , self(self) {}

        column_tx_t(
          frame_t frame,
          deltafor_column_impl<value_t, delta_alg, Derived, max_frame_size>&
            self)
          : inner(std::list<frame_t>())
          , self(self) {
            std::get<frame_list_t>(inner).push_back(std::move(frame));
        }

        void commit() && noexcept {
            if (std::holds_alternative<frame_tx_t>(inner)) {
                std::move(std::get<frame_tx_t>(inner)).commit();
            } else {
                self._frames.splice(
                  self._frames.end(), std::get<frame_list_t>(inner));
            }
        }
    };

    // Transactional append operation
    //
    // The method returns an optional with the state of the
    // transaction. If the append operation doesn't require
    // memory allocation the optional is null. Otherwise it
    // will contain a transaction which need to be commited.
    // The actual append operation can allocate memory and
    // can throw but 'commit' method of the transaction
    // doesn't allocate and doesn't throw.
    //
    // Transaction can be aborted by destroying the tx object.
    // Only one transaction can be active at a time. The caller
    // of the method is supposed to check the returned value
    // and to call 'commit' if it's not 'nullopt'.
    std::optional<column_tx_t> append_tx(value_t value) {
        std::optional<column_tx_t> tx;
        if (_frames.empty() || _frames.back().size() == max_frame_size) {
            frame_t tmp{};
            tmp.append(value);
            tx.emplace(std::move(tmp), *this);
            return tx;
        }
        auto inner = _frames.back().append_tx(value);
        if (!inner.has_value()) {
            // transactional op is already committed
            return tx;
        }
        tx.emplace(std::move(*inner), *this);
        return tx;
    }

    const_iterator at_index(size_t index) const {
        auto inner = index;
        for (auto it = _frames.begin(); it != _frames.end(); ++it) {
            if (it->size() <= inner) {
                inner -= it->size();
            } else {
                return const_iterator(it, _frames.end(), inner, index);
            }
        }
        return end();
    }

    /// Get element by index, use 'hint' to speedup the operation.
    ///
    /// The 'hint' has to correspond to any index lower or equal to
    /// 'index'. The 'hint' values has to be stored externally.
    const_iterator at_index(size_t index, const hint_t& hint) const {
        auto inner = index;
        for (auto it = _frames.begin(); it != _frames.end(); ++it) {
            if (it->size() <= inner) {
                inner -= it->size();
            } else {
                return const_iterator(it, _frames.end(), inner, hint, index);
            }
        }
        return end();
    }

    size_t size() const {
        if (_frames.empty()) {
            return 0;
        }
        size_t total = 0;
        for (const auto& f : _frames) {
            total += f.size();
        }
        return total;
    }

    bool empty() const {
        // short circuit at the first non empty frame
        return std::ranges::all_of(
          _frames, [](auto& f) { return f.size() == 0; });
    }

    size_t mem_use() const {
        size_t total = 0;
        for (const auto& p : _frames) {
            total += p.mem_use();
        }
        return sizeof(*this) + total;
    }

    const_iterator begin() const { return const_iterator(_frames); }

    const_iterator end() const { return const_iterator(); }

    const_iterator find(value_t value) const {
        return to_underlying().pred_search(value, std::equal_to<>{});
    }

    const_iterator upper_bound(value_t value) const {
        return to_underlying().pred_search(value, std::greater<>{});
    }

    const_iterator lower_bound(value_t value) const {
        return to_underlying().pred_search(value, std::greater_equal<>{});
    }

    std::optional<value_t> last_value() const {
        if (size() == 0) {
            return std::nullopt;
        }
        return _frames.back().last_value();
    }

    bool contains(value_t value) const { return find(value) != end(); }

    /// Prefix truncate column. Value at the position 'ix_exclusive' will
    /// become a new start of the column.
    void prefix_truncate_ix(uint32_t ix_exclusive) {
        for (auto it = _frames.begin(); it != _frames.end(); it++) {
            if (it->size() <= ix_exclusive) {
                ix_exclusive -= it->size();
            } else {
                it->prefix_truncate_ix(ix_exclusive);
                _frames.erase(_frames.begin(), it);
                break;
            }
        }
    }

    void serde_write(iobuf& out) {
        // std::list is not part of the serde-enabled types, save is as
        // size,elements
        const auto frames_size = _frames.size();
        if (unlikely(
              frames_size > std::numeric_limits<serde::serde_size_t>::max())) {
            throw serde::serde_exception(fmt_with_ctx(
              ssx::sformat,
              "serde: {}::_frames size {} exceeds serde_size_t",
              serde::type_str<self_t>(),
              frames_size));
        }
        serde::write(out, static_cast<serde::serde_size_t>(frames_size));
        for (auto& e : _frames) {
            serde::write(out, std::move(e));
        }
    }

    void serde_read(iobuf_parser& in, const serde::header& h) {
        // std::list is not part of the serde-enabled types, retrieve size and
        // push_back the elements
        if (unlikely(in.bytes_left() < h._bytes_left_limit)) {
            throw serde::serde_exception(fmt_with_ctx(
              ssx::sformat,
              "field spill over in {}, field type {}: envelope_end={}, "
              "in.bytes_left()={}",
              serde::type_str<self_t>(),
              serde::type_str<std::remove_reference_t<decltype(_frames)>>(),
              h._bytes_left_limit,
              in.bytes_left()));
        }
        const auto frames_size = serde::read_nested<serde::serde_size_t>(
          in, h._bytes_left_limit);
        for (auto i = 0U; i < frames_size; ++i) {
            _frames.push_back(
              serde::read_nested<frame_t>(in, h._bytes_left_limit));
        }
    }

    /// Returns a column that shares the underlying iobuf
    /// The method itself is not unsafe, but the returned object can modify the
    /// original object, so users need to think about the use case
    auto unsafe_alias() const -> Derived {
        auto tmp = Derived{};
        for (auto& e : _frames) {
            tmp._frames.push_back(e.unsafe_alias());
        }
        return tmp;
    }

protected:
    auto get_frame_iterator_by_element_index(size_t ix) const {
        return std::find_if(
          _frames.begin(), _frames.end(), [ix](const frame_t& f) mutable {
              if (f.size() > ix) {
                  return true;
              }
              ix -= f.size();
              return false;
          });
    }

    std::list<frame_t> _frames;
};

/// DeltaFor encoded collection of values.
///
/// Contains a list of frames. The frames are not overlapping with each
/// other. The iterator is used to scan both all frames seamlessly. This
/// specialization is for xor-delta algorithm. It doesn't allow skipping
/// frames so all search operations require full scan. Random access by
/// index can skip frames since indexes are monotonic.
template<class value_t, size_t max_frame_size>
class deltafor_column<value_t, details::delta_xor, max_frame_size>
  : public deltafor_column_impl<
      value_t,
      details::delta_xor{},
      deltafor_column<value_t, details::delta_xor, max_frame_size>,
      max_frame_size> {
    using base_t = deltafor_column_impl<
      value_t,
      details::delta_xor{},
      deltafor_column<value_t, details::delta_xor, max_frame_size>,
      max_frame_size>;

public:
    using delta_alg = details::delta_xor;
    using base_t::base_t;
    using typename base_t::const_iterator;

    /// Find first value that matches the predicate
    const_iterator pred_search(
      value_t value, std::regular_invocable<value_t, value_t> auto pred) const {
        for (auto it = this->begin(); it != this->end(); ++it) {
            if (pred(*it, value)) {
                return it;
            }
        }
        return this->end();
    }
};

/// Column that can store only monotonic integer sequences.
///
/// Optimized for quick append/at/find operations.
/// Find/lower_bound/upper_bound operations are complited within
/// single digit microsecond intervals even with millions of elements
/// in the column. The actual decoding is only performed for a single frame.
/// The access by index is also fast (same order of magnitued as search).
template<class value_t, size_t max_frame_size>
class deltafor_column<value_t, details::delta_delta<value_t>, max_frame_size>
  : public deltafor_column_impl<
      value_t,
      details::delta_delta<value_t>{},
      deltafor_column<value_t, details::delta_delta<value_t>, max_frame_size>,
      max_frame_size> {
    using base_t = deltafor_column_impl<
      value_t,
      details::delta_delta<value_t>{},
      deltafor_column<value_t, details::delta_delta<value_t>, max_frame_size>,
      max_frame_size>;

public:
    using delta_alg = details::delta_delta<value_t>;
    using base_t::base_t;
    using typename base_t::const_iterator;

    const_iterator pred_search(
      value_t value, std::regular_invocable<value_t, value_t> auto pred) const {
        auto it = this->_frames.begin();
        size_t index = 0;
        for (; it != this->_frames.end(); ++it) {
            // The code is only used with equal/greater/greater_equal
            // predicates to implement find/lower_bound/upper_bound. Because
            // of that we can hardcode the '>=' operation here.
            if (it->last_value().has_value() && *it->last_value() >= value) {
                break;
            }
            index += it->size();
        }
        if (it != this->_frames.end()) {
            auto start = const_iterator(it, this->_frames.end(), 0, index);
            for (; start != this->end(); ++start) {
                if (pred(*start, value)) {
                    return start;
                }
            }
        }
        return this->end();
    }
};
