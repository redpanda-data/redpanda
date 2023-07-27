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

#include <concepts>
#include <cstddef>
#include <cstdint>
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

    bool operator==(delta_delta const&) const = default;
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
  uint8_t const* input, unsigned_serializable auto& output) {
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
