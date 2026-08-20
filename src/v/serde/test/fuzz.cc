#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "reflection/type_traits.h"
#include "serde/envelope.h"
#include "serde/read_header.h"
#include "serde/rw/envelope.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/optional.h"
#include "serde/rw/rw.h"
#include "serde/rw/scalar.h"
#include "serde/rw/sstring.h"
#include "serde/rw/vector.h"
#include "serde/serde_exception.h"
#include "serde/serde_size_t.h"
#include "serde/test/generated_structs.h"
#include "serde/type_str.h"

#if defined(MAIN)
#include <fstream>
#endif
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

constexpr const auto max_depth = 2;
constexpr const auto max_vector_size = 6;
constexpr const auto max_str_size = 25;

template<typename... T1, typename... T2, std::size_t... I>
bool eq(
  const std::tuple<T1...>& a,
  const std::tuple<T2...>& b,
  std::index_sequence<I...>) {
    return ((std::get<I>(a) == std::get<I>(b)) && ...);
}

template<serde::is_envelope E>
constexpr size_t arity
  = std::tuple_size_v<decltype(envelope_to_tuple(std::declval<E>()))>;

template<serde::is_envelope T1, serde::is_envelope T2>
bool operator==(T1 const& a, T2 const& b) {
    return eq(
      envelope_to_tuple(a),
      envelope_to_tuple(b),
      std::make_index_sequence<std::min(arity<T1>, arity<T2>)>());
}

struct data_gen {
    data_gen(const std::uint8_t* data, const std::size_t size)
      : _data{data}
      , _size{size} {}

    template<
      typename T,
      std::enable_if_t<std::is_trivially_copyable_v<T>>* = nullptr>
    T get() {
        auto val = T{};
        for (auto i = 0U; i != sizeof(val); ++i) {
            const auto byte = get_byte();
            std::memcpy(reinterpret_cast<std::uint8_t*>(&val) + i, &byte, 1);
        }
        return val;
    }

    std::uint8_t get_byte() {
        const auto d = _data[_i];
        ++_i;
        if (_i == _size) {
            _i = 0U;
        }
        return d;
    }

    const std::uint8_t* _data{};
    std::size_t _size{};
    std::size_t _i{};
};

template<typename T, std::size_t... Generation>
void init(
  T& t,
  data_gen& gen,
  std::index_sequence<Generation...> generations,
  int depth = 0) {
    if constexpr (serde::is_envelope<T>) {
        ((std::apply(
           [&](auto&&... args) {
               (init(args, gen, generations, depth + 1), ...);
           },
           t.template get_generation<Generation>())),
         ...);
    } else if constexpr (reflection::is_std_optional<T>) {
        if (
          depth != max_depth
          && gen.get<std::uint8_t>()
               > std::numeric_limits<std::uint8_t>::max() / 2) {
            t = std::make_optional<typename std::decay_t<T>::value_type>();
            init(*t, gen, generations, depth + 1);
        } else {
            t = std::nullopt;
        }
    } else if constexpr (reflection::is_std_vector<T>) {
        if (depth != max_depth) {
            t.resize(gen.get<uint8_t>() % max_vector_size);
            for (auto& v : t) {
                init(v, gen, generations, depth + 1);
            }
        }
    } else if constexpr (std::is_same_v<ss::sstring, std::decay_t<T>>) {
        t.resize(gen.get<uint8_t>() % max_str_size);
        for (auto& v : t) {
            v = (gen.get<char>() & std::numeric_limits<char>::max());
        }
    } else if constexpr (std::is_same_v<iobuf, std::decay_t<T>>) {
        auto s = ss::sstring{};
        init(s, gen, generations, depth + 1);
        t.append(std::move(s).release());
    } else {
        t = gen.get<T>();
    }
}

template<typename... T, std::size_t... I, std::size_t... Generation>
std::tuple<T...> init(
  data_gen gen,
  std::index_sequence<I...>,
  std::index_sequence<Generation...> generations) {
    auto structs = std::tuple<T...>{};
    (init(std::get<I>(structs), gen, generations), ...);
    return structs;
}

template<typename T>
void serialize(iobuf& iob, T&& t) {
    iob = serde::to_iobuf(std::forward<T>(t));
}

template<typename... T, std::size_t... I>
std::array<iobuf, sizeof...(T)>
serialize(std::tuple<T...>&& structs, std::index_sequence<I...>) {
    auto target = std::array<iobuf, sizeof...(T)>{};
    (serialize(target[I], std::move(std::get<I>(structs))), ...);
    return target;
}

template<typename T>
bool test(const T& orig, iobuf&& serialized) {
    return serde::from_iobuf<T>(std::move(serialized)) == orig;
}

template<typename... T, std::size_t... I>
bool test(
  const std::tuple<T...>& original,
  std::array<iobuf, sizeof...(T)>&& serialized,
  std::index_sequence<I...>) {
    return (test(std::get<I>(original), std::move(serialized[I])) && ...);
}

template<typename... T, std::size_t... Generation>
bool test_success(
  type_list<T...>,
  data_gen gen,
  std::index_sequence<Generation...> generations) {
    constexpr const auto idx_seq = std::index_sequence_for<T...>();
    return test(
      init<T...>(gen, idx_seq, generations),
      serialize(init<T...>(gen, idx_seq, generations), idx_seq),
      idx_seq);
}

template<typename... T1, typename... T2, std::size_t... Generation>
bool test_failure(
  type_list<T1...>,
  type_list<T2...>,
  data_gen gen,
  std::index_sequence<Generation...> generations) {
    constexpr const auto idx_seq = std::index_sequence_for<T1...>();
    return test(
      init<T1...>(gen, idx_seq, generations),
      serialize(init<T2...>(gen, idx_seq, generations), idx_seq),
      idx_seq);
}

template<typename T, std::size_t... Generations>
bool eq_generations(T&& a, T&& b, std::index_sequence<Generations...>) {
    return (
      (a.template get_generation<Generations>()
       == b.template get_generation<Generations>())
      && ...);
}

template<typename... T, std::size_t... I, std::size_t... Generations>
bool test_generations(
  std::tuple<T...>&& original,
  std::array<iobuf, sizeof...(T)>&& serialized,
  std::index_sequence<I...>,
  std::index_sequence<Generations...> generations) {
    return (
      eq_generations(
        std::move(std::get<I>(original)),
        serde::from_iobuf<T>(std::move(serialized[I])),
        generations)
      && ...);
}

template<typename... T1, typename... T2, std::size_t... Generations>
bool test_version_upgrade(
  type_list<T1...>,
  type_list<T2...>,
  data_gen gen,
  std::index_sequence<Generations...> generations) {
    constexpr const auto idx_seq = std::index_sequence_for<T1...>();
    return test_generations(
      init<T1...>(gen, idx_seq, generations),
      serialize(init<T2...>(gen, idx_seq, generations), idx_seq),
      idx_seq,
      generations);
}

// ---------------------------------------------------------------------------
// Decode-side fuzzing.
//
// Everything above is a round trip: the decoder only ever sees the output of
// its own writer, so every size prefix agrees with the bytes that follow it and
// no malformed encoding is ever tried. The generated structs are also all
// serde_fields() envelopes, and serde/rw/envelope.h forwards the *incoming*
// bytes_left_limit to generated fields, so the limit stays at the 0 that
// read<T>() seeds and the bounds guards are never asked about a real scope end.
//
// This half serializes a valid value, corrupts the bytes and decodes the
// result. The types below are hand-written for two reasons: their readers
// forward their own h._bytes_left_limit the way ~40 production call sites do
// (security::acl.cc, raft/types.cc, cluster/controller_snapshot.cc, ...), and
// they check the scope invariant around every field read.
// ---------------------------------------------------------------------------

// The invariant a nested read is entitled to assume is
// bytes_left() >= bytes_left_limit: the parser has not yet passed the end of
// the scope it is reading in. A preceding variable-length field can break it,
// because sstring.h and iobuf.h consume their whole declared length in one step
// without consulting the limit, so a length prefix that is too large walks the
// parser past the scope end while leaving it inside the buffer. What must not
// happen is a read *completing* from there: those bytes belong to the enclosing
// scope, and a field decoded from them is silently wrong. The scalar guard is
// what has to reject it.
template<typename T>
void read_scoped(iobuf_parser& in, T& t, const std::size_t bytes_left_limit) {
    const auto before = in.bytes_left();
    serde::read_nested(in, t, bytes_left_limit);
    if (before < bytes_left_limit) {
        std::cout
          << "read of " << serde::type_str<T>() << " started "
          << bytes_left_limit - before
          << " bytes past the end of its serde scope (bytes_left=" << before
          << ", bytes_left_limit=" << bytes_left_limit
          << ") and completed: the field was decoded from the enclosing "
             "scope's bytes\n"
          << std::flush;
        __builtin_trap();
    }
}

// Offsets of the size prefixes of the encoding written most recently. mutate()
// aims at these: a prefix that lies is the only input that can push a decode
// past a scope end, since every other read is bounded by a scalar guard.
std::vector<std::size_t> size_prefix_offsets;

// Each variable-length field is followed by a one-byte scalar, which is the
// read that gets to ask the guard whether the scope still has room. This is the
// shape of the chain in security::acl_binding_filter::serde_read, where an
// optional<sstring> is followed by an optional whose first read is a bool.
struct scoped_leaf
  : serde::envelope<scoped_leaf, serde::version<0>, serde::compat_version<0>> {
    std::vector<std::int32_t> _vec;
    std::int8_t _a{};
    iobuf _buf;
    std::int8_t _b{};
    std::optional<ss::sstring> _opt;
    std::int8_t _c{};
    ss::sstring _str;
    std::int8_t _d{};

    template<std::size_t Generation>
    auto get_generation() {
        static_assert(Generation == 0);
        return std::tie(_vec, _a, _buf, _b, _opt, _c, _str, _d);
    }

    void serde_write(iobuf& out) const {
        size_prefix_offsets.push_back(out.size_bytes());
        serde::write(out, _vec);
        serde::write(out, _a);
        size_prefix_offsets.push_back(out.size_bytes());
        serde::write(out, _buf.copy());
        serde::write(out, _b);
        // Past the presence flag, where the string's own prefix begins when the
        // optional is engaged.
        size_prefix_offsets.push_back(out.size_bytes() + 1);
        serde::write(out, _opt);
        serde::write(out, _c);
        size_prefix_offsets.push_back(out.size_bytes());
        serde::write(out, _str);
        serde::write(out, _d);
    }

    void serde_read(iobuf_parser& in, const serde::header& h) {
        read_scoped(in, _vec, h._bytes_left_limit);
        read_scoped(in, _a, h._bytes_left_limit);
        read_scoped(in, _buf, h._bytes_left_limit);
        read_scoped(in, _b, h._bytes_left_limit);
        read_scoped(in, _opt, h._bytes_left_limit);
        read_scoped(in, _c, h._bytes_left_limit);
        read_scoped(in, _str, h._bytes_left_limit);
        read_scoped(in, _d, h._bytes_left_limit);
    }
};

// _leaf is followed by more fields, so its envelope does not end at the end of
// the buffer and its reader sees a nonzero bytes_left_limit. These fields are
// also the bytes an overrunning field inside _leaf steals.
struct scoped_outer
  : serde::envelope<scoped_outer, serde::version<0>, serde::compat_version<0>> {
    scoped_leaf _leaf;
    std::int64_t _tail_n{};
    ss::sstring _tail_str;
    std::vector<std::int64_t> _tail_vec;

    template<std::size_t Generation>
    auto get_generation() {
        static_assert(Generation == 0);
        return std::tie(_leaf, _tail_n, _tail_str, _tail_vec);
    }

    auto serde_fields() {
        return std::tie(_leaf, _tail_n, _tail_str, _tail_vec);
    }
};

std::string flatten(const iobuf& b) {
    auto s = std::string(b.size_bytes(), '\0');
    auto in = iobuf_const_parser{b};
    in.consume_to(s.size(), s.data());
    return s;
}

iobuf unflatten(const std::string& s) {
    auto b = iobuf{};
    b.append(s.data(), s.size());
    return b;
}

// serde writes its size prefixes little-endian.
serde::serde_size_t load_le(const std::string& s, std::size_t pos) {
    auto v = serde::serde_size_t{};
    for (auto i = 0U; i != sizeof(v); ++i) {
        v |= static_cast<serde::serde_size_t>(
               static_cast<unsigned char>(s[pos + i]))
             << (8U * i);
    }
    return v;
}

void store_le(std::string& s, std::size_t pos, serde::serde_size_t v) {
    for (auto i = 0U; i != sizeof(v); ++i) {
        s[pos + i] = static_cast<char>((v >> (8U * i)) & 0xffU);
    }
}

// Corrupts an encoding the way a bit flip on disk, a truncated write or a
// version skew does. Growing a size prefix by a little is the mutation that
// matters most: the overrun has to clear the end of the enclosing scope but
// stay inside the buffer, so a wild value is much less interesting than a
// slightly wrong one.
void mutate(std::string& s, data_gen& gen) {
    const auto rounds = 1U + gen.get<std::uint8_t>() % 3U;
    for (auto r = 0U; r != rounds; ++r) {
        if (s.empty()) {
            return;
        }
        const auto op = gen.get<std::uint8_t>() % 5U;
        auto pos = std::size_t{gen.get<std::uint32_t>() % s.size()};
        if (op <= 1U && !size_prefix_offsets.empty()) {
            pos = size_prefix_offsets
              [gen.get<std::uint32_t>() % size_prefix_offsets.size()];
        }
        switch (op) {
        case 0:
        case 1:
            if (pos + sizeof(serde::serde_size_t) <= s.size()) {
                store_le(
                  s, pos, load_le(s, pos) + 1U + gen.get<std::uint8_t>() % 32U);
            }
            break;
        case 2:
            s[pos] = static_cast<char>(gen.get<std::uint8_t>());
            break;
        case 3:
            s.resize(pos);
            break;
        case 4:
            s.push_back(static_cast<char>(gen.get<std::uint8_t>()));
            break;
        }
    }
}

// Untrusted bytes may be rejected, but only in one of these ways. Anything else
// - a bad_variant_access, a logic_error, a seastar exception - is the decoder
// failing to handle input it is required to handle. Returning a value is fine;
// what it must not do is return one having read a field from outside its scope,
// which is what read_scoped() above traps on.
template<typename T>
void decode_untrusted(const std::string& encoded, const char* what) {
    try {
        auto decoded = serde::from_iobuf<T>(unflatten(encoded));
        asm volatile("" ::"r"(&decoded) : "memory");
    } catch (const serde::serde_exception&) {
        // The decoder rejected the input, which is the contract.
    } catch (const std::out_of_range&) {
        // A read ran past the end of the physical iobuf.
    } catch (const std::bad_alloc&) {
        // A garbage length reached an allocation.
    } catch (const std::length_error&) {
        // Ditto, for a length past a container's max_size().
    } catch (const std::exception& e) {
        std::cout << what << ": unexpected exception: " << e.what() << "\n"
                  << std::flush;
        __builtin_trap();
    }
}

// The untouched encoding has to decode cleanly and stay inside its scopes, or
// the corrupted run proves nothing: a writer and reader that disagree would
// look like a decoder that rejects everything.
template<typename T>
void decode_valid(const std::string& encoded) {
    try {
        auto decoded = serde::from_iobuf<T>(unflatten(encoded));
        asm volatile("" ::"r"(&decoded) : "memory");
    } catch (const std::exception& e) {
        std::cout << "valid encoding rejected: " << e.what() << "\n"
                  << std::flush;
        __builtin_trap();
    }
}

template<typename T>
void fuzz_decode(data_gen gen) {
    auto orig = T{};
    init(orig, gen, std::make_index_sequence<1>());

    size_prefix_offsets.clear();
    const auto encoded = flatten(serde::to_iobuf(orig));

    decode_valid<T>(encoded);

    auto corrupted = encoded;
    mutate(corrupted, gen);
    decode_untrusted<T>(corrupted, "corrupted encoding");
}

template<typename... T>
void fuzz_decode_all(type_list<T...>, data_gen gen) {
    (fuzz_decode<T>(gen), ...);
}

// Decoding the fuzzer's bytes as they arrive, with no valid encoding underneath
// them. The mutation pass only ever explores payloads that are nearly
// well-formed, since it starts from something the writer produced; this one
// starts from nothing, so it reaches the shapes a mutation of a valid encoding
// will not reach - a version pair no writer emits, an envelope size that
// disagrees with the fields inside it, a nesting depth the writer never
// produces. libFuzzer's coverage feedback supplies the few header bytes needed
// to get past read_header, and from there every reader is looking at lengths it
// has no reason to trust.
template<typename... T>
void fuzz_decode_raw(type_list<T...>, const std::string& bytes) {
    (decode_untrusted<T>(bytes, "random bytes"), ...);
}

void fuzz_serde(const uint8_t* data, size_t size) {
    constexpr const auto gen1 = std::make_index_sequence<1>();

    try {
        test_success(types_21{}, {data, size}, gen1);
        test_success(types_31{}, {data, size}, gen1);
    } catch (const std::exception& e) {
        std::cout << e.what() << "\n";
        __builtin_trap();
    }

    auto failed = false;
    try {
        test_failure(types_21{}, types_31{}, {data, size}, gen1);
    } catch (...) {
        failed = true;
    }
    if (!failed) {
        __builtin_trap();
    }

    test_version_upgrade(types_21{}, types_22{}, {data, size}, gen1);
    test_version_upgrade(types_22{}, types_21{}, {data, size}, gen1);

    fuzz_decode<scoped_outer>({data, size});
    fuzz_decode_all(types_21{}, {data, size});

    const auto raw = std::string{reinterpret_cast<const char*>(data), size};
    fuzz_decode_raw(type_list<scoped_outer>{}, raw);
    fuzz_decode_raw(types_21{}, raw);
}

#if defined(MAIN)
int main(int argc, char** argv) {
    if (argc != 2) {
        std::cout << "usage: " << argv[0] << " INPUT\n";
        return 1;
    }

    auto in = std::ifstream{};
    in.exceptions(std::ios::failbit | std::ios::badbit);
    in.open(argv[1], std::ios_base::binary);
    auto str = std::string{};

    in.seekg(0, std::ios::end);
    str.reserve(in.tellg());
    in.seekg(0, std::ios::beg);

    str.assign(
      (std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    const auto data = reinterpret_cast<const std::uint8_t*>(str.data());
    const auto size = str.size();

    fuzz_serde(data, size);
};
#else
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0) {
        return 0;
    }

    fuzz_serde(data, size);

    return 0;
}
#endif
