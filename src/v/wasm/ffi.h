/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "reflection/type_traits.h"
#include "utils/hdr_hist.h"
#include "vassert.h"

#include <cstdint>
#include <span>
#include <string_view>
#include <type_traits>
#include <vector>

namespace wasm::ffi {

/**
 * An container for a sequence of T from the Wasm VM guest.
 *
 * This can be used in exposed functions, and the parameter translation will
 * convert this to two parameters on the guest side: a raw pointer and the size
 * of that pointer.
 *
 * We'll bounds check the entire array during the parameter translation
 * transparently.
 */
template<typename T>
class array {
public:
    using element_type = T;

    array()
      : _ptr(nullptr)
      , _size(0) {}
    array(T* ptr, uint32_t size)
      : _ptr(ptr)
      , _size(size) {}

    array(array<T>&&) noexcept = default;
    array& operator=(array<T>&&) noexcept = default;

    array(const array<T>&) noexcept = default;
    array& operator=(const array<T>&) noexcept = default;

    ~array() = default;

    explicit operator bool() const noexcept { return bool(_ptr); }

    T* raw() noexcept { return _ptr; }

    const T* raw() const noexcept { return _ptr; }

    T& operator[](uint32_t index) noexcept {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        return _ptr[index];
    }

    const T& operator[](uint32_t index) const noexcept {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        return _ptr[index];
    }

    uint32_t size() const noexcept { return _size; }

    ffi::array<T> slice(size_t offset, size_t length) {
        vassert(
          offset + length <= _size,
          "out of bounds slice for ffi::array offset={} length={} size={}",
          offset,
          length,
          _size);
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        return ffi::array(_ptr + offset, length);
    }

private:
    T* _ptr;
    uint32_t _size;
};

std::string_view array_as_string_view(array<uint8_t> arr);

/**
 * A helper class to compute the size of buffer that is needed for encoding.
 *
 * This can be used in conjunction with `writer` share code between computing
 * the size of needed for a buffer and actually writing to it.
 *
 * Example FFI call usage:
 *
 * template <typename T>
 * int32_t serialize_flubber(const flubber& f, T* out) {
 *   out->append(f.foo());
 *   out->append_with_length(f.bar());
 *   return out->total();
 * }
 *
 * int32_t get_flubber_len() {
 *   sizer s;
 *   return serialize_flubber(flubber(), &s);
 * }
 * int32_t get_flubber(ffi::array<uint8_t> buf) {
 *   writer w(buf);
 *   return serialize_flubber(flubber(), &w);
 * }
 *
 */
class sizer {
public:
    sizer() = default;
    sizer(const sizer&) = delete;
    sizer& operator=(const sizer&) = delete;
    sizer(sizer&&) = default;
    sizer& operator=(sizer&&) = default;
    ~sizer() = default;

    void append(std::string_view);
    void append(bytes_view);
    void append(const iobuf&);
    void append_with_length(std::string_view);
    void append_with_length(const iobuf&);
    void append(uint32_t);
    void append(int32_t);
    void append(uint64_t);
    void append(int64_t);

    size_t total() const noexcept { return _offset; };

private:
    size_t _offset{0};
};

/**
 * A helper class for writing data to a ffi::array<uint8_t> (aka guest buffer).
 *
 * See the sizer documentation for more information.
 */
class writer {
public:
    explicit writer(array<uint8_t>);
    writer(const writer&) = delete;
    writer& operator=(const writer&) = delete;
    writer(writer&&) = default;
    writer& operator=(writer&&) = default;
    ~writer() = default;

    void append(std::string_view);
    void append(bytes_view);
    void append(const iobuf&);
    void append_with_length(std::string_view);
    void append_with_length(const iobuf&);
    void append(uint32_t);
    void append(int32_t);
    void append(uint64_t);
    void append(int64_t);

    size_t total() const noexcept { return _offset; };

private:
    void ensure_size(size_t);
    array<uint8_t> slice_remainder();

    bytes _tmp;
    array<uint8_t> _output;
    size_t _offset{0};
};

/**
 * A helper class for reading data from a ffi::array<uint8_t> (aka guest
 * buffer).
 *
 * This can perform the mirror of operations that are supported in writer.
 */
class reader {
public:
    explicit reader(array<uint8_t>);
    reader(const reader&) = delete;
    reader& operator=(const reader&) = delete;
    reader(reader&&) = default;
    reader& operator=(reader&&) = default;
    ~reader() = default;

    ss::sstring read_string(size_t);
    iobuf read_iobuf(size_t);
    iobuf read_sized_iobuf();
    ss::sstring read_sized_string();
    int64_t read_varint();

private:
    array<uint8_t> slice_remainder();

    array<uint8_t> _input;
    size_t _offset{0};
};

/**
 * An abstraction for linear memory within a WASM guest.
 *
 * This is used to translate from guest memory into host memory.
 */
class memory {
public:
    memory() = default;
    virtual ~memory() = default;
    memory(const memory&) = delete;
    memory& operator=(const memory&) = delete;
    memory(memory&&) = default;
    memory& operator=(memory&&) = default;

    /* returns the host pointer for a given guest ptr and length. Throws if out
     * of bounds. */
    virtual void* translate_raw(size_t guest_ptr, size_t len) = 0;

    template<typename T>
    ffi::array<T> translate(size_t guest_ptr, size_t len) {
        void* ptr = translate_raw(guest_ptr, len * sizeof(T));
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        return ffi::array<T>(reinterpret_cast<T*>(ptr), len);
    }
};

/** The values we support passing via FFI right now. */
enum class val_type { i32, i64, f32, f64 };
std::ostream& operator<<(std::ostream& o, val_type vt);

namespace detail {
template<class T>
struct dependent_false : std::false_type {};

template<typename T>
struct is_array {
    static constexpr bool value = false;
};
template<template<typename...> class C, typename U>
struct is_array<C<U>> {
    static constexpr bool value = std::is_same<C<U>, array<U>>::value;
};

/**
 * Translate a single type into the types needed by the FFI boundary.
 */
template<typename Type>
void transform_type(std::vector<val_type>& types) {
    if constexpr (std::is_same_v<memory*, Type> || std::is_void_v<Type>) {
        // We don't pass memory type over the FFI boundary, but make the runtime
        // provide it, so we can just ignore it here (along with void for return
        // types).
    } else if constexpr (
      is_array<Type>::value || std::is_same_v<Type, ss::sstring>) {
        // Push back an arg for the pointer
        types.push_back(val_type::i32);
        // Push back an other arg for the length
        types.push_back(val_type::i32);
    } else if constexpr (
      std::is_same_v<Type, int64_t> || std::is_same_v<Type, uint64_t>) {
        types.push_back(val_type::i64);
    } else if constexpr (std::is_pointer_v<Type> || std::is_integral_v<Type>) {
        types.push_back(val_type::i32);
    } else if constexpr (reflection::is_rp_named_type<Type>) {
        transform_type<typename Type::type>(types);
    } else if constexpr (ss::is_future<Type>::value) {
        transform_type<typename Type::value_type>(types);
    } else {
        static_assert(dependent_false<Type>::value, "Unknown type");
    }
}

/**
 * This extracts raw FFI call parameters into our higher level types.
 */
template<typename Type>
std::tuple<Type> extract_parameter(
  ffi::memory* mem, std::span<const uint64_t> raw_params, unsigned& idx) {
    if constexpr (std::is_same_v<ffi::memory*, Type>) {
        return std::tuple(mem);
    } else if constexpr (detail::is_array<Type>::value) {
        auto guest_ptr = static_cast<uint32_t>(raw_params[idx++]);
        auto ptr_len = static_cast<uint32_t>(raw_params[idx++]);
        void* host_ptr = mem->translate_raw(
          guest_ptr, ptr_len * sizeof(typename Type::element_type));
        return std::make_tuple(ffi::array<typename Type::element_type>(
          // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
          reinterpret_cast<typename Type::element_type*>(host_ptr),
          ptr_len));
    } else if constexpr (std::is_same_v<ss::sstring, Type>) {
        auto guest_ptr = static_cast<uint32_t>(raw_params[idx++]);
        auto ptr_len = static_cast<uint32_t>(raw_params[idx++]);
        void* host_ptr = mem->translate_raw(guest_ptr, ptr_len);
        return std::make_tuple(ss::sstring(
          // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
          reinterpret_cast<char*>(host_ptr),
          ptr_len));
    } else if constexpr (
      std::is_same_v<Type, const void*> || std::is_same_v<Type, void*>) {
        ++idx;
        // TODO: Remove this temporary hack
        return std::make_tuple(static_cast<Type>(nullptr));
    } else if constexpr (std::is_pointer_v<Type>) {
        // Assume this is an out val
        auto guest_ptr = static_cast<uint32_t>(raw_params[idx++]);
        uint32_t ptr_len = sizeof(typename std::remove_pointer_t<Type>);
        void* host_ptr = mem->translate_raw(guest_ptr, ptr_len);
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        return std::make_tuple(reinterpret_cast<Type>(host_ptr));
    } else if constexpr (std::is_integral_v<Type>) {
        return std::make_tuple(static_cast<Type>(raw_params[idx++]));
    } else if constexpr (reflection::is_rp_named_type<Type>) {
        auto [underlying] = extract_parameter<typename Type::type>(
          mem, raw_params, idx);
        return std::tuple<Type>(underlying);
    } else {
        static_assert(dependent_false<Type>::value, "Unknown type");
    }
}

template<typename Type>
constexpr size_t num_parameters() {
    if constexpr (std::is_same_v<ffi::memory*, Type>) {
        return 0;
    } else if constexpr (
      is_array<Type>::value || std::is_same_v<ss::sstring, Type>) {
        // one for the pointer and one for the length
        return 2;
    } else if constexpr (std::is_pointer_v<Type> || std::is_integral_v<Type>) {
        return 1;
    } else if constexpr (reflection::is_rp_named_type<Type>) {
        return num_parameters<typename Type::type>();
    } else {
        static_assert(dependent_false<Type>::value, "Unknown type");
    }
}

template<typename... Args>
concept EmptyPack = sizeof...(Args) == 0;
} // namespace detail

template<typename... Rest>
void transform_types(std::vector<val_type>&)
requires detail::EmptyPack<Rest...>
{
    // Nothing to do
}

template<typename Type, typename... Rest>
void transform_types(std::vector<val_type>& types) {
    detail::transform_type<Type>(types);
    transform_types<Rest...>(types);
}

template<typename... Rest>
std::tuple<>
extract_parameters(ffi::memory*, std::span<const uint64_t>, unsigned)
requires detail::EmptyPack<Rest...>
{
    return std::make_tuple();
}

template<typename Type, typename... Rest>
std::tuple<Type, Rest...> extract_parameters(
  ffi::memory* mem, std::span<const uint64_t> params, unsigned idx) {
    auto head_type = detail::extract_parameter<Type>(mem, params, idx);
    return std::tuple_cat(
      std::move(head_type), extract_parameters<Rest...>(mem, params, idx));
}

template<typename... Rest>
constexpr size_t parameter_count()
requires detail::EmptyPack<Rest...>
{
    return 0;
}

template<typename Type, typename... Rest>
constexpr size_t parameter_count() {
    return detail::num_parameters<Type>() + parameter_count<Rest...>();
}
} // namespace wasm::ffi
