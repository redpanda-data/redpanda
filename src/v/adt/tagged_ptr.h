#pragma once

// XXX that for ARM, we can only use 8bits instead of 16bits
// https://www.kernel.org/doc/Documentation/arm64/tagged-pointers.txt

#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>

#include <boost/cstdint.hpp>
#include <boost/predef.h>


#ifndef BOOST_ARCH_X86_64
#if !defined(__aarch64__)
#error tagged_ptr unsupported platform
#endif
#endif

template <class T>
class tagged_ptr {
 public:
  using reference = typename std::add_lvalue_reference<T>::type;

  constexpr static inline uintptr_t
  pack_ptr(T *p, uint16_t tag) {
    uintptr_t ip = reinterpret_cast<uintptr_t>(p);
    ip |= static_cast<uintptr_t>(tag) << 48;
    return ip;
  }

 public:
  explicit tagged_ptr() noexcept : ptr_(0) {}
  tagged_ptr(tagged_ptr const &o) : ptr_(o.ptr_) {}
  tagged_ptr(tagged_ptr &&o) noexcept : ptr_(std::move(o.ptr_)) {}
  explicit tagged_ptr(T *p, uint16_t t = 0) : ptr_(pack_ptr(p, t)) {}

  void
  set(T *p, uint16_t t) {
    ptr_ = pack_ptr(p, t);
  }

  void
  clear() {
    ptr_ = 0;
  }

  bool
  operator==(volatile tagged_ptr const &p) const {
    return (ptr_ == p.ptr_);
  }

  bool
  operator!=(volatile tagged_ptr const &p) const {
    return !operator==(p);
  }

  T *
  get_ptr() const {
    return reinterpret_cast<T *>(ptr_ & ((1ULL << 48) - 1));
  }

  void
  set_ptr(T *p) {
    uint16_t tag = get_tag();
    ptr_ = pack_ptr(p, tag);
  }

  uint16_t
  get_tag() const {
    return ptr_ >> 48;
  }

  void
  set_tag(uint16_t t) {
    T *p = get_ptr();
    ptr_ = pack_ptr(p, t);
  }

  // Cannot have a ref to void* - illegal.
  // return just void* in that case.
  template <typename U = T>
  typename std::enable_if<std::is_same<U, void>::value, void *>::type
  operator*() const {
    return get_ptr();
  }
  template <typename U = T>
  typename std::enable_if<!std::is_same<U, void>::value, U &>::type
  operator*() const {
    return *get_ptr();
  }

  T *operator->() const { return get_ptr(); }

  operator bool(void) const { return get_ptr() != 0; }

 protected:
  // on x86_64 the most significant 16 bits are clear
  // store pointer on the least 48 bits
  uintptr_t ptr_;
};

