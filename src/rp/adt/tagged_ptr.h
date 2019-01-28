#pragma once

// taken from boost/lockfree/detail/tagged_ptr_ptrcompression.hpp
// with modifications for our compiler
// XXX that for ARM, we can only use 8bits instead of 16bits
// https://www.kernel.org/doc/Documentation/arm64/tagged-pointers.txt

#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>

#include <boost/cstdint.hpp>
#include <boost/predef.h>

namespace rp {

#ifndef BOOST_ARCH_X86_64
#if !defined(__aarch64__)
#error tagged_ptr unsupported platform
#endif
#endif

template <class T>
class tagged_ptr {
  using compressed_ptr_t = std::uint64_t;

 public:
  using tag_t = std::uint16_t;

 private:
  union cast_unit {
    compressed_ptr_t value;
    tag_t tag[4];
  };

  static constexpr const int kTagIndex = 3;
  static constexpr const compressed_ptr_t kPtrMask =
    0xffffffffffffUL;  //(1L<<48L)-1;

  constexpr static T *
  extract_ptr(volatile compressed_ptr_t const &i) {
    return (T *)(i & kPtrMask);
  }

  constexpr static tag_t
  extract_tag(volatile compressed_ptr_t const &i) {
    cast_unit cu = {i};
    return cu.tag[kTagIndex];
  }

  constexpr static compressed_ptr_t
  pack_ptr(T *ptr, tag_t tag) {
    cast_unit ret = {compressed_ptr_t(ptr)};
    ret.tag[kTagIndex] = tag;
    return ret.value;
  }

 public:
  /** uninitialized constructor */
  tagged_ptr() noexcept : ptr_(0) {}
  tagged_ptr(tagged_ptr const &p) = default;
  explicit tagged_ptr(T *p, tag_t t = 0) : ptr_(pack_ptr(p, t)) {}

  /** unsafe set operation */
  /* @{ */
  tagged_ptr &operator=(tagged_ptr const &p) = default;

  void
  set(T *p, tag_t t) {
    ptr_ = pack_ptr(p, t);
  }

  void
  clear() {
    ptr_ = 0;
  }
  /* @} */

  /** comparing semantics */
  /* @{ */
  bool
  operator==(volatile tagged_ptr const &p) const {
    return (ptr_ == p.ptr_);
  }

  bool
  operator!=(volatile tagged_ptr const &p) const {
    return !operator==(p);
  }
  /* @} */

  /** pointer access */
  /* @{ */
  T *
  get_ptr() const {
    return extract_ptr(ptr_);
  }

  void
  set_ptr(T *p) {
    tag_t tag = get_tag();
    ptr_ = pack_ptr(p, tag);
  }
  /* @} */

  /** tag access */
  /* @{ */
  tag_t
  get_tag() const {
    return extract_tag(ptr_);
  }

  void
  set_tag(tag_t t) {
    T *p = get_ptr();
    ptr_ = pack_ptr(p, t);
  }
  /* @} */

  /** smart pointer support  */
  /* @{ */

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
  /* @} */

 protected:
  compressed_ptr_t ptr_;
};

}  // namespace rp
