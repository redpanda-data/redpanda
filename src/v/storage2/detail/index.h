#pragma once

#include "storage2/detail/functor_traits.h"

#include <seastar/util/tuple_utils.hh>

#include <tuple>
#include <type_traits>

namespace storage::detail {

namespace {

template<typename Key, typename Index>
using indexed_by = std::is_same<Key, typename Index::key_type>;

template<typename Key, typename Index>
constexpr bool indexed_by_v = indexed_by<Key, Index>::value;

template<typename T>
struct identity {
    using type = T;
};

template<typename Key>
constexpr auto index_by_key_impl() {
    return identity<std::false_type>();
}

template<typename Key, typename Current, typename... Rest>
constexpr auto index_by_key_impl() {
    if constexpr (indexed_by_v<Key, Current>) {
        return identity<Current>();
    } else {
        return index_by_key_impl<Key, Rest...>();
    }
}

template<typename Key, typename... Indexers>
struct is_indexable_impl : public std::false_type {};

template<typename Key, typename... Indexers>
struct is_indexable_impl<Key, std::tuple<Indexers...>> {
    static constexpr bool value
      = std::disjunction_v<indexed_by<Key, Indexers>...>;
};
} // namespace

/**
 * Given a tuple of indexers, this type will return a reference
 * to the first element that uses type of KeyType as the position type.
 */
template<typename KeyType, typename... Indices>
auto& index_by_key(std::tuple<Indices...>& indices) {
    using result = decltype(detail::index_by_key_impl<KeyType, Indices...>());
    return std::get<typename result::type>(indices);
}

template<typename KeyType, typename... Indices>
auto const& index_by_key(std::tuple<Indices...> const& indices) {
    using result = decltype(detail::index_by_key_impl<KeyType, Indices...>());
    return std::get<std::remove_const_t<typename result::type>>(indices);
}

/**
 * Checks if any of the tuple member is
 * a log segment index with position type Key.
 */
template<typename Key, typename... Indices>
constexpr bool has_index_key(const std::tuple<Indices...>&) {
    static_assert(sizeof...(Indices) != 0);
    return is_indexable_impl<Key, std::tuple<Indices...>>::value;
}

template<typename Key, typename Indices>
using is_indexable = std::enable_if_t<detail::is_indexable_impl<
  std::decay_t<Key>,
  std::decay_t<typename Indices::indices_type>>::value>;

/**
 * The number of active indices is known at compile time, this metafunction
 * will create an array of type "Of" and for each index in "Indices".
 */
template<typename Indices, typename Of>
using as_array_of = std::array<Of, Indices::count()>;

/**
 * Same as seastar::tuple_map, except here the tuple reference
 * doesn't have to be const. Todo: implement a simplified version
 * here. Relying on seastar internal helpers is not cool.
 */
template<typename Function, typename... Elements>
auto tuple_map(std::tuple<Elements...>& t, Function&& f) {
    return seastar::internal::tuple_map_helper(
      t, std::forward<Function>(f), std::index_sequence_for<Elements...>());
}

} // namespace storage::detail
