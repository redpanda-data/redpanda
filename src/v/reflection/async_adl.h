/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "reflection/adl.h"
#include "ssx/future-util.h"

#include <boost/range/irange.hpp>

#include <iterator>
#include <optional>

namespace reflection {

template<typename T>
struct async_adl {
    using type = std::remove_reference_t<std::decay_t<T>>;
    // provide extension point for rpc on the cases that we need to
    ss::future<> to(iobuf& out, type t) {
        reflection::adl<type>{}.to(out, std::move(t));
        return ss::make_ready_future<>();
    }
    ss::future<type> from(iobuf_parser& p) {
        return ss::make_ready_future<type>(reflection::adl<type>{}.from(p));
    }
};

namespace detail {

template<typename T>
concept Reservable = requires(T t, int32_t i) { t.reserve(i); };

template<typename List>
struct async_adl_list {
    using T = typename List::value_type;

    ss::future<> to(iobuf& out, List l) {
        reflection::serialize<int32_t>(out, l.size());
        return ss::do_with(std::move(l), [&out](List& e) {
            return ss::do_for_each(
              e, [&out](typename List::value_type& element) {
                  return async_adl<T>{}.to(out, std::move(element));
              });
        });
    }

    ss::future<List> from(iobuf_parser& in) {
        const auto size = adl<int32_t>{}.from(in);
        List list;
        if constexpr (Reservable<List>) {
            list.reserve(size);
        }
        auto range = boost::irange<int32_t>(0, size);
        return ss::do_with(
          std::move(list),
          range,
          [&in](List& list, const boost::integer_range<int32_t>& r) {
              return ss::do_for_each(
                       r,
                       [&list, &in](int32_t) {
                           return async_adl<T>{}.from(in).then(
                             [&list](T value) {
                                 std::back_inserter(list) = std::move(value);
                             });
                       })
                .then([&list] { return std::move(list); });
          });
    }
};

template<typename Map>
struct async_adl_map {
    using K = typename Map::key_type;
    using V = typename Map::mapped_type;

    /// Keys of the collection are copied rather then moved to their respective
    /// serializer.
    ss::future<> to(iobuf& out, Map map) {
        reflection::serialize(out, static_cast<int32_t>(map.size()));
        return ss::do_with(std::move(map), [&out](Map& map) {
            return ss::do_for_each(map, [&out](typename Map::value_type& p) {
                /// We must copy the key since map keys are all const
                auto copy = p.first;
                return reflection::async_adl<K>{}
                  .to(out, std::move(copy))
                  .then([&out, data = std::move(p.second)]() mutable {
                      return reflection::async_adl<V>{}.to(
                        out, std::move(data));
                  });
            });
        });
    }

    ss::future<Map> from(iobuf_parser& in) {
        int32_t size = reflection::adl<int32_t>{}.from(in);
        auto range = boost::irange<int32_t>(0, size);
        return ss::do_with(
          Map(),
          range,
          [&in](Map& result, const boost::integer_range<int32_t>& r) {
              return ss::do_for_each(
                       r,
                       [&in, &result](int32_t) {
                           return reflection::async_adl<K>{}.from(in).then(
                             [&in, &result](K key) mutable {
                                 return reflection::async_adl<V>{}
                                   .from(in)
                                   .then([key = std::move(key),
                                          &result](V value) mutable {
                                       auto [_, r] = result.emplace(
                                         std::move(key), std::move(value));
                                       vassert(r, "Item wasn't inserted");
                                   });
                             });
                       })
                .then([&result] { return std::move(result); });
          });
    }
};

} // namespace detail

/// Specializations of async_adl for nested types, optional and vector
template<typename T>
struct async_adl<std::optional<T>> {
    using value_type = std::remove_reference_t<std::decay_t<T>>;

    ss::future<> to(iobuf& out, std::optional<value_type> t) {
        if (t) {
            adl<int8_t>{}.to(out, 1);
            return async_adl<value_type>{}.to(out, std::move(t.value()));
        }
        adl<int8_t>{}.to(out, 0);
        return ss::now();
    }

    ss::future<std::optional<value_type>> from(iobuf_parser& in) {
        int8_t is_set = adl<int8_t>{}.from(in);
        if (is_set == 0) {
            return ss::make_ready_future<std::optional<value_type>>(
              std::nullopt);
        }
        return async_adl<value_type>{}.from(in).then([](value_type vt) {
            return ss::make_ready_future<std::optional<value_type>>(
              std::move(vt));
        });
    }
};

template<typename T>
ss::future<T> from_iobuf_async(iobuf b) {
    iobuf_parser parser(std::move(b));
    return ss::do_with(std::move(parser), [](iobuf_parser& parser) {
        return async_adl<std::decay_t<T>>{}.from(parser);
    });
}

} // namespace reflection
