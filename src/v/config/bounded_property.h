// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "config/base_property.h"
#include "config/property.h"

namespace config {

/**
 * Detail section for concepts used in public class definitions, avoid
 * polluating the overall config:: namespace with these.
 */
namespace detail {

/**
 * Traits required for a type to be usable with `numeric_bounds`
 */
template<typename T>
concept numeric = requires(const T& x) {
    {x % x};
    { x < x } -> std::same_as<bool>;
    { x > x } -> std::same_as<bool>;
};

/**
 * Concept that is true for stdlib containers which publish their
 * inner contained type as ::value_type
 */
template<typename T>
concept has_value_type = requires(T x) {
    typename T::value_type;
};

/**
 * inner_type is a struct whose ::inner member reflects
 * the value_type of T if T has such an attribute.  Otherwise
 * ::inner is equal to T.
 *
 * Useful when you expect either a std::optional<> or a bare value,
 * but always want to use the bare value type.
 */
template<typename T>
struct inner_type {
    using inner = T;
};

template<typename T>
requires has_value_type<T>
struct inner_type<T> {
    using inner = typename T::value_type;
};

} // namespace detail

/**
 * Define valid bounds for a numeric configuration property.
 */
template<typename T>
requires detail::numeric<T>
struct numeric_bounds {
    std::optional<T> min = std::nullopt;
    std::optional<T> max = std::nullopt;
    std::optional<T> align = std::nullopt;
    std::optional<odd_even_constraint> oddeven = std::nullopt;

    T clamp(T& original) {
        T result = original;

        if (align.has_value()) {
            auto remainder = result % align.value();
            result -= remainder;
        }

        if (min.has_value()) {
            result = std::max(min.value(), result);
        }

        if (max.has_value()) {
            result = std::min(max.value(), result);
        }

        return result;
    }

    std::optional<ss::sstring> validate(T& value) {
        if (min.has_value() && value < min.value()) {
            return fmt::format("too small, must be at least {}", min.value());
        } else if (max.has_value() && value > max.value()) {
            return fmt::format("too large, must be at most {}", max.value());
        } else if (align.has_value() && value % align.value() != T{0}) {
            return fmt::format(
              "not aligned, must be aligned to nearest {}", align.value());
        } else if (
          oddeven.has_value() && oddeven.value() == odd_even_constraint::odd
          && value % 2 == T{0}) {
            return fmt::format("value must be odd");
        } else if (
          oddeven.has_value() && oddeven.value() == odd_even_constraint::even
          && value % 2 == T{1}) {
            return fmt::format("value must be even");
        }
        return std::nullopt;
    }
};

template<typename T, typename I = typename detail::inner_type<T>::inner>
class bounded_property : public property<T> {
public:
    bounded_property(
      config_store& conf,
      std::string_view name,
      std::string_view desc,
      base_property::metadata meta,
      T def,
      numeric_bounds<I> bounds,
      std::optional<legacy_default<T>> legacy = std::nullopt)
      : property<T>(
        conf,
        name,
        desc,
        meta,
        def,
        [this](T new_value) -> std::optional<ss::sstring> {
            // Extract inner value if we are an optional<>,
            // and pass through into numeric_bounds::validate
            using outer_type = std::decay_t<T>;
            if constexpr (reflection::is_std_optional<outer_type>) {
                if (new_value.has_value()) {
                    return _bounds.validate(new_value.value());
                } else {
                    // nullopt is always valid
                    return std::nullopt;
                }
            } else {
                return _bounds.validate(new_value);
            }
        },
        legacy)
      , _bounds(bounds)
      , _example(generate_example()) {}

    bool set_value(YAML::Node n) override {
        auto val = std::move(n.as<T>());

        using outer_type = std::decay_t<T>;

        // If we somehow are applying an invalid value, clamp it
        // to the valid range.  This may happen if the value was
        // set in an earlier version of redpanda with looser bounds,
        // or if we are dealing with a value set directly in our store
        // rather than via admin API.

        // If T is a std::optional, then need to unpack the value.
        if constexpr (reflection::is_std_optional<outer_type>) {
            if (val.has_value()) {
                return property<T>::update_value(
                  std::move(_bounds.clamp(val.value())));
            } else {
                // nullopt is always valid, never clamped.  Pass it through.
                return property<T>::update_value(std::move(val));
            }
        } else {
            return property<T>::update_value(std::move(_bounds.clamp(val)));
        }
    };

    std::optional<std::string_view> example() const override {
        if (!_example.empty()) {
            return _example;
        } else {
            return property<T>::example();
        }
    }

private:
    /*
     * Pre-generate an example for docs/api, if the explicit property
     * metadata does not provide one.
     */
    ss::sstring generate_example() {
        auto parent = property<T>::example();
        if (parent.has_value()) {
            // Don't bother, our metadata already provides an example
            return "";
        }
        I guess;

        if (_bounds.min.has_value() && _bounds.max.has_value()) {
            // Take midpoint of min/max and align it.
            guess = _bounds.min.value()
                    + (_bounds.max.value() - _bounds.min.value()) / 2;
            if (_bounds.align.has_value()) {
                guess -= guess % _bounds.align.value();
            }
        } else {
            if constexpr (reflection::is_std_optional<T>) {
                if (property<T>::_default.has_value()) {
                    guess = property<T>::_default.value();
                } else {
                    // No non-nil default, do not try to give an example
                    return "";
                }

            } else {
                guess = property<T>::_default;
            }
        }
        return fmt::format("{}", guess);
    }

    numeric_bounds<I> _bounds;

    // The example value is stored rather than generated on the fly, to
    // satisfy the example() interface that wants a string_view: it
    // needs a lifetime as long as our own.
    ss::sstring _example;
};

} // namespace config
