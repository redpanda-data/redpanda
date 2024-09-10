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

#include <optional>

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
concept numeric = requires(const T& x, const T& y) {
    x - y;
    x + y;
    x / 2;
    { x < y } -> std::same_as<bool>;
    { x > y } -> std::same_as<bool>;
};

/**
 * Traits required for a type to be usable with `numeric_integral_bounds`
 */
template<typename T>
concept numeric_integral = requires(const T& x, const T& y) {
    requires numeric<T>;
    x % y;
};

/**
 * Concept that is true for stdlib containers which publish their
 * inner contained type as ::value_type
 */
template<typename T>
concept has_value_type = requires(T x) { typename T::value_type; };

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

/**
 * Traits for the bounds types
 */
template<typename T>
concept bounds = requires(T bounds, const typename T::underlying_t& value) {
    {
        bounds.min
    } -> std::convertible_to<std::optional<typename T::underlying_t>>;
    {
        bounds.max
    } -> std::convertible_to<std::optional<typename T::underlying_t>>;
    { bounds.validate(value) } -> std::same_as<std::optional<ss::sstring>>;
    { bounds.clamp(value) } -> std::same_as<typename T::underlying_t>;
};

} // namespace detail

/**
 * Define valid bounds for an integral numeric configuration property.
 */
template<typename T>
requires detail::numeric_integral<T>
struct numeric_integral_bounds {
    using underlying_t = T;
    std::optional<T> min = std::nullopt;
    std::optional<T> max = std::nullopt;
    std::optional<T> align = std::nullopt;
    std::optional<odd_even_constraint> oddeven = std::nullopt;

    T clamp(const T& original) const {
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

    std::optional<ss::sstring> validate(const T& value) const {
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

/**
 * Define valid bounds for any numeric configuration property.
 */
template<detail::numeric T>
struct numeric_bounds {
    using underlying_t = T;
    std::optional<T> min = std::nullopt;
    std::optional<T> max = std::nullopt;

    T clamp(T value) const {
        if (min.has_value()) {
            value = std::max(min.value(), value);
        }
        if (max.has_value()) {
            value = std::min(max.value(), value);
        }
        return value;
    }

    std::optional<ss::sstring> validate(const T& value) const {
        if (min.has_value() && value < min.value()) {
            return fmt::format("too small, must be at least {}", min.value());
        } else if (max.has_value() && value > max.value()) {
            return fmt::format("too large, must be at most {}", max.value());
        }
        return std::nullopt;
    }
};

/**
 * A property that validates its value against the constraints defined by \p B
 *
 * \tparam T Underlying property value type
 * \tparam B Bounds class, like \ref numeric_integral_bounds or
 *      \ref numeric_bounds, or user defined that satisfies \ref detail::bounds
 * \tparam I Always default
 */
template<
  typename T,
  template<typename> typename B = numeric_integral_bounds,
  typename I = typename detail::inner_type<T>::inner>
requires detail::bounds<B<I>>
class bounded_property : public property<T> {
public:
    bounded_property(
      config_store& conf,
      std::string_view name,
      std::string_view desc,
      base_property::metadata meta,
      T def,
      B<I> bounds,
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

    using property<T>::set_value;

    void set_value(std::any v) override {
        property<T>::update_value(std::any_cast<T>(std::move(v)));
    }

    bool set_value(YAML::Node n) override {
        auto val = std::move(n.as<T>());
        return clamp_and_update(val);
    }

    std::optional<std::string_view> example() const override {
        if (!_example.empty()) {
            return _example;
        } else {
            return property<T>::example();
        }
    }

private:
    bool clamp_and_update(T val) {
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
    }

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
            guess = _bounds.clamp(
              _bounds.min.value()
              + (_bounds.max.value() - _bounds.min.value()) / 2);
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

    B<I> _bounds;

    // The example value is stored rather than generated on the fly, to
    // satisfy the example() interface that wants a string_view: it
    // needs a lifetime as long as our own.
    ss::sstring _example;
};

} // namespace config
