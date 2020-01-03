#pragma once

#include <tuple>
#include <type_traits>

namespace storage::detail {

/**
 * This metafunction takes a functor (including lambdas) and extracts their
 * signature into a return value, types of accepted parameters and their
 * constness.
 *
 * Use cases:
 * It is used to validathe if the fold function Op is valid and
 * to extract the type of elements it consumes by extracting the second arg
 * type.
 */
template<typename Op>
struct functor_traits {
private:
    template<typename>
    struct functor_traits_impl;

    // specialization for mutable lambda and non-const operator()
    template<typename Obj, typename Ret, typename... Args>
    struct functor_traits_impl<Ret (Obj::*)(Args...)> {
        static constexpr bool is_mutable = true;
        static constexpr size_t arity = sizeof...(Args);
        using return_type = Ret;
        using args_type = std::tuple<Args...>;
    };

    // specialization for const lambas and const operator()
    template<typename Obj, typename Ret, typename... Args>
    struct functor_traits_impl<Ret (Obj::*)(Args...) const> {
        static constexpr bool is_mutable = false;
        static constexpr size_t arity = sizeof...(Args);
        using return_type = Ret;
        using args_type = std::tuple<Args...>;
    };

    using op_type = decltype(&Op::operator());

public: // functor properties
    static constexpr bool is_mutable = functor_traits_impl<op_type>::is_mutable;
    static constexpr size_t arity = functor_traits_impl<op_type>::arity;

public: // decomposed function signature
    using args_type = typename functor_traits_impl<op_type>::args_type;
    using return_type = typename functor_traits_impl<op_type>::return_type;

    // access individual arg types at index I
    template<size_t I>
    using arg_t = std::tuple_element_t<I, args_type>;
};
} // namespace storage::detail 