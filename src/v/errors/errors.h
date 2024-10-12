#pragma once

#include "base/vassert.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <exception>
#include <type_traits>
#include <variant>

namespace errors {

// Should implement 'E operator () (std::exception_ptr&& e) noexcept;
template<class E>
struct exception_mapping_policy;

template<class T, class E>
concept is_maybe_exception_mapping_policy = requires(T x) {
    //{x(std::exception_ptr())} -> std::same_as<E>;
    requires noexcept(x(std::exception_ptr()));
    requires std::is_invocable_r_v<E, T, std::exception_ptr>;
};

template<typename, typename = void>
constexpr bool is_maybe_type_v = false;

template<typename T>
constexpr bool
  is_maybe_type_v<T, std::void_t<decltype(sizeof(typename T::maybe_t))>>
  = true;

/// Result of the computation or the error code
/// \param T is a result type
template<class T, class E, class P = exception_mapping_policy<E>>
requires is_maybe_exception_mapping_policy<P, E> && (!std::is_void_v<T>)
class [[nodiscard]] maybe {
    struct value_tag {};

    using mapper = P;

    template<class F>
    maybe(value_tag, F&& f)
      : _something(std::forward<F>(f)) {}

    explicit maybe(E e)
      : _something(std::move(e)) {}

public:
    using maybe_t = maybe<T, E, P>;
    using value_t = T;

    // C-tors
    template<class F>
    static maybe_t ok(F&& val) {
        return maybe_t(value_tag(), std::forward<F>(val));
    }
    static maybe_t err(E e) { return maybe_t(e); }
    static maybe_t err(std::exception_ptr&& e) {
        mapper m;
        auto err = m(std::move(e));
        return maybe_t(err);
    }

    bool is_ok() const noexcept {
        return std::holds_alternative<T>(_something);
    }

    bool is_err() const noexcept {
        return std::holds_alternative<E>(_something);
    }

    std::optional<T> ok() const noexcept {
        if (is_ok()) {
            return std::make_optional(std::get<T>(_something));
        }
        return std::nullopt;
    }

    std::optional<E> err() const noexcept {
        if (is_err()) {
            return std::make_optional(std::get<E>(_something));
        }
        return std::nullopt;
    }

    template<class F>
    requires std::is_invocable_r_v<bool, F, T>
    bool is_ok_and(F&& f) const {
        return is_ok() && std::invoke(std::forward<F>(f), unwrap());
    }

    template<class F>
    requires std::is_invocable_r_v<bool, F, E>
    bool is_err_and(F&& f) const {
        return is_err() && std::invoke(std::forward<F>(f), unwrap_err());
    }

    T unwrap() const noexcept {
        vassert(is_ok(), "'unwrap' failed");
        return ok().value();
    }

    T&& unwrap_ref() && noexcept {
        vassert(is_ok(), "'unwrap_ref' failed");
        return std::get<T>(std::move(_something));
    }

    T unwrap_or_default() const noexcept { return ok().value_or(T()); }

    E unwrap_err() const noexcept {
        vassert(is_err(), "'unwrap_err' failed");
        return err().value();
    }

    /// Calls 'func' if the result is ok, otherwise returns err.
    /// The method can be used for control flow.
    template<class F>
    requires std::is_invocable_v<F, T>
    auto and_then(F&& func) const noexcept {
        using ret_t = decltype(func(T()));
        if constexpr (std::is_void_v<ret_t>) {
            // TODO: implement maybe<void, E> specialization
            // to handle this case
            static_assert(false, "specialization not implemented");
        } else if constexpr (is_maybe_type_v<ret_t>) {
            if (is_err()) {
                return ret_t::err(unwrap_err());
            }
            // Functor returns maybe type. In this case the expectation is that
            // the functor handles errors internally and only returns an
            // erroneous maybe value instead of throwing exceptions.
            static_assert(
              noexcept(func(T())), "Functor shouldn be declared as nothrow");
            return std::invoke(std::forward<F>(func), unwrap());
        } else {
            if (is_err()) {
                return maybe<ret_t, E, P>::err(unwrap_err());
            }
            try {
                // Functor returns value that should be converted to
                // the maybe type
                return maybe<ret_t, E, P>::ok(
                  std::invoke(std::forward<F>(func), unwrap()));
            } catch (...) {
                return maybe<ret_t, E, P>::err(std::current_exception());
            }
        }
        __builtin_unreachable();
    }

    /// Calls 'func' if the result is ok, otherwise returns err.
    /// This method allows functor to move the value out of the
    /// 'maybe' wrapper to avoid making copies.
    template<class F>
    requires std::is_invocable_v<F, T>
    auto and_then_ref(F&& func) && noexcept {
        using ret_t = decltype(func(T()));
        if constexpr (std::is_void_v<ret_t>) {
            // TODO: implement maybe<void, E> specialization
            // to handle this case
            static_assert(false, "specialization not implemented");
        } else if constexpr (is_maybe_type_v<ret_t>) {
            if (is_err()) {
                return ret_t::err(unwrap_err());
            }
            // Functor returns maybe type. In this case the expectation is that
            // the functor handles errors internally and only returns an
            // erroneous maybe value instead of throwing exceptions.
            static_assert(
              noexcept(func(T())), "Functor shouldn be declared as nothrow");
            return std::invoke(
              std::forward<F>(func), std::get<T>(std::move(_something)));
        } else {
            if (is_err()) {
                return maybe<ret_t, E, P>::err(unwrap_err());
            }
            try {
                return maybe<ret_t, E, P>::ok(std::invoke(
                  std::forward<F>(func), std::get<T>(std::move(_something))));
            } catch (...) {
                return maybe<ret_t, E, P>::err(std::current_exception());
            }
        }
        __builtin_unreachable();
    }

    /// Calls 'func' if the result is err, otherwise returns ok.
    /// The method can be used for control flow.
    template<class F>
    requires std::is_invocable_v<F, E>
    auto or_else(F&& func) const noexcept {
        using ret_t = decltype(func(E()));
        if constexpr (std::is_void_v<ret_t>) {
            // TODO: implement maybe<void, E> specialization
            // to handle this case
            static_assert(false, "specialization not implemented");
        } else if constexpr (is_maybe_type_v<ret_t>) {
            if (is_ok()) {
                return ret_t::ok(unwrap());
            }
            // Functor returns maybe type. In this case the expectation is that
            // the functor handles errors internally and only returns an
            // erroneous maybe value instead of throwing exceptions.
            static_assert(
              noexcept(func(T())), "Functor shouldn be declared as nothrow");
            return std::invoke(std::forward<F>(func), unwrap_err());
        } else {
            if (is_ok()) {
                return maybe<ret_t, E, P>::ok(unwrap());
            }
            try {
                // Functor returns value that should be converted to
                // the maybe type.
                return maybe<ret_t, E, P>::ok(
                  std::invoke(std::forward<F>(func), unwrap_err()));
            } catch (...) {
                return maybe<ret_t, E, P>::err(std::current_exception());
            }
        }
        __builtin_unreachable();
    }

    /// Calls 'func' if the result is err, otherwise returns ok.
    /// The method can be used for control flow.
    template<class F>
    requires std::is_invocable_v<F, E>
    auto or_else_ref(F&& func) && noexcept {
        using ret_t = decltype(func(E()));
        if constexpr (std::is_void_v<ret_t>) {
            // TODO: implement maybe<void, E> specialization
            // to handle this case
            static_assert(false, "specialization not implemented");
        } else if constexpr (is_maybe_type_v<ret_t>) {
            if (is_ok()) {
                return ret_t::ok(std::get<T>(std::move(_something)));
            }
            // Functor returns maybe type. In this case the expectation is that
            // the functor handles errors internally and only returns an
            // erroneous maybe value instead of throwing exceptions.
            static_assert(
              noexcept(func(T())), "Functor shouldn be declared as nothrow");
            return std::invoke(std::forward<F>(func), unwrap_err());
        } else {
            if (is_ok()) {
                return maybe<ret_t, E, P>::ok(
                  std::get<T>(std::move(_something)));
            }
            try {
                // Functor returns value that should be converted to
                // the maybe type.
                return maybe<ret_t, E, P>::ok(
                  std::invoke(std::forward<F>(func), unwrap_err()));
            } catch (...) {
                return maybe<ret_t, E, P>::err(std::current_exception());
            }
        }
        __builtin_unreachable();
    }

private:
    std::variant<T, E> _something;
};

} // namespace errors

namespace seastar {

namespace internal {

template<typename T, typename E, typename P>
class coroutine_traits_base<errors::maybe<T, E, P>> {
public:
    class promise_type final : public seastar::task {
        seastar::promise<errors::maybe<T, E, P>> _promise;

    public:
        promise_type() = default;
        promise_type(promise_type&&) = delete;
        promise_type(const promise_type&) = delete;

        template<typename... U>
        void return_value(U&&... value) {
            _promise.set_value(
              errors::maybe<T, E, P>::ok(std::forward<U>(value)...));
        }

        void return_value(coroutine::exception ce) noexcept {
            _promise.set_value(errors::maybe<T, E>::err(std::move(ce.eptr)));
        }

        void set_exception(std::exception_ptr&& eptr) noexcept {
            _promise.set_value(errors::maybe<T, E>::err(std::move(eptr)));
        }

        [[deprecated("This is deprecated in Seastar")]]
        void return_value(future<errors::maybe<T, E>>&& fut) noexcept {
            fut.forward_to(std::move(_promise));
        }

        void unhandled_exception() noexcept {
            _promise.set_value(
              errors::maybe<T, E>::err(std::current_exception()));
        }

        seastar::future<errors::maybe<T, E>> get_return_object() noexcept {
            return _promise.get_future();
        }

        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }

        virtual void run_and_dispose() noexcept override {
            auto handle = std::coroutine_handle<promise_type>::from_promise(
              *this);
            handle.resume();
        }

        task* waiting_task() noexcept override {
            return _promise.waiting_task();
        }

        scheduling_group set_scheduling_group(scheduling_group sg) noexcept {
            return std::exchange(this->_sg, sg);
        }
    };
};
} // namespace internal

} // namespace seastar
