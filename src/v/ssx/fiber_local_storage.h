#include "seastarx.h"
#include "ssx/this_fiber_id.h"
#include "utils/intrusive_list_helpers.h"
#include "vassert.h"

#include <seastar/core/future.hh>

#include <boost/iterator/filter_iterator.hpp>

namespace internal {

template<class Tag, class ValueT>
class fiber_local_impl {
public:
    ~fiber_local_impl() = default;

    explicit fiber_local_impl(ValueT&& value)
      : _id(ssx::this_fiber_id())
      , _value(std::move(value)) {
        _fifo.push_front(std::ref(*this));
    }

    fiber_local_impl()
      : _id(ssx::this_fiber_id())
      , _value() {
        _fifo.push_front(std::ref(*this));
    }

    fiber_local_impl(const fiber_local_impl&) = delete;
    fiber_local_impl& operator=(const fiber_local_impl&) = delete;

    fiber_local_impl(fiber_local_impl&& other) noexcept
      : _id(other._id)
      , _value(std::move(other._value)) {
        _hook.swap_nodes(other._hook);
    }

    fiber_local_impl& operator=(fiber_local_impl&& other) noexcept {
        _id = other._id;
        _value = std::move(other._value);
        _hook.swap_nodes(other._hook);
    }

    static fiber_local_impl<Tag, ValueT>* get_fiber_local() {
        auto id = ssx::this_fiber_id();
        auto it = std::find_if(
          _fifo.begin(),
          _fifo.end(),
          [id](const fiber_local_impl<Tag, ValueT>& it) {
              if (it._id == id) {
                  return true;
              }
              return false;
          });
        return it == _fifo.end() ? nullptr : &(*it);
    }

    template<class T>
    void set(T&& val) noexcept {
        _value = std::forward<T>(val);
    }

    const ValueT& get() const noexcept { return _value; }

    static bool is_current_fiber(const fiber_local_impl<Tag, ValueT>& fls) {
        auto id = ssx::this_fiber_id();
        return fls._id == id;
    }

    auto begin() const {
        return boost::make_filter_iterator(
          &is_current_fiber, _fifo.begin(), _fifo.end());
    }

    auto end() const {
        return boost::make_filter_iterator(
          &is_current_fiber, _fifo.end(), _fifo.end());
    }

private:
    uint64_t _id;
    intrusive_list_hook _hook;
    ValueT _value;

    using intr_list_t
      = intrusive_list<fiber_local_impl, &fiber_local_impl::_hook>;

    /// Thread local storage for the fiber
    static thread_local intr_list_t _fifo;
};

template<class Tag, class ValueT>
thread_local typename fiber_local_impl<Tag, ValueT>::intr_list_t
  fiber_local_impl<Tag, ValueT>::_fifo
  = intr_list_t();

} // namespace internal

namespace ssx {

/// Fiber local storage instance.
///
/// This is a container type that can hold object of any
/// type which becomes available for any code that runs in
/// the same fiber.
/// Any fiber_local object with the same tag type will have the same content.
/// Even if the fiber_local object is recreated the data that it holds will
/// still be available.
///
/// When the fiber_local instance gets destroyed its data is destroyed as well
/// and no longer available.
template<class Tag, class ValueT>
class fiber_local : private internal::fiber_local_impl<Tag, ValueT> {
public:
    using base = internal::fiber_local_impl<Tag, ValueT>;

    fiber_local() = default;
    fiber_local(const fiber_local&) = delete;
    fiber_local& operator=(const fiber_local&) = delete;
    fiber_local(fiber_local&&) noexcept = default;
    fiber_local& operator=(fiber_local&&) noexcept = default;
    ~fiber_local() = default;

    explicit fiber_local(ValueT&& v)
      : base(std::move(v)) {}

    /// Get top level FLS value
    using base::get;
    /// Update top level FLS value
    using base::set;
    /// Enumerate all FLS instances that belong to the current fiber
    using base::begin;
    using base::end;
};

/// Selector for the fiber local storage
///
/// The object of this type can be used to fetch data from fiber_local instance.
/// The fiber_local instance need to be created on the stack of the fiber before
/// any of the methods of the fiber_local_selector are used.
///
/// \code
/// struct my_tag_type_t;
/// ss::future<int> async_op() {
///     fiber_local<my_tag_type_t, int> fiber(1);
///     return another_async_op().then([] {
///         fiber_local_selector<my_tag_type_t, int> selector;
///         return ss::make_ready_future<int>(selector.get());
///     });
/// }
/// \endcode
template<class Tag, class ValueT>
class fiber_local_selector {
    using impl_t = internal::fiber_local_impl<Tag, ValueT>;

    // Try to grab fiber_local instance from TLS.
    // Terminate on failure.
    impl_t* get_local_storage() const {
        auto ptr = impl_t::get_fiber_local();
        vassert(ptr, "fiber_local doesn't exist for the current fiber");
        return ptr;
    }
    // Try to grab fiber_local instance from TLS.
    impl_t* maybe_get_local_storage() const {
        return impl_t::get_fiber_local();
    }

public:
    ValueT get() const {
        auto fls = get_local_storage();
        return fls->get();
    }

    std::optional<ValueT> try_get() const {
        auto fls = maybe_get_local_storage();
        if (fls) {
            return fls->get();
        }
        return std::nullopt;
    }

    ValueT operator*() const { return get(); }

    bool has_value() const {
        auto fls = maybe_get_local_storage();
        return fls != nullptr;
    }

    template<class T>
    void set(T&& value) const {
        auto fls = get_local_storage();
        fls->template set<ValueT>(std::forward<T>(value));
    }

    template<class T>
    bool try_set(T&& value) const {
        auto fls = maybe_get_local_storage();
        if (fls) {
            fls->template set<ValueT>(std::forward<T>(value));
            return true;
        }
        return false;
    }
};

} // namespace ssx
