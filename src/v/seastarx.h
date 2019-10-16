#pragma once

namespace seastar {

template<typename T>
class shared_ptr;

template<typename T>
shared_ptr<T> make_shared(T&&);

template<typename T, typename... A>
shared_ptr<T> make_shared(A&&... a);

} // namespace seastar

using namespace seastar; // NOLINT
using seastar::make_shared;
using seastar::shared_ptr;
