#pragma once

namespace seastar {

template <typename T>
class shared_ptr;

template <typename T>
shared_ptr<T> make_shared(T&&);

template <typename T, typename... A>
shared_ptr<T> make_shared(A&&... a);

}

using namespace seastar;
using seastar::shared_ptr;
using seastar::make_shared;

