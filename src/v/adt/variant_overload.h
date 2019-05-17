#pragma once

// https://www.bfilipek.com/2018/06/variant.html#overload
template <class... Ts>
struct variant_overload : Ts... {
  using Ts::operator()...;
};
template <class... Ts>
variant_overload(Ts...)->variant_overload<Ts...>;
