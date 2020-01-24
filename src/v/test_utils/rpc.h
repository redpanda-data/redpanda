#pragma once
#include "reflection/adl.h"

template<typename T>
T serialize_roundtrip_rpc(T&& t) {
    iobuf io = reflection::to_iobuf(std::forward<T>(t));
    iobuf_parser parser(std::move(io));
    return reflection::adl<T>{}.from(parser);
}
