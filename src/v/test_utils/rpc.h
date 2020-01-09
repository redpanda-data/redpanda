#pragma once
#include "rpc/deserialize.h"
#include "rpc/serialize.h"

template<typename T>
ss::future<T> serialize_roundtrip_rpc(T t) {
    iobuf out;
    rpc::serialize(out, std::move(t));
    return rpc::deserialize<T>(std::move(out));
}
