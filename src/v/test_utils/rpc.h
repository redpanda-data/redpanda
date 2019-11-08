#pragma once
#include "rpc/deserialize.h"
#include "rpc/serialize.h"
#include "test_utils/bytes_ostream.h"

template<typename T>
future<T> serialize_roundtrip_rpc(T t) {
    bytes_ostream out;
    rpc::serialize(out, std::move(t));
    auto fb = release_to_fragbuf(std::move(out));
    
    return rpc::deserialize<T>(std::move(fb));
}