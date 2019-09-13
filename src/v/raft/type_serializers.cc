#include "raft/types.h"
#include "rpc/serialize.h"
#include "rpc/types.h"

namespace rpc {

template<>
void serialize(bytes_ostream& out, model::broker&& r) {
    rpc::serialize(out, r.id()());
    rpc::serialize(out, sstring(r.host()));
    rpc::serialize(out, r.port());
}

// from wire
template<>
inline future<> deserialize(source& in, model::offset& r) {
    using type = model::offset::type;
    type& ref = *reinterpret_cast<type*>(&r);
    return deserialize(in, ref);
}
template<>
inline future<> deserialize(source& in, model::broker& r) {
    return do_with(
      model::node_id(),
      sstring(),
      int32_t(),
      [&in, &r](model::node_id& id, sstring& host, int32_t& port) {
          return rpc::deserialize(in, id)
            .then([&] { return rpc::deserialize(in, host); })
            .then([&] { return rpc::deserialize(in, port); })
            .then([&]() mutable {
                r = model::broker(std::move(id), std::move(host), port, {});
            });
      });
}

} // namespace rpc
