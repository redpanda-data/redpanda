#pragma once

#include "config/data_directory_path.h"
#include "config/seed_server.h"
#include "config/tls_config.h"
#include "likely.h"
#include "model/metadata.h"

#include <fmt/core.h>

#define RAPIDJSON_HAS_STDSTRING 1
#define RAPIDJSON_ASSERT(x)                                                    \
    do {                                                                       \
        if (unlikely(!(x))) {                                                  \
            std::cerr << "Rapidjson failure: " << __FILE__ << ":" << __LINE__  \
                      << "' " << #x << " '";                                   \
            std::terminate();                                                  \
        }                                                                      \
    } while (0)

#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>
#include <seastar/net/socket_defs.hh>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <chrono>
#include <type_traits>

namespace config {

void rjson_serialize(rapidjson::Writer<rapidjson::StringBuffer>& w, short v);

void rjson_serialize(rapidjson::Writer<rapidjson::StringBuffer>& w, bool v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, long long v);

void rjson_serialize(rapidjson::Writer<rapidjson::StringBuffer>& w, int v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, unsigned int v);

void rjson_serialize(rapidjson::Writer<rapidjson::StringBuffer>& w, long v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, unsigned long v);

void rjson_serialize(rapidjson::Writer<rapidjson::StringBuffer>& w, double v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const ss::sstring& v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const ss::socket_address& v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const unresolved_address& v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const std::chrono::milliseconds& v);

template<typename T>
void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const std::optional<T>& v) {
    if (v) {
        rjson_serialize(w, *v);
        return;
    }
    w.Null();
}

template<typename T, typename Tag>
void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const named_type<T, Tag>& v) {
    rjson_serialize(w, v());
}

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const data_directory_path& v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const seed_server& v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const key_cert& v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const tls_config& v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const std::vector<seed_server>& v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const std::vector<ss::sstring>& v);

} // namespace config
