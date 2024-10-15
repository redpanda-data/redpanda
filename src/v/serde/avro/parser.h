#pragma once

#include "bytes/iobuf.h"
#include "container/fragmented_vector.h"

#include <avro/Schema.hh>

#include <variant>

namespace serde::avro {

namespace parsed {

struct record;
struct list;
struct map;
struct avro_null {};
struct avro_union;
/**
 * Avro primitive being either a number, enum, string, bytes, avro fixed or null
 */
using primitive = std::variant<
  avro_null,
  double,
  float,
  int32_t, // Can be an enum value
  int64_t,
  bool,
  iobuf // iobuf can represent either bytes, string or fixed types
  >;

/**
 * Variant representing a result of Avro message deserialization
 */
using message = std::variant<primitive, map, record, avro_union, list>;

/**
 * Avro map represented as a vector of pairs, the same as in Avro. Avro MAP does
 * not guarantee unique keys
 */
struct map {
    chunked_vector<std::pair<iobuf, std::unique_ptr<message>>> entries;
};

struct list {
    chunked_vector<std::unique_ptr<message>> elements;
};

/**
 * Avro record, representing a struct with a vector of fields. As for the AVRO
 * specification the order of fields in the vector is the exactly the same as
 * the order of fields in the schema, AVRO do not support optional fields. The
 * optionals are often expressed as a union of an actual type and AVRO_NULL,
 * which is expressed as a separate entry in the fields vector.
 */
struct record {
    chunked_vector<std::unique_ptr<message>> fields;
};

/**
 * AVRO union is decoded in a special way to make it possible to easily corelate
 * the parsing result with Avro schema.
 */
struct avro_union {
    // an index of the type that the union is holding
    size_t branch;
    std::unique_ptr<message> message;
};

} // namespace parsed

/**
 * Simple binary Avro parser following the DFS traversal of Avro schema to
 * decode the binary representation stored in the input_buffer.
 *
 * The parser supports all the types from AVRO spec and any incompatibilities
 * should be reported as a bug.
 *
 * To make the result type hierarchy and the parser implementation simple the
 * parser doesn't interpret AVRO logical types and it is up to the caller to
 * interpret the content those type according to the logic type specification.
 *
 * IMPORTANT:
 * The returned message in the case of strings or bytes fields will
 * hold a reference (via share) to the original input data iobuf.
 *
 */
ss::future<std::unique_ptr<parsed::message>>
parse(iobuf input_buffer, const ::avro::ValidSchema& schema);

} // namespace serde::avro
