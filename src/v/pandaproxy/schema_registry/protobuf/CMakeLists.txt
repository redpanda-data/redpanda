find_package(Protobuf REQUIRED)

set(PROTO_FILES
  confluent/meta.proto
  confluent/types/decimal.proto
  google/type/calendar_period.proto
  google/type/color.proto
  google/type/date.proto
  google/type/datetime.proto
  google/type/dayofweek.proto
  google/type/decimal.proto
  google/type/expr.proto
  google/type/fraction.proto
  google/type/interval.proto
  google/type/latlng.proto
  google/type/localized_text.proto
  google/type/money.proto
  google/type/month.proto
  google/type/phone_number.proto
  google/type/postal_address.proto
  google/type/quaternion.proto
  google/type/timeofday.proto
)

set(PROTOBUF_GENERATE_CPP_APPEND_PATH OFF)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

v_cc_library(
  NAME pandaproxy_schema_registry_protobuf
  SRCS ${PROTO_SRCS}
  DEPS protobuf::libprotobuf
)

target_include_directories(v_pandaproxy_schema_registry_protobuf
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
)
