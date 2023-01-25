
find_package(LibXml2 REQUIRED)

v_cc_library(
  NAME cloud_storage_clients
  SRCS
    abs_client.cc
    abs_error.cc
    client_pool.cc
    client_probe.cc
    configuration.cc
    s3_client.cc
    s3_error.cc
    util.cc
    xml_sax_parser.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::cloud_roles
    v::net
    LibXml2::LibXml2
  DEFINES
    -DBOOST_ASIO_HAS_STD_INVOKE_RESULT
)
add_subdirectory(tests)
add_subdirectory(test_client)

