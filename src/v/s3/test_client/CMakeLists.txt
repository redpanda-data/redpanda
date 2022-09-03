# client
add_executable(s3_test_client s3_test_client_main.cc)
target_link_libraries(s3_test_client PUBLIC v::model v::net v::http v::s3 v::cloud_roles)
set_property(TARGET s3_test_client PROPERTY POSITION_INDEPENDENT_CODE ON)

