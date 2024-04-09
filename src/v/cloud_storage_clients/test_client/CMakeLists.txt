# s3_client
add_executable(s3_test_client s3_test_client_main.cc)
target_link_libraries(s3_test_client PUBLIC v::model v::net v::http v::cloud_storage_clients v::cloud_roles v::syschecks)
set_property(TARGET s3_test_client PROPERTY POSITION_INDEPENDENT_CODE ON)

# abs_client
add_executable(abs_test_client abs_test_client_main.cc)
target_link_libraries(abs_test_client PUBLIC v::model v::net v::http v::cloud_storage_clients v::cloud_roles v::syschecks)
set_property(TARGET abs_test_client PROPERTY POSITION_INDEPENDENT_CODE ON)
