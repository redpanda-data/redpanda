# client
add_executable(http_demo_client client.cc)
target_link_libraries(http_demo_client PUBLIC v::model v::rpc v::http v::ssx)
set_property(TARGET http_demo_client PROPERTY POSITION_INDEPENDENT_CODE ON)

