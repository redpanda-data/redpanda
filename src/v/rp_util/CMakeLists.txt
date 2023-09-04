# Packages utilities exposed by core that are useful to clients.
add_executable(rp_util main.cc)
set_property(TARGET rp_util PROPERTY POSITION_INDEPENDENT_CODE ON)
target_link_libraries(rp_util PUBLIC v::clientutil v::compat v::version)
install(TARGETS rp_util DESTINATION bin)
