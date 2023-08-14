set(GOPATH ${CMAKE_CURRENT_BINARY_DIR})

function(add_go_dependency NAME MAIN_SRC)
  get_filename_component(MAIN_SRC_ABS ${MAIN_SRC} ABSOLUTE)
  string(REPLACE "-" "_" target_name ${NAME})
  add_custom_command(OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${NAME}"
                    COMMAND env GOPATH=${GOPATH} ${CMAKE_GO_BINARY} build
                    -modcacherw -o "${CMAKE_CURRENT_BINARY_DIR}/${NAME}"
                    ${CMAKE_GO_FLAGS} ${MAIN_SRC}
                    WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
                    DEPENDS "${CMAKE_CURRENT_LIST_DIR}/${MAIN_SRC}")
  add_custom_target(${target_name} DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/${NAME}")
  install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/${NAME} DESTINATION bin)
endfunction(add_go_dependency)

add_go_dependency(kafka-request-generator kafka-request-generator.go)
