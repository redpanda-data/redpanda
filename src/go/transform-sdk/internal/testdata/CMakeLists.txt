set(GOPATH ${CMAKE_CURRENT_BINARY_DIR})
execute_process(COMMAND ${CMAKE_COMMAND} -E env 
  ${CMAKE_GO_BINARY} env GOROOT 
  OUTPUT_VARIABLE GOROOT 
  OUTPUT_STRIP_TRAILING_WHITESPACE)

function(add_wasm_transform NAME)
  find_program(TINYGO_BIN "tinygo")
  set(WASM_OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${NAME}.wasm")
  add_custom_command(OUTPUT ${WASM_OUTPUT}
                     COMMAND env GOPATH="${GOPATH}" GOROOT="${GOROOT}" 
                     ${TINYGO_BIN} build -o ${WASM_OUTPUT} -target wasi
                     "${NAME}/transform.go"
                     WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
		     DEPENDS 
                    "${CMAKE_CURRENT_LIST_DIR}/${NAME}/transform.go"
                    # If we update any libraries then we need to rebuild transforms
                    "${CMAKE_CURRENT_LIST_DIR}/go.sum"
                    "${CMAKE_CURRENT_LIST_DIR}/go.mod"
                     )
  string(REPLACE "-" "_" target_name ${NAME})
  add_custom_target(wasm_testdata_${target_name} DEPENDS "${WASM_OUTPUT}")
endfunction(add_wasm_transform)

add_wasm_transform(identity) 
add_wasm_transform(transform-error) 
add_wasm_transform(transform-panic) 
add_wasm_transform(setup-panic) 
add_wasm_transform(wasi) 

