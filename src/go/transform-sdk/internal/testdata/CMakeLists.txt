execute_process(COMMAND ${CMAKE_COMMAND} -E env 
  ${GO_PROGRAM} env GOROOT
  OUTPUT_VARIABLE GOROOT 
  OUTPUT_STRIP_TRAILING_WHITESPACE)
find_package(Python3 REQUIRED COMPONENTS Interpreter)

function(add_wasm_transform NAME)
  find_program(TINYGO_BIN "tinygo")
  set(wasm_output "${CMAKE_CURRENT_BINARY_DIR}/${NAME}.wasm")
  set(tinygo_cmd ${TINYGO_BIN} build -o ${wasm_output} -quiet -target wasi -scheduler none "${NAME}/transform.go")
  set(gopath ${CMAKE_CURRENT_BINARY_DIR}/${NAME})
  add_custom_command(OUTPUT ${wasm_output}
                     COMMAND Python3::Interpreter ${CMAKE_CURRENT_SOURCE_DIR}/retry.py
                     ARGS -- ${CMAKE_COMMAND} -E env PATH="${GOROOT}/bin:$ENV{PATH}" GOPATH="${gopath}" GOFLAGS="-modcacherw" GOROOT="${GOROOT}" ${tinygo_cmd}
                     WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
		     DEPENDS 
                    "${CMAKE_CURRENT_LIST_DIR}/${NAME}/transform.go"
                    # If we update any libraries then we need to rebuild transforms
                    "${CMAKE_CURRENT_LIST_DIR}/go.sum"
                    "${CMAKE_CURRENT_LIST_DIR}/go.mod")
  string(REPLACE "-" "_" target_name ${NAME})
  add_custom_target(wasm_testdata_${target_name} DEPENDS "${wasm_output}")
endfunction(add_wasm_transform)

add_wasm_transform(identity) 
add_wasm_transform(transform-error) 
add_wasm_transform(transform-panic) 
add_wasm_transform(setup-panic) 
add_wasm_transform(wasi) 
add_wasm_transform(schema-registry) 
add_wasm_transform(dynamic) 

