function(rpcgen)
  set(one_value_args TARGET VAR IN_FILE OUT_FILE)
  set(multi_value_args INCLUDES LIBRARIES DEFINITIONS COMPILE_OPTIONS)
  cmake_parse_arguments(args "" "${one_value_args}" "${multi_value_args}" ${ARGN})
  get_filename_component(out_dir ${args_OUT_FILE} DIRECTORY)
  set(generator "${PROJECT_SOURCE_DIR}/src/v/rpc/rpc_compiler.py")
  add_custom_command(
    DEPENDS
      ${args_IN_FILE}
      ${generator}
    OUTPUT ${args_OUT_FILE}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${out_dir}
    COMMAND ${generator} --service_file ${args_IN_FILE} --output_file ${args_OUT_FILE})

  add_library(${args_TARGET} STATIC ${args_OUT_FILE})
  set_target_properties(${args_TARGET} PROPERTIES LINKER_LANGUAGE CXX)
  target_include_directories(${args_TARGET} PUBLIC ${args_INCLUDES}
    ${CMAKE_CURRENT_LIST_DIR})
  target_link_libraries(${args_TARGET} PUBLIC v::rpc)
  set(${args_OUT_FILE} ${args_TARGET} PARENT_SCOPE)
endfunction ()
