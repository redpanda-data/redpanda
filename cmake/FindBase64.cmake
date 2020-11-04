include (FindPackageHandleStandardArgs REQUIRED)

find_path (Base64_IMPORTED_OBJECT
  NAMES libbase64.o
  HINTS ${REDPANDA_DEPS_INSTALL_DIR}/lib)

find_path (Base64_INCLUDE_DIR
  NAMES libbase64.h
  HINTS ${REDPANDA_DEPS_INSTALL_DIR}/include)

mark_as_advanced (
  Base64_IMPORTED_OBJECT
  Base64_INCLUDE_DIR)

find_package_handle_standard_args (Base64 DEFAULT_MSG
  Base64_IMPORTED_OBJECT
  Base64_INCLUDE_DIR)

if (Base64_FOUND)
  set (Base64_IMPORTED_OBJECTS ${Base64_IMPORTED_OBJECT})
  set (Base64_INCLUDE_DIRS ${Base64_INCLUDE_DIR})
endif ()

if (Base64_FOUND AND NOT (TARGET Base64::base64))
  add_library(Base64::base64 OBJECT IMPORTED)
  set_target_properties(Base64::base64
    PROPERTIES
      IMPORTED_OBJECTS ${Base64_IMPORTED_OBJECT}/libbase64.o
      INTERFACE_INCLUDE_DIRECTORIES ${Base64_INCLUDE_DIR})
endif ()
