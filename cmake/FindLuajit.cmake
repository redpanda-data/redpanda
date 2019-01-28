find_package (PkgConfig REQUIRED)

pkg_search_module (Luajit_PC
  QUIET
  luajit-5.1)

find_library(Luajit_LIBRARY
  NAMES luajit-5.1
  HINTS
    ${Luajit_PC_LIBDIR}
    ${Luajit_PC_LIBRARY_DIRS})

find_path(Luajit_INCLUDE_DIR
  NAMES "luajit-2.1/lua.h"
  HINTS
    ${Luajit_PC_INCLUDEDIR}
    ${Luajit_PC_INCLUDEDIRS})

mark_as_advanced(
  Luajit_LIBRARY
  Luajit_INCLUDE_DIR)

include (FindPackageHandleStandardArgs)

find_package_handle_standard_args(Luajit
  REQUIRED_VARS
    Luajit_LIBRARY
    Luajit_INCLUDE_DIR
  VERSION_VAR Luajit_PC_VERSION)

set (Luajit_LIBRARIES ${Luajit_LIBRARY})
list(APPEND Luajit_INCLUDE_DIRS
  ${Luajit_INCLUDE_DIR}
  # so that it works w/ sol2 library
  "${Luajit_INCLUDE_DIR}/luajit-2.1"
  )

if (Luajit_FOUND AND NOT (TARGET Luajit::luajit))
  add_library (Luajit::luajit UNKNOWN IMPORTED)

  set_target_properties (Luajit::luajit
    PROPERTIES
      IMPORTED_LOCATION ${Luajit_LIBRARY}
      INTERFACE_INCLUDE_DIRECTORIES "${Luajit_INCLUDE_DIRS}")
endif ()
