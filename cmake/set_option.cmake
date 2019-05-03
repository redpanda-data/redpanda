# source
# https://github.com/edsiper/cmake-options
#
macro(RP_SET_OPTION option value)
  set(${option} ${value} CACHE INTERNAL "" FORCE)
endmacro()
