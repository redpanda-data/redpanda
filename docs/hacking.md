# hacking

## to build: 

`./tools/build.py --deps=true --build=release`

change `--build=debug` for debug builds and remove `--deps=true` if you don't want to 
install the deps. 

To build one particular target do:

`cd build/Release && ninja redpanda` it will build the `redpanda` target and all the transitive
dependencies and nothing else. 


## gdb

We build our DEBUG binaries w/ `--split-dwarf`

More info at https://gcc.gnu.org/wiki/DebugFission.

This effectively splits debug info files to a `.dwo`

Note that distcc is *not* compatible with it, but icecream
(https://github.com/icecc/icecream) is.
