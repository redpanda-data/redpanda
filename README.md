This source code is Private and Confidential Property of Vectorized, Inc.
Please report abuse to: security@vectorized.io

# How to get started [build status](https://console.cloud.google.com/cloud-build/builds?project=redpandaci)

## Setup

We mainly work with four types of builds based on the compiler (`gcc` 
or `clang`) and CMake build type (`debug` or `release`). The build 
folder has the structure `build/<build-type>/<compiler>`. To get 
started, run:

```
tools/bootstraph.sh
```

The above (idempotent) [bootstrapping script](tools/bootstrap.sh):

  * Installs OS package dependencies on deb- and rpm-based 
    distributions (we mainly develop on Fedora but others should be 
    supported).
  * Installs [`vtools`](./tools), our tooling Python-based CLI 
    interface, putting the binary in the `build/bin` folder.
  * Installs `clang`, from source, putting the build and binaries 
    inside the `v/build/llvm` folder.
  * Installs 3rd-party project dependencies, also from source, putting 
    the build files in `build/<build-type>/<compiler>/v_deps_build` 
    and output binary files in 
    `build/<build-type>/<compiler>/v_deps_install`. 3rd party code is 
    built differently depending on the build type (debug builds are 
    sanitized), thus they need to be specialized for each compiler and 
    build type.

The `vtools` command can be executed with:

```
build/bin/vtools
```

The [`tools/alias.sh`](tools/alias.sh) script defines a set of utility 
aliases:

```bash
source tools/alias.sh
```

the above makes `vtools` and `bootstrap` commands available in the 
current shell that can be executed from any subfolder within the 
project.

If you use `zsh` simply copy the `tools/alias.sh` to your 
~/.oh-my-zsh/custom/vectorized.sh and never worry about this again!

## Build

To build `redpanda`:

```bash
vtools build cpp [--reconfigure]
```

The `--reconfigure` flag re-runs `cmake`, which might be needed after 
fetching updates from git (e.g. after a `git pull`).


By default, `vtools` assumes `release` and `gcc` options. To build 
using clang:

```bash
vtools build cpp --clang
```

to build a `debug` build:

```bash
vtools build cpp --build-type debug
```

For running other tasks:

```bash
vtools <subcommand> --help
```

> **NOTE**: if something unexpected happens with the build, you might 
> want to resort to the "nuclear option":
>
> ```bash
> rm -r ./build/<build-type>/<compiler>/
> ```
>
> the above deletes the build folder for a build-type/compiler 
> combination.


## Incremental build

```sh
cd build/release/gcc  # [debug,release]/[gcc,clang] folders
ninja redpanda    # can be any target, use 'cmake -N' to list 
```

## Debugging main build:

```sh
cmake -DCMAKE_BUILD_TYPE=Debug   # [Debug, Release]           \
      -Bbuild/<type>/<compiler>  # [debug,release], [gcc,clang] folders \
      -H.                        # assumes you are at the tld \
make -j8                         # or (($(nproc)-1)) 
```

## Contributing

See our [contributing guide](CONTRIBUTING.md).
