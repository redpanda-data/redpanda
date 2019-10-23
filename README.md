# How to get started

## Building 

```sh
python3 tools/build.py                                    \
        --deps=true             `# [true, false]`         \
        --targets=all           `# [cpp, go]`             \
        --build=release         `# [release, debug]`      \
        --log=debug             `# [info, debug, trace]`  \
        --fmt=false             `# [true, false]`         \
        --clang=internal        `# [<empty>, internal]`   \
        --packages rpm deb tar   # [rpm, deb, tar]
```


## Incremental 

```sh
cd build/release  # [debug, release] folders
ninja redpanda    # can be any target, use 'cmake -N' to list 
```


## Debugging main build:

```sh
cmake -DCMAKE_BUILD_TYPE=Debug  `# [Debug, Release]`           \
      -Bbuild/debug             `# [debug, release] folders`   \
      -H.                        # assumes you are at the tld 

make -j8                         # or (($(nproc)-1)) 
```


## Contributing
* [See our contributing guide](CONTRIBUTING.md)


```sh
# Run before submitting changes
python3 tools/build.py   \
        --cpplint=1      \
        --log=debug      \
        --build=none     \
        --files=all
```
