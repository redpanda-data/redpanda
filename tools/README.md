# Build system tools

These tools are typically invoked from the main `$ROOT/build.py` 


## Notes

To run a program with toplev.py (from pmutools)

```sh
#!/bin/bash
set -evx
# csv output
toplev.py  -I 1000 -l3 -x, -o tlev.csv --core C1,C2 "$@"
```

To perf record useful things for tracking perf issues:

```sh
#!/bin/bash
set -evx
perf record \
     -e L1-icache-load-misses,branch-load-misses,cache-misses \
     -e LLC-loads,LLC-load-misses,instructions \
     -e cycles,branch-load-misses,faults,bus-cycles,mem-stores \
     -e branches \
     -p $1

```

To make an HDR histogram graph:

```
$ROOT/tools/make_percentile_plot server.hgrm
```

