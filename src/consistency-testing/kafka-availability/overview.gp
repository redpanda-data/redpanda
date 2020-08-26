set terminal png size 1600,1200
set output 'overview.png'

set multiplot

set lmargin 6
set rmargin 10


set yrange [0:20000]
set xrange [0:240]
set pointsize 0.2

set size 1, 0.2
set origin 0, 0
unset ytics
set y2tics

plot 'lat-ok.log' using 1:2 title "latency" with points lt rgb "black" pt 7

set parametric
plot [t=0:20000] 60,t notitle lt rgb "red"
plot [t=0:20000] 120,t notitle lt rgb "red"
unset parametric


set yrange [0:160000]
set pointsize 0.2

set size 1, 0.4
set origin 0, 0.2
set tmargin 0
set border 11

plot 'lat-ok.log' using 1:2 title "latency" with points lt rgb "black" pt 7

set parametric
plot [t=0:160000] 60,t notitle lt rgb "red"
plot [t=0:160000] 120,t notitle lt rgb "red"
unset parametric

set yrange [0:1000]

set size 1, 0.4
set origin 0, 0.6
set format x ""
set bmargin 0
set tmargin 3
set border 15
unset y2tics
set ytics

plot '1s.log' using 1:2 title "ops per 1s" with line lt rgb "black"

set parametric
plot [t=0:1000] 60,t notitle lt rgb "red"
plot [t=0:1000] 120,t notitle lt rgb "red"
unset parametric

unset multiplot