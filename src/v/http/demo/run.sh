#!/bin/bash
if [[ ! -d "./src/v/http/demo" ]]; then
  echo "The script should be started from root v directory"
  exit 1
fi
python ./src/v/http/demo/echo.py &
echo "Starting web server..."
sleep 3

demo_client_bin=
if [[ -f "./build/release/gcc/bin/http_demo_client" ]]; then
  demo_client_bin="./build/release/gcc/bin/http_demo_client"
fi
if [[ -f "./build/release/clang/bin/http_demo_client" ]]; then
  demo_client_bin=${demo_client_bin:-"./build/release/clang/bin/http_demo_client"}
fi
if [[ -f "./build/debug/gcc/bin/http_demo_client" ]]; then
  demo_client_bin=${demo_client_bin:-"./build/debug/gcc/bin/http_demo_client"}
fi
if [[ -f "./build/debug/clang/bin/http_demo_client" ]]; then
  demo_client_bin=${demo_client_bin:-"./build/debug/clang/bin/http_demo_client"}
fi

${demo_client_bin} --port=8080 --target=/echo --method=POST --data="Hello World"
echo "Stopping web server..."
pkill -f echo.py
wait
