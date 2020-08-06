# KvellDB

## How to run

```bash
mkdir node0 node1 node2
./kvelldb --ip 127.0.0.1 --workdir node0 --node-id 0 --port 11214 --httpport 10214 --peers 1,127.0.0.1:11215 --peers 2,127.0.0.1:11216 --cpus 1 2>&1 | tee kvell0.log
./kvelldb --ip 127.0.0.1 --workdir node1 --node-id 1 --port 11215 --httpport 10215 --peers 0,127.0.0.1:11214 --peers 2,127.0.0.1:11216 --cpus 1 2>&1 | tee kvell1.log
./kvelldb --ip 127.0.0.1 --workdir node2 --node-id 2 --port 11216 --httpport 10216 --peers 0,127.0.0.1:11214 --peers 1,127.0.0.1:11215 --cpus 1 2>&1 | tee kvell2.log
```

## How to test

```bash

curl -v http://127.0.0.1:10214/read?key=key0

curl -v --header "Content-Type: application/json" \
     --request POST \
     --data '{"key":"key0", "value":"value0", "writeID": "j3qq4"}' \
     http://127.0.0.1:10214/write

curl -v http://127.0.0.1:10214/read?key=key0

curl -v --header "Content-Type: application/json" \
     --request POST \
     --data '{"key":"key0", "prevWriteID": "j3qq4", "value":"value0", "writeID": "h7h2v"}' \
     http://127.0.0.1:10214/cas

curl -v http://127.0.0.1:10214/read?key=key0
```