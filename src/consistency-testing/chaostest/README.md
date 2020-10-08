# Setup

Start a cluster `vtools_dev_cluster --provider aws`

Let the addresses are:

44.234.58.114 / 172.31.32.56
44.234.105.210 / 172.31.42.158
34.223.82.229 / 172.31.40.28

- public ip / private ip
- 44.234.58.114 / 172.31.32.56 (2)
- 44.234.105.210 / 172.31.42.158 (1)
- 34.223.82.229 / 172.31.40.28 (0)

Stop redpanda there:

    for ip in 44.234.58.114 44.234.45.247 44.234.105.210 34.223.82.229; do 
      echo $ip
      ssh -i "~/.ssh/vectorized/deployments/aws-cluster" admin@$ip sudo systemctl stop redpanda
    done

Copy `kvelldb-control` and `iofaults` to the kvelldb / redpanda nodes:

    for ip in 44.234.58.114 44.234.105.210 34.223.82.229; do 
      scp -i "~/.ssh/vectorized/deployments/aws-cluster" -r src/consistency-testing/chaostest/control admin@$ip:
      scp -i "~/.ssh/vectorized/deployments/aws-cluster" -r src/consistency-testing/iofaults/iofaults.py admin@$ip:
    done

Install dependencies

    for ip in 44.234.58.114 44.234.105.210 34.223.82.229; do 
      ssh -i "~/.ssh/vectorized/deployments/aws-cluster" admin@$ip sudo apt-get -y install python3-pip kafkacat
      ssh -i "~/.ssh/vectorized/deployments/aws-cluster" admin@$ip sudo pip3 install flask fusepy kafka-python argparse
    done

Update `*.settings.json` files

Start a control node

Copy keys to a control node

    scp ~/.ssh/vectorized/deployments/aws-cluster ubuntu@34.222.184.68:

Copy `gobekli` and `chaostest` to the control node

    scp -r src/consistency-testing/gobekli ubuntu@34.222.184.68:
    scp -r src/consistency-testing/chaostest ubuntu@34.222.184.68:

Install gobekli

    ssh ubuntu@34.222.184.68 "cd gobekli; sudo python3.7 setup.py develop"

Check that control can connect to kvelldb nodes

    ssh ubuntu@34.222.184.68
    for ip in 172.31.32.56 172.31.42.158 172.31.40.28; do ssh -i /home/ubuntu/aws-cluster admin@$ip; done

Run the tests

    ssh ubuntu@34.222.184.68 "cd chaostest; rm -rf results; mkdir results; python3.7 test-kvelldb.py example.settings.json"