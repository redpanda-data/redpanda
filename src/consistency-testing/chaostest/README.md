# Setup

Start a cluster `vtools_dev_cluster --provider aws`

Let the addresses are (public ip / private ip)
    
    54.191.185.137 / 172.31.17.252
    54.201.237.117 / 172.31.17.183
    35.160.0.244 / 172.31.31.215

Stop redpanda there:

    for ip in 54.191.185.137 54.201.237.117 35.160.0.244; do 
      echo $ip
      ssh -i "~/.ssh/vectorized/deployments/aws-cluster" admin@$ip sudo systemctl stop redpanda
    done

Copy control and `iofaults` to the kvelldb / redpanda nodes:

    for ip in 54.191.185.137 54.201.237.117 35.160.0.244; do 
      scp -i "~/.ssh/vectorized/deployments/aws-cluster" -r src/consistency-testing/chaostest/control admin@$ip:
      scp -i "~/.ssh/vectorized/deployments/aws-cluster" -r src/consistency-testing/iofaults/iofaults.py admin@$ip:
    done

Install dependencies

    for ip in 54.191.185.137 54.201.237.117 35.160.0.244; do 
      ssh -i "~/.ssh/vectorized/deployments/aws-cluster" admin@$ip sudo apt-get -y install python3-pip kafkacat
      ssh -i "~/.ssh/vectorized/deployments/aws-cluster" admin@$ip sudo pip3 install flask fusepy kafka-python argparse
    done

Update `*.settings.json` files

Start a control node - 34.221.236.57

Check that it's python version >= 3.7

Copy keys to a control node

    scp ~/.ssh/vectorized/deployments/aws-cluster ubuntu@34.221.236.57:

Copy `gobekli` and `chaostest` to the control node

    scp -r src/consistency-testing/gobekli ubuntu@34.221.236.57:
    scp -r src/consistency-testing/chaostest ubuntu@34.221.236.57:

Install dependencies

    ssh ubuntu@34.221.236.57 sudo apt-get update
    ssh ubuntu@34.221.236.57 sudo apt-get build-essential python3-pip
    ssh ubuntu@34.221.236.57 sudo pip3 install sh


Install gobekli

    ssh ubuntu@34.222.184.68 "cd gobekli; sudo python3 setup.py develop"

Check that control can connect to kvelldb nodes

    ssh ubuntu@34.222.184.68
    for ip in 172.31.17.252 172.31.17.183 172.31.31.215; do ssh -i /home/ubuntu/aws-cluster admin@$ip; done

Run the tests

    python3 test-redpanda.py example.redpanda.mrsw.json --override verbose=true --override 'faults=["baseline"]'