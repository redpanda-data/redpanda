# Setup

Start a cluster `vtools_dev_cluster --provider aws`

Let the addresses are:

- public ip / private ip
- 34.223.65.155 / 172.31.38.96
- 44.234.116.254 / 172.31.37.22
- 44.229.42.78 / 172.31.35.89

Stop redpanda there:

    for ip in 34.223.65.155 44.234.116.254 44.229.42.78; do 
      ssh -i "~/.ssh/vectorized/deployments/aws-cluster" admin@$ip sudo systemctl stop redpanda
    done

Copy `kvelldb-control` and `iofaults` to the kvelldb nodes:

    for ip in 34.223.65.155 44.234.116.254 44.229.42.78; do 
      scp -i "~/.ssh/vectorized/deployments/aws-cluster" -r src/consistency-testing/chaostest/kvelldb-control admin@$ip:
      scp -i "~/.ssh/vectorized/deployments/aws-cluster" -r src/consistency-testing/iofaults/iofaults.py admin@$ip:
    done

Install dependencies

    for ip in 34.223.65.155 44.234.116.254 44.229.42.78; do 
      ssh -i "~/.ssh/vectorized/deployments/aws-cluster" admin@$ip sudo apt-get -y install python3-pip
      ssh -i "~/.ssh/vectorized/deployments/aws-cluster" admin@$ip sudo pip3 install flask fusepy
    done

Update `example.settings.json`

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
    for ip in 172.31.38.96 172.31.37.22 172.31.35.89; do ssh -i /home/ubuntu/aws-cluster admin@$ip; done

Run the tests

    ssh ubuntu@34.222.184.68 "cd chaostest; rm -rf results; mkdir results; python3.7 test-kvelldb.py example.settings.json"

    or

    ssh ubuntu@34.222.184.68 "cd chaostest; rm -rf results; mkdir results; python3.7 test-kvelldb.py example.settings.json --verbose"