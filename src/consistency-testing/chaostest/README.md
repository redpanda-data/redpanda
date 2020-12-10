# Setup

Start a cluster `vtools_dev_cluster --provider aws`

Let the addresses are (public ip / private ip)

  export server1_ext=54.187.163.147 server1_int=172.31.23.172
  export server2_ext=54.202.119.176 server2_int=172.31.19.127
  export server3_ext=54.200.109.158 server3_int=172.31.19.227
  
  export prometheus_ext=35.161.221.166

Start a control node, let address is

  export control_ext=34.221.236.57 control_int=172.31.19.127

Update ip-addresses of this document and test configs

    for system in kafka redpanda; do
        for workflow in comrmw mrsw; do
            for mode in int ext; do
                sed -i'' "s/427.0.0.1/$server1_int/" src/consistency-testing/chaostest/test-plan-templates/$system.$workflow.$mode.json
                sed -i'' "s/427.0.0.2/$server2_int/" src/consistency-testing/chaostest/test-plan-templates/$system.$workflow.$mode.json
                sed -i'' "s/427.0.0.3/$server3_int/" src/consistency-testing/chaostest/test-plan-templates/$system.$workflow.$mode.json
                sed -i'' "s/527.0.0.1/$control_int/" src/consistency-testing/chaostest/test-plan-templates/$system.$workflow.$mode.json
            done
        done
    done

    for workflow in comrmw mrsw; do
        sed -i'' "s/427.0.0.1/$server1_int/" src/consistency-testing/chaostest/test-plan-templates/kvelldb.$workflow.json
        sed -i'' "s/427.0.0.2/$server2_int/" src/consistency-testing/chaostest/test-plan-templates/kvelldb.$workflow.json
        sed -i'' "s/427.0.0.3/$server3_int/" src/consistency-testing/chaostest/test-plan-templates/kvelldb.$workflow.json
    done

Stop redpanda & prometheus:

    for ip in $server1_ext $server2_ext $server3_ext; do 
      ssh -i "~/.ssh/vectorized/deployments/aws-cluster" ubuntu@$ip sudo systemctl stop redpanda
    done

    ssh -i "~/.ssh/vectorized/deployments/aws-cluster" ubuntu@$prometheus_ext sudo systemctl stop prometheus

Copy control and `iofaults` to the kvelldb / redpanda nodes:

    for ip in $server1_ext $server2_ext $server3_ext; do 
      scp -i "~/.ssh/vectorized/deployments/aws-cluster" -r src/consistency-testing/chaostest/control ubuntu@$ip:
      scp -i "~/.ssh/vectorized/deployments/aws-cluster" -r src/consistency-testing/iofaults/iofaults.py ubuntu@$ip:
    done

Install dependencies

    for ip in $server1_ext $server2_ext $server3_ext; do 
      ssh -i "~/.ssh/vectorized/deployments/aws-cluster" ubuntu@$ip sudo apt-get -y install python3-pip kafkacat
      ssh -i "~/.ssh/vectorized/deployments/aws-cluster" ubuntu@$ip sudo pip3 install flask fusepy kafka-python argparse
      ssh -i "~/.ssh/vectorized/deployments/aws-cluster" ubuntu@$ip "cd /opt; sudo wget https://downloads.apache.org/zookeeper/zookeeper-3.6.2/apache-zookeeper-3.6.2-bin.tar.gz"
      ssh -i "~/.ssh/vectorized/deployments/aws-cluster" ubuntu@$ip "cd /opt; sudo tar xzf apache-zookeeper-3.6.2-bin.tar.gz"
      ssh -i "~/.ssh/vectorized/deployments/aws-cluster" ubuntu@$ip "cd /opt; sudo rm apache-zookeeper-3.6.2-bin.tar.gz"
      ssh -i "~/.ssh/vectorized/deployments/aws-cluster" ubuntu@$ip "cd /opt; sudo wget https://downloads.apache.org/kafka/2.6.0/kafka_2.12-2.6.0.tgz"
      ssh -i "~/.ssh/vectorized/deployments/aws-cluster" ubuntu@$ip "cd /opt; sudo tar xzf kafka_2.12-2.6.0.tgz"
      ssh -i "~/.ssh/vectorized/deployments/aws-cluster" ubuntu@$ip "cd /opt; sudo rm kafka_2.12-2.6.0.tgz"
    done

Copy keys to a control node

    scp ~/.ssh/vectorized/deployments/aws-cluster ubuntu@$control_ext:

Copy `gobekli` and `chaostest` to the control node

    scp -r src/consistency-testing/gobekli ubuntu@$control_ext:
    scp -r src/consistency-testing/chaostest ubuntu@$control_ext:

Install dependencies

    ssh ubuntu@$control_ext sudo apt-get update
    ssh ubuntu@$control_ext sudo apt-get build-essential python3-pip
    ssh ubuntu@$control_ext sudo pip3 install sh


Install gobekli

    sudo apt-get update
    sudo apt-get install build-essential
    sudo apt install python3-pip
    sudo pip3 install sh
    ssh ubuntu@$control_ext "cd gobekli; sudo python3 setup.py develop"

Check that control can connect to kvelldb nodes

    ssh ubuntu@$control_ext
    for ip in $server1_int $server2_int $server3_int; do ssh -i /home/ubuntu/aws-cluster ubuntu@$ip touch foo; done

Open zk ports in firewall: 2888 3888 2181

Run the tests

    Individual tests

        python3 test-kafka.py example.kafka.comrmw.json --override 'faults=["baseline"]'
        python3 test-redpanda.py example.redpanda.comrmw.json --override 'faults=["baseline"]'
        python3 test-kvelldb.py example.kvelldb.comrmw.json --override 'faults=["baseline"]'

    The whole suite

        python3 test-kafka.py example.kafka.comrmw.json
        python3 test-redpanda.py example.redpanda.comrmw.json
        python3 test-kvelldb.py example.kvelldb.comrmw.json
