import os

#The Sarama root directory
TESTS_DIR = os.path.join("/opt", "sarama")


class SaramaHelper:
    """
    The base class for sarama example helpers
    """
    def __init__(self, redpanda):
        #Instance of redpanda
        self._redpanda = redpanda

        #The result of the internal condiiton.
        #The internal condition is defined in the children.
        self._condition_met = False

    #Calls the internal condition and
    #automatically stores the result
    def condition(self, line):
        self._condition_met = self._condition(line)

    #Was the internal condition met?
    def condition_met(self):
        return self._condition_met

    #Set the name of the node assigned to
    #this example.
    def set_node_name(self, node_name):
        #Noop by default since some examples
        #don't need this
        pass


class SaramaInterceptorsHelper(SaramaHelper):
    """
    The helper class for Sarama's interceptors example
    """
    def __init__(self, redpanda, topic):
        super(SaramaInterceptorsHelper, self).__init__(redpanda)

        #The kafka topic
        self._topic = topic

    #The internal condition to determine if the
    #example is successful. Returns boolean.
    def _condition(self, line):
        return 'SpanContext' in line

    #Return the command to call in the shell
    def cmd(self):
        EXAMPLE_DIR = os.path.join(TESTS_DIR, "examples/interceptors")
        cmd = f"interceptors -brokers {self._redpanda.brokers()} -topic {self._topic}"
        return os.path.join(EXAMPLE_DIR, cmd)

    #Return the process name to kill
    def process_to_kill(self):
        return "interceptors"


class SaramaHttpServerHelper(SaramaHelper):
    """
    The helper class for Sarama's http server example
    """
    def __init__(self, redpanda):
        super(SaramaHttpServerHelper, self).__init__(redpanda)

        #The name of the node assigned to this example
        self._node_name = ""

    #The internal condition to determine if the
    #example is successful. Returns boolean.
    def _condition(self, line):
        return 'Listening for requests' in line

    #Return the command to call in the shell
    def cmd(self):
        EXAMPLE_DIR = os.path.join(TESTS_DIR, "examples/http_server")
        cmd = f"http_server -addr {self._node_name}:8080 -brokers {self._redpanda.brokers()}"
        return os.path.join(EXAMPLE_DIR, cmd)

    #Return the process name to kill
    def process_to_kill(self):
        return "http_server"

    #What is the name of the node
    #assigned to this example?
    def node_name(self):
        return self._node_name

    #Set the name of the node assigned to
    #this example.
    def set_node_name(self, node_name):
        self._node_name = node_name


class SaramaConsumerGroupHelper(SaramaHelper):
    """
    The helper class for Sarama's consumergroup example
    """
    def __init__(self, redpanda, topic):
        super(SaramaConsumerGroupHelper, self).__init__(redpanda)

        #The kafka topic
        self._topic = topic

    #The internal condition to determine if the
    #example is successful. Returns boolean.
    def _condition(self, line):
        return 'Message claimed:' in line

    #Return the command to call in the shell
    def cmd(self):
        EXAMPLE_DIR = os.path.join(TESTS_DIR, "examples/consumergroup")
        cmd = f"consumer -brokers=\"{self._redpanda.brokers()}\" -topics=\"{self._topic}\" -group=\"example\""
        return os.path.join(EXAMPLE_DIR, cmd)

    #Return the process name to kill
    def process_to_kill(self):
        return "consumer"


#A factory method to produce the command to run
#Sarama's SASL/SCRAM authentication example.
#Here, we do not create a SaramaHelper because
#the SASL/SCRAM example runs in the foreground.
def sarama_sasl_scram(redpanda, topic):
    EXAMPLE_DIR = os.path.join(TESTS_DIR, "examples/sasl_scram_client")
    creds = redpanda.SUPERUSER_CREDENTIALS
    cmd = f"sasl_scram_client -brokers {redpanda.brokers()} -username {creds[0]} -passwd {creds[1]} -topic {topic} -algorithm sha256"

    return os.path.join(EXAMPLE_DIR, cmd)


#A factory method to create the necessary helper class
#determined by the test function name
def create_helper(func_name, redpanda, topic):
    if func_name == "test_sarama_interceptors":
        return SaramaInterceptorsHelper(redpanda, topic)
    elif func_name == "test_sarama_http_server":
        return SaramaHttpServerHelper(redpanda)
    elif func_name == "test_sarama_consumergroup":
        return SaramaConsumerGroupHelper(redpanda, topic)
    else:
        raise RuntimeError("create_helper failed: Invalid function name")
