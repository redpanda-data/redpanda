class KafkaServiceAdapter:
    '''
        Simple adapter to match KafkaService interface with 
        what is required by Redpanda test clients
    '''
    def __init__(self, test_context, kafka_service):
        self._context = test_context
        self._kafka_service = kafka_service

    @property
    def logger(self):
        return self._kafka_service.logger

    def brokers(self):
        return self._kafka_service.bootstrap_servers()

    def start(self):
        return self._kafka_service.start()

    def start(self, add_principals=""):
        return self._kafka_service.start(add_principals)

    # required for rpk
    def find_binary(self, name):
        rp_install_path_root = self._context.globals.get(
            "rp_install_path_root", None)
        return f"{rp_install_path_root}/bin/{name}"
