from logging import exception


class Mapping:
    def __init__(self, type_name, mapping):
        self.mapping = mapping
        self.type_name = type_name

    def decode(self, key):
        if key in self.mapping:
            return self.mapping[key]

        raise Exception('unknown mapping {self.name} - {key}')

    def encode(self, key_str):
        for value, name in self.mapping.items():
            if name == key_str:
                return value

        raise Exception(f"unknown {self.type_name} - {key_str}")
