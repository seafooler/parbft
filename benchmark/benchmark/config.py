from json import dump, load


class ConfigError(Exception):
    pass


class BenchParameters:
    def __init__(self, json):
        try:
            nodes = json['nodes']
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes or any(x <= 0 for x in nodes):
                raise ConfigError('Missing or invalid number of nodes')

            rate = json['rate']
            rate = rate if isinstance(rate, list) else [rate]
            if not rate:
                raise ConfigError('Missing input rate')

            self.nodes = [int(x) for x in nodes]
            self.rate = [int(x) for x in rate]
            self.tx_size = int(json['tx_size'])
            self.faults = int(json['faults'])
            self.duration = int(json['duration'])
            self.runs = int(json['runs']) if 'runs' in json else 1
        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')

        if min(self.nodes) <= self.faults:
            raise ConfigError('There should be more nodes than faults')