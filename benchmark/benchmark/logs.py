# encoding=UTF-8

from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean

from utils import Print
class ParseError(Exception):
    pass


class LogParser:
    def __init__(self, nodes, faults, protocol, ddos):
        inputs = [nodes]
        ##nodes 是一个[, , , ,] 包含了4个节点file的所有信息
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.protocol = protocol
        self.ddos = ddos
        self.faults = faults
        self.committee_size = len(nodes) + faults

        # Parse the nodes logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_nodes, nodes)
        except (ValueError, IndexError) as e:
            print('Failed to parse node logs:', {e})
        # proposals, commits, sizes, self.received_samples, timeouts, self.configs \
        proposals, commits \
            = zip(*results)

        #到这里得到的commits是4个node-* file的commit信息的字典组成的tuple: ({},{},{},{})
        self.proposals = self._merge_nodes_proposal([x.items() for x in proposals])
        self.commits = self._merge_nodes_commit([x.items() for x in commits])

        #到这里merge后就得到了一个大字典
    def _merge_nodes_proposal(self, input):
        #单纯的为了合并4个dict，可以偷偷把commit时间算成最早的,同时我们忽略前两个块
        merged = {}
        for x in input:
            for k, t in x:
                if not k in merged or merged[k] > t:
                    merged[k] = t
        if '0' in merged.keys():
            merged.pop('0')
        if '1' in merged.keys():
            merged.pop('1')

        return merged
    def _merge_nodes_commit(self, input):
        #单纯的为了合并4个dict，可以偷偷把commit时间算成最早的,同时我们忽略前两个块
        merged = {}
        for x in input:
            for k, (t,n) in x:
                if not k in merged or merged[k][0] > t:
                    merged[k] = (t,n)
        if '0' in merged.keys():
            merged.pop('0')
        if '1' in merged.keys():
            merged.pop('1')
        return merged


    def _parse_nodes(self, log):
        ##有几个node就会被调用几遍
        tmp = findall(r'(.*Z).*successfully broadcast.*height=(.*)',log) #[('2023-03-04T02:01:33.655Z', '8'), ('2023-03-04T02:01:33.799Z', '9'), ('2023-03-04T02:01:41.643Z', '63')]
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results_proposal(tmp)

        tmp = findall(r'(.*Z).*Commit a block in Bolt.*block_index=(.*) tx_num=(.*)',log)
        tmp = [(d, (self._to_posix(t), n)) for t, d, n in tmp]
        commits = self._merge_results_commit(tmp)

        return proposals, commits#, sizes, samples, timeouts, configs

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _merge_results_proposal(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for k,t in input:
            if not k in merged or merged[k] > t:
                merged[k] = t
        return merged

    def _merge_results_commit(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for k, (t, n) in input:
            if not k in merged or merged[k][0] > t:
                merged[k] = (t,n)
        return merged


    def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(t for t,_ in self.commits.values())
        duration = end - start
        #bytes = sum(self.sizes.values())
        #bps = bytes / duration
        tps = sum(int(n) for d,(t,n) in self.commits.items()) / duration
        return tps, duration

    def _consensus_latency(self):
        # print("lantency PRINT")
        # print(self.commits)
        latency = [t - self.proposals[d] for d, (t,_) in self.commits.items()]
        return mean(latency) if latency else 0

    # def result_trial(self):
    #     return sum(int(n) for d,(t,n) in self.commits.items()) 

    def result(self):
        consensus_latency = self._consensus_latency() * 1000
        consensus_tps, duration = self._consensus_throughput()

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Protocol: {self.protocol} \n'
            f' DDOS attack: {self.ddos} \n'
            f' Committee size: {self.committee_size} nodes\n'
            # f' Input rate: {sum(self.rate):,} tx/s\n'
            # f' Transaction size: {self.size[0]:,} B\n'
            f' Faults: {self.faults} nodes\n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            #f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    @classmethod
    def process(cls, directory, faults=0, protocol=0, ddos=False):
        assert isinstance(directory, str)
        nodes = []
        for filename in sorted(glob(join(directory, 'node-*.log'))):
            with open(filename, 'r') as f:
                nodes += [f.read()]
        #也许是为了代码简洁，其实这个才是真正的构造函数。__init__被调用了2遍，第一遍是废物
        return cls(nodes, faults=faults, protocol=protocol, ddos=ddos)