from os import error
from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os.path import basename, splitext
from time import sleep
from math import ceil
from os.path import join
import subprocess

from benchmark.config import BenchParameters, ConfigError
from benchmark.utils import BenchError, Print, PathMaker, progress_bar
from benchmark.commands import CommandMaker
from benchmark.logs import LogParser, ParseError
from aws.instance import InstanceManager


class FabricError(Exception):
    ''' Wrapper for Fabric exception with a meaningfull error message. '''

    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[-1]
        super().__init__(message)


class ExecutionError(Exception):
    pass


class Bench:
    def __init__(self, ctx):
        self.manager = InstanceManager.make()
        self.settings = self.manager.settings
        try:
            Print.heading(f'self.manager.settings.key_path {self.manager.settings.key_path}')
            ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                self.manager.settings.key_path+"/"+self.manager.settings.key_name+'.pem'
            )
            self.connect = ctx.connect_kwargs
        except (IOError, PasswordRequiredException, SSHException) as e:
            raise BenchError('Failed to load SSH key', e)

    def _check_stderr(self, output):
        if isinstance(output, dict):
            for x in output.values():
                if x.stderr:
                    raise ExecutionError(x.stderr)
        else:
            if output.stderr:
                raise ExecutionError(output.stderr)

    def install(self):
        Print.info('Installing golang and cloning the repo...')
        cmd = [
            'sudo apt-get update',
            'sudo apt-get -y upgrade',
            'sudo apt-get -y autoremove',

            # # The following dependencies prevent the error: [error: linker `cc` not found].
            # 'sudo apt-get -y install build-essential',
            # 'sudo apt-get -y install cmake',
            #
            # Install golang
            'wget https://go.dev/dl/go1.18.linux-amd64.tar.gz',
            'tar -zxvf go1.18.linux-amd64.tar.gz',
            'sudo mv go /usr/local',
            'echo \'export PATH=$PATH:/usr/local/go/bin\' >> ~/.bashrc',
            'source ~/.bashrc',

            # Clone the repo.
            f'(git clone {self.settings.repo_url} || (cd {self.settings.repo_name} ; git pull))'
        ]
        hosts = self.manager.hosts(flat=True)
        try:
            g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
            Print.heading(f'Initialized testbed of {len(hosts)} nodes')
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to install repo on testbed', e)

    def kill(self, hosts=[], delete_logs=False):
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        hosts = hosts if hosts else self.manager.hosts(flat=True)
        delete_logs = CommandMaker.clean_logs() if delete_logs else 'true'
        cmd = [delete_logs, f'({CommandMaker.kill()} || true)']
        try:
            g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
        except GroupException as e:
            raise BenchError('Failed to kill nodes', FabricError(e))

    def _select_hosts(self, bench_parameters):
        nodes = max(bench_parameters.nodes)

        # Ensure there are enough hosts.
        hosts = self.manager.hosts()
        if sum(len(x) for x in hosts.values()) < nodes:
            return []

        # Select the hosts in different data centers.
        ordered = zip(*hosts.values())
        ordered = [x for y in ordered for x in y]
        return ordered[:nodes]

    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
        output = c.run(cmd, hide=True)
        self._check_stderr(output)

    def _update(self, hosts):
        Print.info(
            f'Updating {len(hosts)} nodes (branch "{self.settings.branch}")...'
        )
        cmd = [
            f'(cd {self.settings.repo_name} && git fetch -f)',
            f'(cd {self.settings.repo_name} && git checkout -f {self.settings.branch})',
            f'(cd {self.settings.repo_name} && git pull -f)',
            f'(cd {self.settings.repo_name} && {CommandMaker.compile()})',
            CommandMaker.alias_binaries(
                f'./{self.settings.repo_name}/'
            )
        ]
        g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
        g.run(' && '.join(cmd), hide=True)

    def _config(self, hosts, delay):
        Print.info('Generating configuration files...')
        # Cleanup all local configuration files.
        cmd = CommandMaker.cleanup()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        ips_elem = "\nid_ip: \n"
        for i in range(len(hosts)):
            ips_elem += ("  " + str(i) + ": " + hosts[i] + "\n")

        subprocess.run(['pwd'], shell=True, stderr=subprocess.DEVNULL)

        delay_elem = "mock_latency: " + str(delay) + "\n"

        with open('/vagrant/parbft/benchmark/config_temp.yaml', 'a') as f:
            f.write(ips_elem)
            f.write(delay_elem)

        Print.heading(f'hosts: {hosts}')

        cmd = 'go run /vagrant/parbft/config_gen/main.go'
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Cleanup all nodes.
        cmd = f'{CommandMaker.cleanup()} || true'
        g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
        g.run(cmd, hide=True)

        # Upload configuration files.
        progress = progress_bar(hosts, prefix='Uploading config files:')
        for i, host in enumerate(progress):
            c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
            c.put(PathMaker.config_file(i), './config.yaml')

        return

    def _run_single(self, hosts, bench_parameters):
        Print.info('Booting testbed...')

        # Kill any potentially unfinished run and delete logs.
        self.kill(hosts=hosts, delete_logs=True)

        Print.info('Killed previous instances')
        sleep(10)

        # Run the nodes.
        node_logs = [PathMaker.node_log_file(i) for i in range(len(hosts))]
        for host, log_file in zip(hosts, node_logs):
            cmd = CommandMaker.run_node()
            self._background_run(host, cmd, log_file)

        # Wait for the nodes to synchronize
        Print.info('Waiting for the nodes to synchronize...')
        sleep(bench_parameters.timeout_delay / 1000)

        # Wait for all transactions to be processed.
        duration = bench_parameters.duration
        for _ in progress_bar(range(100), prefix=f'Running benchmark ({duration} sec):'):
            sleep(ceil(duration / 100))
        self.kill(hosts=hosts, delete_logs=False)

    def _logs(self, hosts, faults):
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Download log files.
        progress = progress_bar(hosts, prefix='Downloading logs:')
        for i, host in enumerate(progress):
            c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
            c.get(PathMaker.node_log_file(i), local=PathMaker.node_log_file(i))

        # Parse logs and return the parser.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process(PathMaker.logs_path(), faults=faults)

    def run(self, bench_parameters_dict, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting remote benchmark')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        # Select which hosts to use.
        selected_hosts = self._select_hosts(bench_parameters)
        if not selected_hosts:
            Print.warn('There are not enough instances available')
            return

        # Update nodes.
        try:
            self._update(selected_hosts)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to update nodes', e)


        Print.info('Running ParBFT')

        Print.info(f'{bench_parameters.faults} faults')

        hosts = selected_hosts[:bench_parameters.nodes[0]]

        # Run benchmarks.
        for n in bench_parameters.nodes:
            hosts = selected_hosts[:n]

            # Do not boot faulty nodes.
            faults = bench_parameters.faults
            hosts = hosts[:n-faults]

            # Run the benchmark.
            for d in bench_parameters.mock_latencies:
                # Upload all configuration files.
                # try:
                #     self._config(hosts, d)
                #     # self._config()
                # except (subprocess.SubprocessError, GroupException) as e:
                #     e = FabricError(e) if isinstance(e, GroupException) else e
                #     Print.error(BenchError('Failed to configure nodes', e))

                boundary = "------------------- " + str(d) + "ms -------------------"
                with open('/vagrant/parbft/benchmark/results/bench-16-0.txt', 'a') as f:
                    f.write(boundary)

                for i in range(bench_parameters.runs):
                    Print.heading(f'Run {i+1}/{bench_parameters.runs}')
                    try:
                        self._run_single(
                            hosts, bench_parameters
                        )
                        self._logs(hosts, faults).print(PathMaker.result_file(
                            n, faults
                        ))
                    except (subprocess.SubprocessError, GroupException, ParseError) as e:
                        self.kill(hosts=hosts)
                        if isinstance(e, GroupException):
                            e = FabricError(e)
                        Print.error(BenchError('Benchmark failed', e))
                        continue
