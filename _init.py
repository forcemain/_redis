#! -*- coding: utf-8 -*-


import os
import yaml


from redis import StrictRedis
from functools import partial
from agent.util.enhance import Switch
from agent.metrics.baseloader import BaseLoader
from agent.metrics.basemetric import BaseMetric
from agent.metrics.metric_data import MetricData
from agent.metrics.basecollect import BaseCollector


class Redis(BaseMetric):
    def __init__(self, redis_aof_last_rewrite_time=None, redis_aof_rewrite=None, redis_clients_biggest_input_buf=None,
                 redis_clients_blocked=None, redis_clients_longest_output_list=None, redis_cpu_sys=None,
                 redis_cpu_sys_children=None, redis_cpu_user=None, redis_cpu_user_children=None,
                 redis_info_latency_ms=None, redis_keys_evicted=None, redis_keys_expired=None,
                 redis_mem_fragmentation_ratio=None, redis_mem_lua=None, redis_mem_rss=None, redis_mem_used=None,
                 redis_net_clients=None, redis_net_commands=None, redis_net_rejected=None, redis_net_slaves=None,
                 redis_perf_latest_fork_usec=None, redis_pubsub_channels=None, redis_rdb_bgsave=None,
                 redis_rdb_changes_since_last=None, redis_rdb_last_bgsave_time=None,
                 redis_replication_master_repl_offs=None, redis_stats_keyspace_hits=None,
                 redis_stats_keyspace_misses=None):
        self.redis_aof_last_rewrite_time = redis_aof_last_rewrite_time
        self.redis_aof_rewrite = redis_aof_rewrite
        self.redis_clients_biggest_input_buf = redis_clients_biggest_input_buf
        self.redis_clients_blocked = redis_clients_blocked
        self.redis_clients_longest_output_list = redis_clients_longest_output_list
        self.redis_cpu_sys = redis_cpu_sys
        self.redis_cpu_sys_children = redis_cpu_sys_children
        self.redis_cpu_user = redis_cpu_user
        self.redis_cpu_user_children = redis_cpu_user_children
        self.redis_info_latency_ms = redis_info_latency_ms
        self.redis_keys_evicted = redis_keys_evicted
        self.redis_keys_expired = redis_keys_expired
        self.redis_mem_fragmentation_ratio = redis_mem_fragmentation_ratio
        self.redis_mem_lua = redis_mem_lua
        self.redis_mem_rss = redis_mem_rss
        self.redis_mem_used = redis_mem_used
        self.redis_net_clients = redis_net_clients
        self.redis_net_commands = redis_net_commands
        self.redis_net_rejected = redis_net_rejected
        self.redis_net_slaves = redis_net_slaves
        self.redis_perf_latest_fork_usec = redis_perf_latest_fork_usec
        self.redis_pubsub_channels = redis_pubsub_channels
        self.redis_rdb_bgsave = redis_rdb_bgsave
        self.redis_rdb_changes_since_last = redis_rdb_changes_since_last
        self.redis_rdb_last_bgsave_time = redis_rdb_last_bgsave_time
        self.redis_replication_master_repl_offs = redis_replication_master_repl_offs
        self.redis_stats_keyspace_hits = redis_stats_keyspace_hits
        self.redis_stats_keyspace_misses = redis_stats_keyspace_misses

    def get_redis_aof_last_rewrite_time(self):
        return self.redis_aof_last_rewrite_time

    def set_redis_aof_last_rewrite_time(self, redis_aof_last_rewrite_time):
        self.redis_aof_last_rewrite_time = redis_aof_last_rewrite_time

    def get_redis_aof_rewrite(self):
        return self.redis_aof_rewrite

    def set_redis_aof_rewrite(self, redis_aof_rewrite):
        self.redis_aof_rewrite = redis_aof_rewrite

    def get_redis_clients_biggest_input_buf(self):
        return self.redis_clients_biggest_input_buf

    def set_redis_clients_biggest_input_buf(self, redis_clients_biggest_input_buf):
        self.redis_clients_biggest_input_buf = redis_clients_biggest_input_buf

    def get_redis_clients_blocked(self):
        return self.redis_clients_blocked

    def set_redis_clients_blocked(self, redis_clients_blocked):
        self.redis_clients_blocked = redis_clients_blocked

    def get_redis_clients_longest_output_list(self):
        return self.redis_clients_longest_output_list

    def set_redis_clients_longest_output_list(self, redis_clients_longest_output_list):
        self.redis_clients_longest_output_list = redis_clients_longest_output_list

    def get_redis_cpu_sys(self):
        return self.redis_cpu_sys

    def set_redis_cpu_sys(self, redis_cpu_sys):
        self.redis_cpu_sys = redis_cpu_sys

    def get_redis_cpu_sys_children(self):
        return self.redis_cpu_sys_children

    def set_redis_cpu_sys_children(self, redis_cpu_sys_children):
        self.redis_cpu_sys_children = redis_cpu_sys_children

    def get_redis_cpu_user(self):
        return self.redis_cpu_user

    def set_redis_cpu_user(self, redis_cpu_user):
        self.redis_cpu_user = redis_cpu_user

    def get_redis_cpu_user_children(self):
        return self.redis_cpu_user_children

    def set_redis_cpu_user_children(self, redis_cpu_user_children):
        self.redis_cpu_user_children = redis_cpu_user_children

    def get_redis_info_latency_ms(self):
        return self.redis_info_latency_ms

    def set_redis_info_latency_ms(self, redis_info_latency_ms):
        self.redis_info_latency_ms = redis_info_latency_ms

    def get_redis_keys_evicted(self):
        return self.redis_keys_evicted

    def set_redis_keys_evicted(self, redis_keys_evicted):
        self.redis_keys_evicted = redis_keys_evicted

    def get_redis_keys_expired(self):
        return self.redis_keys_expired

    def set_redis_keys_expired(self, redis_keys_expired):
        self.redis_keys_expired = redis_keys_expired

    def get_redis_mem_fragmentation_ratio(self):
        return self.redis_mem_fragmentation_ratio

    def set_redis_mem_fragmentation_ratio(self, redis_mem_fragmentation_ratio):
        self.redis_mem_fragmentation_ratio = redis_mem_fragmentation_ratio

    def get_redis_mem_lua(self):
        return self.redis_mem_lua

    def set_redis_mem_lua(self, redis_mem_lua):
        self.redis_mem_lua = redis_mem_lua

    def get_redis_mem_rss(self):
        return self.redis_mem_rss

    def set_redis_mem_rss(self, redis_mem_rss):
        self.redis_mem_rss = redis_mem_rss

    def get_redis_mem_used(self):
        return self.redis_mem_used

    def set_redis_mem_used(self, redis_mem_used):
        self.redis_mem_used = redis_mem_used

    def get_redis_net_clients(self):
        return self.redis_net_clients

    def set_redis_net_clients(self, redis_net_clients):
        self.redis_net_clients = redis_net_clients

    def get_redis_net_commands(self):
        return self.redis_net_commands

    def set_redis_net_commands(self, redis_net_commands):
        self.redis_net_commands = redis_net_commands

    def get_redis_net_rejected(self):
        return self.redis_net_rejected

    def set_redis_net_rejected(self, redis_net_rejected):
        self.redis_net_rejected = redis_net_rejected

    def get_redis_net_slaves(self):
        return self.redis_net_slaves

    def set_redis_net_slaves(self, redis_net_slaves):
        self.redis_net_slaves = redis_net_slaves

    def get_redis_perf_latest_fork_usec(self):
        return self.redis_perf_latest_fork_usec

    def set_redis_perf_latest_fork_usec(self, redis_perf_latest_fork_usec):
        self.redis_perf_latest_fork_usec = redis_perf_latest_fork_usec

    def get_redis_pubsub_channels(self):
        return self.redis_pubsub_channels

    def set_redis_pubsub_channels(self, redis_pubsub_channels):
        self.redis_pubsub_channels = redis_pubsub_channels

    def get_redis_rdb_bgsave(self):
        return self.redis_rdb_bgsave

    def set_redis_rdb_bgsave(self, redis_rdb_bgsave):
        self.redis_rdb_bgsave = redis_rdb_bgsave

    def get_redis_rdb_changes_since_last(self):
        return self.redis_rdb_changes_since_last

    def set_redis_rdb_changes_since_last(self, redis_rdb_changes_since_last):
        self.redis_rdb_changes_since_last = redis_rdb_changes_since_last

    def get_redis_rdb_last_bgsave_time(self):
        return self.redis_rdb_last_bgsave_time

    def set_redis_rdb_last_bgsave_time(self, redis_rdb_last_bgsave_time):
        self.redis_rdb_last_bgsave_time = redis_rdb_last_bgsave_time

    def get_redis_replication_master_repl_offs(self):
        return self.redis_replication_master_repl_offs

    def set_redis_replication_master_repl_offs(self, redis_replication_master_repl_offs):
        self.redis_replication_master_repl_offs = redis_replication_master_repl_offs

    def get_redis_stats_keyspace_hits(self):
        return self.redis_stats_keyspace_hits

    def set_redis_stats_keyspace_hits(self, redis_stats_keyspace_hits):
        self.redis_stats_keyspace_hits = redis_stats_keyspace_hits

    def get_redis_stats_keyspace_misses(self):
        return self.redis_stats_keyspace_misses

    def set_redis_stats_keyspace_misses(self, redis_stats_keyspace_misses):
        self.redis_stats_keyspace_misses = redis_stats_keyspace_misses

    def to_dict(self):
        data = {}
        if isinstance(self.get_redis_aof_last_rewrite_time(), MetricData):
            data['redis_aof_last_rewrite_time'] = self.get_redis_aof_last_rewrite_time().to_dict()
        if isinstance(self.get_redis_aof_rewrite(), MetricData):
            data['redis_aof_rewrite'] = self.get_redis_aof_rewrite().to_dict()
        if isinstance(self.get_redis_clients_biggest_input_buf(), MetricData):
            data['redis_clients_biggest_input_buf'] = self.get_redis_clients_biggest_input_buf().to_dict()
        if isinstance(self.get_redis_clients_blocked(), MetricData):
            data['redis_clients_blocked'] = self.get_redis_clients_blocked().to_dict()
        if isinstance(self.get_redis_clients_longest_output_list(), MetricData):
            data['redis_clients_longest_output_list'] = self.get_redis_clients_longest_output_list().to_dict()
        if isinstance(self.get_redis_cpu_sys(), MetricData):
            data['redis_cpu_sys'] = self.get_redis_cpu_sys().to_dict()
        if isinstance(self.get_redis_cpu_sys_children(), MetricData):
            data['redis_cpu_sys_children'] = self.get_redis_cpu_sys_children().to_dict()
        if isinstance(self.get_redis_cpu_user(), MetricData):
            data['redis_cpu_user'] = self.get_redis_cpu_user().to_dict()
        if isinstance(self.get_redis_cpu_user_children(), MetricData):
            data['redis_cpu_user_children'] = self.get_redis_cpu_user_children().to_dict()
        if isinstance(self.get_redis_info_latency_ms(), MetricData):
            data['redis_info_latency_ms'] = self.get_redis_info_latency_ms().to_dict()
        if isinstance(self.get_redis_keys_evicted(), MetricData):
            data['redis_keys_evicted'] = self.get_redis_keys_evicted().to_dict()
        if isinstance(self.get_redis_keys_expired(), MetricData):
            data['redis_keys_expired'] = self.get_redis_keys_expired().to_dict()
        if isinstance(self.get_redis_mem_fragmentation_ratio(), MetricData):
            data['redis_mem_fragmentation_ratio'] = self.get_redis_mem_fragmentation_ratio().to_dict()
        if isinstance(self.get_redis_mem_lua(), MetricData):
            data['redis_mem_lua'] = self.get_redis_mem_lua().to_dict()
        if isinstance(self.get_redis_mem_rss(), MetricData):
            data['redis_mem_rss'] = self.get_redis_mem_rss().to_dict()
        if isinstance(self.get_redis_mem_used(), MetricData):
            data['redis_mem_used'] = self.get_redis_mem_used().to_dict()
        if isinstance(self.get_redis_net_clients(), MetricData):
            data['redis_net_clients'] = self.get_redis_net_clients().to_dict()
        if isinstance(self.get_redis_net_commands(), MetricData):
            data['redis_net_commands'] = self.get_redis_net_commands().to_dict()
        if isinstance(self.get_redis_net_rejected(), MetricData):
            data['redis_net_rejected'] = self.get_redis_net_rejected().to_dict()
        if isinstance(self.get_redis_net_slaves(), MetricData):
            data['redis_net_slaves'] = self.get_redis_net_slaves().to_dict()
        if isinstance(self.get_redis_perf_latest_fork_usec(), MetricData):
            data['redis_perf_latest_fork_usec'] = self.get_redis_perf_latest_fork_usec().to_dict()
        if isinstance(self.get_redis_pubsub_channels(), MetricData):
            data['redis_pubsub_channels'] = self.get_redis_pubsub_channels().to_dict()
        if isinstance(self.get_redis_rdb_bgsave(), MetricData):
            data['redis_rdb_bgsave'] = self.get_redis_rdb_bgsave().to_dict()
        if isinstance(self.get_redis_rdb_changes_since_last(), MetricData):
            data['redis_rdb_changes_since_last'] = self.get_redis_rdb_changes_since_last().to_dict()
        if isinstance(self.get_redis_rdb_last_bgsave_time(), MetricData):
            data['redis_rdb_last_bgsave_time'] = self.get_redis_rdb_last_bgsave_time().to_dict()
        if isinstance(self.get_redis_replication_master_repl_offs(), MetricData):
            data['redis_replication_master_repl_offs'] = self.get_redis_replication_master_repl_offs().to_dict()
        if isinstance(self.get_redis_stats_keyspace_hits(), MetricData):
            data['redis_stats_keyspace_hits'] = self.get_redis_stats_keyspace_hits().to_dict()
        if isinstance(self.get_redis_stats_keyspace_misses(), MetricData):
            data['redis_stats_keyspace_misses'] = self.get_redis_stats_keyspace_misses().to_dict()

        return data

    def is_valid(self):
        return True


class Loader(BaseLoader):
    def __init__(self, conf=os.path.join(os.path.dirname(__file__), '_init.yaml')):
        with open(conf, 'r+b') as fd:
            self._conf = yaml.load(fd)

    @property
    def init_conf(self):
        return self._conf.get('init_conf') or {}

    @property
    def inst_conf(self):
        return self._conf.get('instances') or []

    @property
    def inst_with_tags(self):
        instances_list = []
        for kwargs in self.inst_conf:
            ins = StrictRedis(**kwargs['init'])
            instances_list.append((ins, kwargs['tags']))

        return instances_list


class Collector(BaseCollector):
    enable = False
    loader = Loader() if enable is True else None

    def get_inst_infos(self, inst):
        inst_infos = {}
        sections = ['Clients', 'Memory', 'Persistence', 'Stats', 'Replication', 'CPU', 'Keyspace']

        for section in sections:
            inst_infos.update(inst.info(section))

        return inst_infos

    def get_tags_infos(self):
        tags_infos_list = []

        for inst, tags in self.loader.inst_with_tags:
            inst_infos = self.get_inst_infos(inst)
            tags_infos_list.append((inst_infos, tags))

        return tags_infos_list

    def get_metricdata(self, redis_info, tags, name):
        for case in Switch(name):
            if case('redis_aof_last_rewrite_time'):
                name = 'redis.aof.last_rewrite_time'
                value = redis_info.get('aof_last_rewrite_time_sec')

                return MetricData(name, tags, value)
            if case('redis_aof_rewrite'):
                name = 'redis.aof.rewrite'
                value = redis_info.get('aof_rewrite_in_progress')

                return MetricData(name, tags, value)
            if case('redis_clients_biggest_input_buf'):
                name = 'redis.clients.biggest_input_buf'
                value = redis_info.get('client_biggest_input_buf')

                return MetricData(name, tags, value)
            if case('redis_clients_blocked'):
                name = 'redis.clients.blocked'
                value = redis_info.get('blocked_clients')

                return MetricData(name, tags, value)
            if case('redis_clients_longest_output_list'):
                name = 'redis.clients.longest_output_list'
                value = redis_info.get('client_longest_output_list')

                return MetricData(name, tags, value)
            if case('redis_cpu_sys'):
                name = 'redis.cpu.sys'
                value = redis_info.get('used_cpu_sys')

                return MetricData(name, tags, value)
            if case('redis_cpu_sys_children'):
                name = 'redis.cpu.sys_children'
                value = redis_info.get('used_cpu_sys_children')

                return MetricData(name, tags, value)
            if case('redis_cpu_user'):
                name = 'redis.cpu.user'
                value = redis_info.get('used_cpu_user')

                return MetricData(name, tags, value)
            if case('redis_cpu_user_children'):
                name = 'redis.cpu.user_children'
                value = redis_info.get('used_cpu_user_children')

                return MetricData(name, tags, value)
            if case('redis_keys_evicted'):
                name = 'redis.keys.evicted{0}'
                value = redis_info.get('evicted_keys')

                return MetricData(name, tags, value)
            if case('redis_keys_expired'):
                name = 'redis.keys.expired'
                value = redis_info.get('expired_keys')

                return MetricData(name, tags, value)
            if case('redis_mem_fragmentation_ratio'):
                name = 'redis.mem.fragmentation_ratio'
                value = redis_info.get('mem_fragmentation_ratio')

                return MetricData(name, tags, value)
            if case('redis_mem_lua'):
                name = 'redis.mem.lua'
                value = redis_info.get('used_memory_lua')

                return MetricData(name, tags, value)
            if case('redis_mem_rss'):
                name = 'redis.mem.rss'
                value = redis_info.get('used_memory_rss')

                return MetricData(name, tags, value)
            if case('redis_mem_used'):
                name = 'redis.mem.used'
                value = redis_info.get('used_memory')

                return MetricData(name, tags, value)
            if case('redis_net_clients'):
                name = 'redis.net.clients'
                value = redis_info.get('connected_clients')

                return MetricData(name, tags, value)
            if case('redis_net_commands'):
                name = 'redis.net.commands'
                value = redis_info.get('total_commands_processed')

                return MetricData(name, tags, value)
            if case('redis_net_rejected'):
                name = 'redis.net.rejected'
                value = redis_info.get('rejected_connections')

                return MetricData(name, tags, value)
            if case('redis_net_slaves'):
                name = 'redis.net.slaves'
                value = redis_info.get('connected_slaves')

                return MetricData(name, tags, value)
            if case('redis_perf_latest_fork_usec'):
                name = 'redis.perf.latest_fork_usec'
                value = redis_info.get('latest_fork_usec')

                return MetricData(name, tags, value)
            if case('redis_pubsub_channels'):
                name = 'redis.pubsub.channels'
                value = redis_info.get('pubsub_channels')

                return MetricData(name, tags, value)
            if case('redis_rdb_bgsave'):
                name = 'redis.rdb.bgsave'
                value = redis_info.get('rdb_bgsave_in_progress')

                return MetricData(name, tags, value)
            if case('redis_rdb_changes_since_last'):
                name = 'redis.rdb.changes_since_last'
                value = redis_info.get('rdb_changes_since_last_save')

                return MetricData(name, tags, value)
            if case('redis_rdb_last_bgsave_time'):
                name = 'redis.rdb.last_bgsave_time'
                value = redis_info.get('rdb_last_bgsave_time_sec')

                return MetricData(name, tags, value)
            if case('redis_replication_master_repl_offs'):
                name = 'redis.replication.master_repl_offs'
                value = redis_info.get('master_repl_offset')

                return MetricData(name, tags, value)
            if case('redis_stats_keyspace_hits'):
                name = 'redis.stats.keyspace_hits'
                value = redis_info.get('keyspace_hits')

                return MetricData(name, tags, value)
            if case('redis_stats_keyspace_misses'):
                name = 'redis.stats.keyspace_misses'
                value = redis_info.get('redis_stats_keyspace_misses')

                return MetricData(name, tags, value)
            if case():
                return None

    def start_collects(self):
        metrics = []

        tags_infos = self.get_tags_infos()
        if not tags_infos:
            return metrics

        for redis_info, tags in tags_infos:
            data_func = partial(self.get_metricdata, redis_info, tags)
            redis_data = {
                'redis_aof_last_rewrite_time': data_func('redis_aof_last_rewrite_time'),
                'redis_aof_rewrite': data_func('redis_aof_rewrite'),
                'redis_clients_biggest_input_buf': data_func('redis_clients_biggest_input_buf'),
                'redis_clients_blocked': data_func('redis_clients_blocked'),
                'redis_clients_longest_output_list': data_func('redis_clients_longest_output_list'),
                'redis_cpu_sys': data_func('redis_cpu_sys'),
                'redis_cpu_sys_children': data_func('redis_cpu_sys_children'),
                'redis_cpu_user': data_func('redis_cpu_user'),
                'redis_cpu_user_children': data_func('redis_cpu_user_children'),
                'redis_info_latency_ms': data_func('redis_info_latency_ms'),
                'redis_keys_evicted': data_func('redis_keys_evicted'),
                'redis_keys_expired': data_func('redis_keys_expired'),
                'redis_mem_fragmentation_ratio': data_func('redis_mem_fragmentation_ratio'),
                'redis_mem_lua': data_func('redis_mem_lua'),
                'redis_mem_rss': data_func('redis_mem_rss'),
                'redis_mem_used': data_func('redis_mem_used'),
                'redis_net_clients': data_func('redis_net_clients'),
                'redis_net_commands': data_func('redis_net_commands'),
                'redis_net_rejected': data_func('redis_net_rejected'),
                'redis_net_slaves': data_func('redis_net_slaves'),
                'redis_perf_latest_fork_usec': data_func('redis_perf_latest_fork_usec'),
                'redis_pubsub_channels': data_func('redis_pubsub_channels'),
                'redis_rdb_bgsave': data_func('redis_rdb_bgsave'),
                'redis_rdb_changes_since_last': data_func('redis_rdb_changes_since_last'),
                'redis_rdb_last_bgsave_time': data_func('redis_rdb_last_bgsave_time'),
                'redis_replication_master_repl_offs': data_func('redis_replication_master_repl_offs'),
                'redis_stats_keyspace_hits': data_func('redis_stats_keyspace_hits'),
                'redis_stats_keyspace_misses': data_func('redis_stats_keyspace_misses')
            }
            instance = Redis(**redis_data)
            metrics.append(instance)

        return metrics
