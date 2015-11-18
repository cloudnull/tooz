# -*- coding: utf-8 -*-
#
# Copyright © 2015 Yahoo! Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import logging
import socket
import time

from etcd import client

from tooz import coordination
from tooz import locking


LOG = logging.getLogger(__name__)
TOOZ_NAMESPACE = b"tooz"  # Default namespace when none is provided


class EtcdLock(locking.Lock):
    def __init__(self, name, lock, client):
        super(EtcdLock, self).__init__(name)
        self.lock = lock
        self.client = client
        self.acquired = False

    def acquire(self, blocking=True):
        """Acquire the lock."""

        self.acquired = self.lock.acquire()
        self.lock.renew(60)

        while not self.lock.is_locked() and blocking:
            time.sleep(.005)  # Rest for a faction of a second to reduce stress
            self.acquired = self.lock.acquire()

        return self.acquired

    def release(self):
        """Release the acquired lock."""
        self.lock.release()
        while self.lock.is_locked():
            time.sleep(.005)  # Rest for a faction of a second to reduce stress
            self.lock.release()


class EtcdDriver(coordination.CoordinationDriver):
    """Initialize the etcd driver."""

    def __init__(self, *args):
        super(EtcdDriver, self).__init__()
        self.member_id = args[0]
        self.parsed_url = args[1]
        self.options = args[2]
        self.timeout = 10
        self.host = socket.gethostname()

        self.ttl = self.options.get('timeout', self.timeout)

        # Create client
        self.client = self.make_client()
        self._namespace = self.options.get('namespace', TOOZ_NAMESPACE)
        self.root_namespace = self._path_join(self._namespace)
        self.lock_namespace = self._path_join(self.root_namespace, b'locks')

    def _make_client(self):
        """Create a Consul Client."""

        _hosts = self.options.get('hosts')
        if not isinstance(_hosts, list):
            _hosts = [_hosts]

        hosts = list()
        for host in _hosts:
            try:
                computer, port = host.split(':')
                hosts.append((computer, port))
            except ValueError:  # assume no port was provided
                hosts.append((host, self.options.get('port', 4001)))


        client_kwargs = {
            'hosts': hosts,
            'protocol': self.options.get('protocol', 'http'),
            'version_prefix': self.options.get('version_prefix', '/v2'),
            'read_timeout': float(self.ttl),
            'allow_redirect': self.options.get('allow_redirect', True),
            'allow_reconnect': self.options.get('allow_reconnect', True),
            'per_host_pool_size': self.options.get('per_host_pool_size', 10),
        }

        return client.Client(**client_kwargs)

    def _start(self):
        """Register a Node and Create a session."""

        path_check = self.client.read(self.root_namespace).value
        if not path_check:
            raise coordination.raise_with_cause(
                coordination.ToozError,
                "operation error: %s" % path_check,
                cause=path_check
            )

    @staticmethod
    def _path_join(*args):
        path = '/'.join(args)
        if not path.startswith('/'):
            path = '/%s' % path
        return path

    def get_lock(self, name):
        local_lock_namespace = self._path_join(self.lock_namespace, name)
        lock = self.client.get_lock(local_lock_namespace, ttl=self.ttl)
        return EtcdLock(name=name, lock=lock, client=self.client)

    def get_groups(self):
        return self.client.read(
            self.root_namespace,
            recursive=True,
            sorted=True
        )

    def get_members(self, group_id):
        group_path = self._path_join(self.root_namespace, group_id)
        return self.client.read(group_path, recursive=True, sorted=True)

    def get_member_capabilities(self, group_id, member_id):
        group_path = self._path_join(self.root_namespace, group_id, member_id)
        return self.client.read(group_path, recursive=True, sorted=True).value

    def get_member_info(self, group_id, member_id):
        group_path = self._path_join(self.root_namespace, group_id, member_id)
        return self.client.read(group_path, sorted=True).value

    def create_group(self, group_id):
        self.client.write(self.root_namespace, group_id)

    def delete_group(self, group_id):
        self.client.delete(self.root_namespace, group_id)

    def get_leader(self, group_id):
        return self.client.leader

    def get_machines(self, group_id):
        return self.client.machines
