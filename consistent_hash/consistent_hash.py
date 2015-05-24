# -*- coding: utf-8 -*-
# Copyright (c) 2012 Yummy Bian <yummy.bian#gmail.com>.
#
# This module is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.

import traceback
import hashlib
import bisect
import re
import sys

if sys.version_info[0] == 3:
    xrange = range


class ConsistentHash(object):

    interleave_count = 40
    hasher = None

    def __init__(self, objects=None):
        """`objects`, when you are running a cluster of Memcached
        servers it could happen to not all server can allocate the
        same amount of memory. You might have a Memcached server
        with 128mb, 512mb, 128mb. If you would the array structure
        all servers would have the same weight in the consistent
        hashing scheme. Spreading the keys 33/33/33 over the servers.
        But as server 2 has more memory available you might want to
        give it more weight so more keys get stored on that server.
        When you are using a object, the key should represent the
        server location syntax and the value the weight of the server.

        By default all servers have a weight of 1.
        {'192.168.0.101:11212': 1,
         '192.168.0.102:11212': 2,
         '192.168.0.103:11212': 1}
        would generate a 25/50/25 distribution of the keys.
        """
        self.keys = []
        self.key_node = {}
        self.nodes = []
        self.index = 0
        self.weights = {}
        self.total_weight = 0

        self.add_nodes(objects)

    def _ingest_objects(self, objects):
        try:
            if isinstance(objects, dict):
                self.nodes.extend(objects.keys())
                self.weights.update(objects.copy())
            elif isinstance(objects, list):
                self.nodes.extend(objects[:])
            elif isinstance(objects, str):
                self.nodes.extend(objects)
            elif objects is None:
                pass
            else:
                raise TypeError("The arguments of nodes must be dict,\
                        list or string.")
        except TypeError:
            traceback.print_exc(file=sys.stdout)

    def add_nodes(self, nodes):
        """
        Adds nodes to the ring.

        Nodes can be a list of nodes (assumed to be of weight 1),
        a dictionary keyed by node name and valued by weight,
        or a string specifying a single node of weight 1.
        """
        self._ingest_objects(nodes)

        self._generate_ring(start=self.index)

        self.index = self.get_nodes_cnt()
        self.keys.sort()

    def _generate_ring(self, start=0):
        # Generates the ring.
        for node in self.nodes[start:]:
            for key in self._node_keys(node):
                self.key_node[key] = node
                self.keys.append(key)

    def del_nodes(self, nodes):
        """
        Deletes nodes from the ring.

        Nodes is expected to be a list of nodes already
        present in the ring.
        """
        try:
            if not isinstance(nodes, list):
                raise TypeError("The arguments of nodes must be list.")
        except TypeError:
            traceback.print_exc(file=sys.stdout)

        # Delete nodes from the ring.
        for node in nodes:
            if node not in self.nodes:
                continue

            for key in self._node_keys(node):
                self.keys.remove(key)
                del self.key_node[key]

            self.index -= 1
            self.nodes.remove(node)

    def _node_keys(self, node):
        """
        Generates the keys specific to a given node.
        """
        if node in self.weights:
            weight = self.weights.get(node)
        else:
            weight = 1

        factor = self.interleave_count * weight

        for j in xrange(0, int(factor)):
            b_key = self._hash_digest('%s-%s' % (node, j))
            for i in xrange(4):
                yield self._hash_val(b_key, lambda x: x + i * 4)

    def get_node(self, string_key):
        """Given a string key a corresponding node in the hash
        ring is returned.

        If the hash ring is empty, `None` is returned.
        """
        pos = self.get_node_pos(string_key)
        if pos is None:
            return None
        return self.key_node[self.keys[pos]]

    def get_node_pos(self, string_key):
        """Given a string key a corresponding node in the hash
        ring is returned along with it's position in the ring.

        If the hash ring is empty, (`None`, `None`) is returned.
        """
        if not self.key_node:
            return None
        key = self.gen_key(string_key)
        nodes = self.keys
        pos = bisect.bisect(nodes, key)
        if pos == len(nodes):
            return 0
        else:
            return pos

    def get_all_nodes(self):
        # Sorted with ascend
        return sorted(self.nodes,
                      key=lambda node: list(map(int, re.split('\W', node))))

    def get_nodes_cnt(self):
        return len(self.nodes)

    def gen_key(self, key):
        """Given a string key it returns a long value,
        this long value represents a place on the hash ring.

        md5 is currently used because it mixes well.
        """
        b_key = self._hash_digest(key)
        return self._hash_val(b_key, lambda x: x)

    def _hash_val(self, b_key, entry_fn):
        """Imagine keys from 0 to 2^32 mapping to a ring,
        so we divide 4 bytes of 16 bytes md5 into a group.
        """
        return ((b_key[entry_fn(3)] << 24)
                | (b_key[entry_fn(2)] << 16)
                | (b_key[entry_fn(1)] << 8)
                | b_key[entry_fn(0)])

    def _hash_digest(self, key):
        key = key.encode() if sys.version_info[0] == 3 \
            and isinstance(key, str) else key

        if self.hasher is not None:
            res = [x if isinstance(x, int) else ord(x)
                   for x in self.hasher(key)]
        else:
            m = hashlib.md5()
            m.update(key)
            res = [x if isinstance(x, int) else ord(x)
                   for x in m.digest()]

        return res
