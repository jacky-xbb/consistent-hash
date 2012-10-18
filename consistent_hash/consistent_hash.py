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
import math
import re
import sys

class ConsistentHash():
    def __init__(self, objects=None):
        """`objects`, when you are running a cluster of Memcached servers
        it could happen to not all server can allocate the same amount of memory. 
        You might have a Memcached server with 128mb, 512, 128mb. If you would the 
        array structure all servers would have the same weight in the consistent 
        hashing scheme. Spreading the keys 33/33/33 over the servers. But as server 
        2 has more memory available you might want to give it more weight so more keys
        get stored on that server. When you are using a object, the key should represent 
        the server location syntax and the value the weight of the server. 

        By default all servers have a weight of 1.
        { '192.168.0.101:11212': 1, '192.168.0.102:11212': 2, '192.168.0.103:11212': 1 } 
        would generate a 25/50/25 distribution of the keys.
        """
        self.keys = []
        self.key_node = {}
        self.nodes = []
        self.index = 0
        self.weights = {} 
        self.total_weight = 0
        self.add_nodes(objects)

    def _check_parameters(self, objects):
        try:
            if isinstance(objects, dict):
                self.nodes.extend(objects.keys())
                self.weights.update(objects.copy())
            elif isinstance(objects, list):
                self.nodes.extend(objects[:])
            elif isinstance(objects, str):
                self.nodes.extend(objects)
            else:
                raise TypeError("The arguments of nodes must be dict, list or string.") 
        except TypeError as error:
            traceback.print_exc(file=sys.stdout)

    def add_nodes(self, objects):
        self._check_parameters(objects)
        # Generates the ring.
        for node in self.nodes[self.index:]:
            self.total_weight += self.weights.get(node, 1)

        for node in self.nodes[self.index:]:
            weight = 1
            if node in self.weights:
                weight = self.weights.get(node)
            factor = 40 * weight
            for j in xrange(0, int(factor)):
                b_key = self._hash_digest('%s-%s' % (node, j))
                for i in xrange(0, 3):
                    key = self._hash_val(b_key, lambda x: x+i*4)
                    self.key_node[key] = node
                    self.keys.append(key)

        self.index = self.get_nodes_cnt()
        self.keys.sort()

    def del_nodes(self, nodes):
        try:
            if not isinstance(nodes, list):
                raise TypeError("The arguments of nodes must be list.") 
        except TypeError as error:
            traceback.print_exc(file=sys.stdout)
        # Delete nodes from the ring. 
        for node in nodes:
            weight = 1
            if node in self.weights:
                weight = self.weights.get(node)
            factor = 40 * weight
            for j in xrange(0, int(factor)):
                b_key = self._hash_digest('%s-%s' % (node, j))
                for i in xrange(0, 3):
                    key = self._hash_val(b_key, lambda x: x+i*4)
                    self.keys.remove(key)
                    del self.key_node[key]
            self.index -= 1
            self.nodes.remove(node)

    def get_node(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned.

        If the hash ring is empty, `None` is returned.
        """
        pos = self.get_node_pos(string_key)
        if pos is None:
            return None
        return self.key_node[self.keys[pos]]

    def get_node_pos(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned
        along with it's position in the ring.

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
        return sorted(self.nodes, key=lambda node:map(int, re.split('\W', node)))
    
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
        return (( b_key[entry_fn(3)] << 24)
                |(b_key[entry_fn(2)] << 16)
                |(b_key[entry_fn(1)] << 8)
                | b_key[entry_fn(0)] )

    def _hash_digest(self, key):
        m = hashlib.md5()
        m.update(key)
        return map(ord, m.digest())

