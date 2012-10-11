import hashlib
import bisect

class ConsistentHash():
    def __init__(self, replicas=3):
        self.replicas = replicas
        self.keys = []
        self.key_node = {}
        self.nodes = []

    def add_nodes(self, nodes):
        self.nodes = list(set(self.nodes + nodes[:]))
        vnodes = self._make_vnodes(nodes)
        for vnode in vnodes:
            key = self._get_key(vnode)
            bisect.insort(self.keys, key)
            self.key_node[key] = vnode

    def del_nodes(self, nodes):
        for node in nodes:
            self.nodes.remove(node)
        vnodes = self._make_vnodes(nodes) 
        for vnode in vnodes:
            key = self._get_key(vnode)
            self.keys.remove(key)
            del self.key_node[key]

    def get_node(self, obj):
        key = self._get_key(obj)
        index = bisect.bisect(self.keys, key) 
        if index == len(self.keys):
            return self.key_node[self.keys[0]].split('-')[0]
        else:
            return self.key_node[self.keys[index]].split('-')[0]
        
    def get_all_nodes(self):
        # Sorted with ascend
        return sorted(self.nodes, key=lambda node:map(int, node.split('.')))
    
    def get_all_vnodes(self):
        # Sorted with clockwise
        return map(lambda node:node[1],
                sorted(self.key_node.items(), key=lambda x:x[0]))
    
    def get_nodes_cnt(self):
        return len(self.nodes)

    def _make_vnodes(self, nodes):
        return ["{node}-{replica}".format(node=node, replica=replica) 
                for node in nodes 
                for replica in range(self.replicas)]

    def _get_key(self, obj):
        m = hashlib.md5()
        m.update(obj)
        return long(m.hexdigest(), 16)

