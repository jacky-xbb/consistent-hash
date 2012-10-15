from nose.tools import *
from random import randint, sample
from consistent_hash.consistent_hash import ConsistentHash

class TestConsistentHash:
    node_nums = 6
    obj_nums = 10000
    obj_len = 10
    @classmethod
    def setup_class(cls):
        cls.nodes = cls.make_random_ips(cls.node_nums)
        cls.objs = cls.make_random_objs(cls.obj_nums, cls.obj_len)
        cls.con_hash = ConsistentHash(cls.nodes)        

    @classmethod
    def teardown_class(cls):
        pass

    def setUp(self):
        self.hit_nums = {}
        
    def tearDown(self):
        pass
        
    def test_add_nodes(self):
        # Add nodes to hashing ring
        self.con_hash.add_nodes(self.nodes) 
        # Get nodes from hashing ring
        for obj in self.objs:
            node = self.con_hash.get_node(obj)
            self.hit_nums[node] = self.hit_nums.get(node, 0) + 1
        self.show_nodes_balance()
            
    def test_del_nodes(self):
        del_nodes = self.nodes[0:2]
        # Delete the nodes from hashing ring
        self.con_hash.del_nodes(del_nodes)
        print 'The {nodes} deleted!!!'.format(nodes=self.nodes[0:2])
        # Get nodes from hashing ring after deleting
        for obj in self.objs:
            node = self.con_hash.get_node(obj)
            self.hit_nums[node] = self.hit_nums.get(node, 0) + 1
        self.show_nodes_balance()
        
    def show_nodes_balance(self):
        print '-'*67
        print 'Nodes count:{nNodes} Replicas:{rep} Objects count:{nObjs} Ideal percent:{per}'.format(
                nNodes = self.con_hash.get_nodes_cnt(),
                rep = self.replicas,
                nObjs = len(self.objs),
                per = self.get_percent(1, self.con_hash.get_nodes_cnt()))
        print '-'*27 + 'Nodes balance' + '-'*27
        for node in self.con_hash.get_all_nodes():
            print 'Nodes name:{nNodes} - Objects count:{nObjs} - percent:{per}'.format(
                    nNodes = node,
                    nObjs = self.hit_nums[node],
                    per = self.get_percent(self.hit_nums[node], self.obj_nums))
            
        
    def get_percent(self, num, sum):
        return "{0:.0f}%".format(float(num) / sum * 100)
         
    @classmethod
    def make_random_ips(cls, num):
        #nodes = []
        #for i in range(num):
        #    nodes.append('.'.join([str(randint(1,255)) for i in range(4)]))
        #return nodes 
        return {'192.168.0.101':1, '192.168.0.102:11212': 1, '192.168.0.103:11212': 2, '192.168.0.104:11212': 1}

    @classmethod
    def make_random_objs(cls, num, len):
        objs = []
        for i in range(num):
            objs.append('.'.join(sample([chr(i) for i in range(48, 123)], len)))
        return objs
