import string
import random
from nose.tools import *
from consistent_hash.consistent_hash import ConsistentHash

class TestConsistentHash:
    init_nodes = {'192.168.0.101:11212':1,
                  '192.168.0.102:11212':1, 
                  '192.168.0.103:11212':1, 
                  '192.168.0.104:11212':1}
    obj_nums = 10000
    @classmethod
    def setup_class(cls):
        cls.objs = cls.gen_random_objs()
        print 'Initial nodes {nodes}'.format(nodes=cls.init_nodes)

    @classmethod
    def teardown_class(cls):
        pass

    def setUp(self):
        self.hit_nums = {}
        self.con_hash = ConsistentHash(self.init_nodes)        
        
    def tearDown(self):
        pass

    def test___init__(self):
        # Get nodes from hashing ring
        for obj in self.objs:
            node = self.con_hash.get_node(obj)
            self.hit_nums[node] = self.hit_nums.get(node, 0) + 1
        self.show_nodes_balance()

    def test_add_nodes(self):
        # Add nodes to hashing ring
        add_nodes = {'192.168.0.105:11212':1}
        self.con_hash.add_nodes(add_nodes) 
        # Get nodes from hashing ring
        for obj in self.objs:
            node = self.con_hash.get_node(obj)
            self.hit_nums[node] = self.hit_nums.get(node, 0) + 1
        self.show_nodes_balance()
        print '->The {nodes} added!!!'.format(nodes=add_nodes)
            
    def test_del_nodes(self):
        #del_nodes = self.nodes[0:2]
        del_nodes = ['192.168.0.102:11212', '192.168.0.104:11212']
        # Delete the nodes from hashing ring
        self.con_hash.del_nodes(del_nodes)
        # Get nodes from hashing ring after deleting
        for obj in self.objs:
            node = self.con_hash.get_node(obj)
            self.hit_nums[node] = self.hit_nums.get(node, 0) + 1
        self.show_nodes_balance()
        print '->The {nodes} deleted!!!'.format(nodes=del_nodes)

    # -------------Help functions-------------
    def show_nodes_balance(self):
        print '-'*67
        print 'Nodes count:{nNodes} Objects count:{nObjs}'.format(
                nNodes = self.con_hash.get_nodes_cnt(),
                nObjs = len(self.objs),
                per = self.get_percent(1, self.con_hash.get_nodes_cnt()))
        print '-'*27 + 'Nodes balance' + '-'*27
        for node in self.con_hash.get_all_nodes():
            print 'Nodes:{nNodes} - Objects count:{nObjs} - percent:{per}'.format(
                    nNodes = node,
                    nObjs = self.hit_nums[node],
                    per = self.get_percent(self.hit_nums[node], self.obj_nums))
            
    def get_percent(self, num, sum):
        return "{0:.0f}%".format(float(num) / sum * 100)
         
    @classmethod
    def gen_random_objs(cls, num=10000, len=10):
        objs = []
        for i in range(num):
            chars = string.letters + string.digits
            objs.append(''.join([random.choice(chars) for i in range(len)]))
        return objs
