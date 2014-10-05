from __future__ import print_function

import string
import sys

if sys.version_info[0] == 3:
    chars = string.ascii_letters + string.digits
else:
    chars = string.letters + string.digits

import random

from consistent_hash.consistent_hash import ConsistentHash

ConsistentHash.interleave_count = 1000


class TestConsistentHash:
    init_nodes = {'192.168.0.101:11212': 1,
                  '192.168.0.102:11212': 1,
                  '192.168.0.103:11212': 1,
                  '192.168.0.104:11212': 1}
    obj_nums = 10000

    @classmethod
    def setup_class(cls):
        cls.objs = cls.gen_random_objs()
        print('Initial nodes {nodes}'.format(nodes=cls.init_nodes))

    @classmethod
    def teardown_class(cls):
        pass

    def setUp(self):
        self.hit_nums = {}

    def tearDown(self):
        pass

    def test___init__(self):
        self.con_hash = ConsistentHash(self.init_nodes)
        # Get nodes from hashing ring
        for obj in self.objs:
            node = self.con_hash.get_node(obj)
            self.hit_nums[node] = self.hit_nums.get(node, 0) + 1
        distribution = self.show_nodes_balance()

        self.validate_distribution(distribution, {
            '192.168.0.101:11212': (23, 27),
            '192.168.0.102:11212': (23, 27),
            '192.168.0.103:11212': (23, 27),
            '192.168.0.104:11212': (23, 27)
        })

    def test_empty__init__(self):
        self.con_hash = ConsistentHash()
        for obj in self.objs:
            node = self.con_hash.get_node(obj)

            if node is not None:
                raise Exception("Should have received an exception \
                                 when hashing using an empty LUT")

        self.con_hash.add_nodes(self.init_nodes)

        for obj in self.objs:
            node = self.con_hash.get_node(obj)
            self.hit_nums[node] = self.hit_nums.get(node, 0) + 1

        distribution = self.show_nodes_balance()

        self.validate_distribution(distribution, {
            '192.168.0.101:11212': (23, 27),
            '192.168.0.102:11212': (23, 27),
            '192.168.0.103:11212': (23, 27),
            '192.168.0.104:11212': (23, 27)
        })

    def test_add_nodes(self):
        self.con_hash = ConsistentHash(self.init_nodes)
        # Add nodes to hashing ring
        add_nodes = {'192.168.0.105:11212': 1}
        self.con_hash.add_nodes(add_nodes)
        # Get nodes from hashing ring
        for obj in self.objs:
            node = self.con_hash.get_node(obj)
            self.hit_nums[node] = self.hit_nums.get(node, 0) + 1
        distribution = self.show_nodes_balance()

        self.validate_distribution(distribution, {
            '192.168.0.105:11212': (17, 23),
            '192.168.0.102:11212': (17, 23),
            '192.168.0.104:11212': (17, 23),
            '192.168.0.101:11212': (17, 23),
            '192.168.0.103:11212': (17, 23)
        })
        print('->The {nodes} added!!!'.format(nodes=add_nodes))

    def test_del_nodes(self):
        self.con_hash = ConsistentHash(self.init_nodes)
        # del_nodes = self.nodes[0:2]
        del_nodes = ['192.168.0.102:11212', '192.168.0.104:11212']
        # Delete the nodes from hashing ring
        self.con_hash.del_nodes(del_nodes)
        # Get nodes from hashing ring after deleting
        for obj in self.objs:
            node = self.con_hash.get_node(obj)
            self.hit_nums[node] = self.hit_nums.get(node, 0) + 1
        distribution = self.show_nodes_balance()

        self.validate_distribution(distribution, {
            '192.168.0.101:11212': (48, 52),
            '192.168.0.103:11212': (48, 52)
        })
        print('->The {nodes} deleted!!!'.format(nodes=del_nodes))

    # -------------Help functions-------------
    def show_nodes_balance(self):
        distribution = {}
        print('-' * 67)
        print('Nodes count:{nNodes} Objects count:{nObjs}'.format(
            nNodes=self.con_hash.get_nodes_cnt(),
            nObjs=len(self.objs)
        ))
        print('-' * 27 + 'Nodes balance' + '-' * 27)

        for node in self.con_hash.get_all_nodes():
            substitutions = {
                'nNodes': node,
                'nObjs': self.hit_nums[node],
                'percentage': self.get_percent(self.hit_nums[node],
                                               self.obj_nums)
            }

            print('Nodes:{nNodes} \
                   - Objects count:{nObjs} \
                   - percent:{percentage}%'.format(**substitutions))

            distribution[node] = substitutions['percentage']

        return distribution

    def validate_distribution(self, actual, expected):
        if expected.keys() != actual.keys():
            raise Exception("Expected nodes does not match actual nodes")

        for i in expected.keys():
            actual_value = actual[i]
            min_value = expected[i][0]
            max_value = expected[i][1]

            if actual_value < min_value or actual_value > max_value:
                print(min_value, actual_value, max_value)
                raise Exception("Value outside of expected range")

        print("Validated ranges")

    def get_percent(self, num, sum):
        return int(float(num) / sum * 100)

    @classmethod
    def gen_random_objs(cls, num=10000, len=10):
        objs = []
        for i in range(num):
            objs.append(''.join([random.choice(chars) for i in range(len)]))
        return objs
