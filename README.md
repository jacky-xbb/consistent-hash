consistent-hash
===============

Implements consistent hashing that can be used when the number of server nodes can increase or decrease.

Install:
===============
    python setup.py install
    
Usage:
===============
It's so easy to use^_^

    from consistent_hash import ConsistentHash

    servers = ['192.169.0.101',
               '192.168.0.102',
               '192.168.0.103',
               '192.168.0.104',
               '192.168.0.105']
    
    # Initial 
    con_hash = ConsistentHash(replicas)
    # Add servers to hash ring
    con_hash.add_nodes(servers)
    # Get a server via the key of object
    server = con_hash.get_node('my_key')
    # Delete the server from hash ring
    con_hash.del_nodes(['192.168.0.102'])


Unit test:
===============
- Firstly, install nose which extends unittest to make testing easier.

        pip install nose
    
    
- Then, run tests.
        
        # Option -s any stdout output will be printed immediately
        nosetests -s


More information about [nose](https://nose.readthedocs.org/en/latest/"nose").
