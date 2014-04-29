consistent-hash
===============

Implements consistent hashing that can be used when the number of server nodes can increase or decrease.The algorithm that is used for consistent hashing is the same as [libketama](https://github.com/RJ/ketama).

Install:
===============
    python setup.py install
    
Usage:
===============
It's so easy to use^_^

    from consistent_hash import ConsistentHash
    
    # You can construct consistent hash with the below three ways
    con_hash = ConsistentHash({'192.168.0.101:11212':1, '192.168.0.102:11212':2, '192.168.0.103:11212':1})
    # Or
    con_hash = ConsistentHash(['192.168.0.101:11212', '192.168.0.102:11212', '192.168.0.103:11212'])
    # Or
    con_hash = ConsistentHash('192.168.0.101:11212')
    
    # Add servers to hash ring
    con_hash.add_nodes({'192.168.0.104:11212':1})
    # Get a server via the key of object
    server = con_hash.get_node('my_key')
    # Delete the server from hash ring, you don't need to indicate weights
    con_hash.del_nodes(['192.168.0.102:11212', '192.168.0.104:11212'])


Unit test:
===============
- Firstly, install nose which extends unittest to make testing easier.

        pip install nose
    
    
- Then, run tests.
        
        # Option -s any stdout output will be printed immediately
        # and -v be more verbose
        nosetests -s -v


More information about [nose](https://nose.readthedocs.org/en/latest/).
