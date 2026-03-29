#!/usr/bin/env python3
"""chord_dht2 - Chord distributed hash table with finger tables."""
import sys, hashlib

def hash_key(key, bits=8):
    h = int(hashlib.sha1(str(key).encode()).hexdigest(), 16)
    return h % (2 ** bits)

class ChordNode:
    def __init__(self, node_id, bits=8):
        self.id = node_id
        self.bits = bits
        self.ring_size = 2 ** bits
        self.successor = self
        self.predecessor = None
        self.finger = [self] * bits
        self.data = {}
    
    def find_successor(self, key):
        if self._in_range(key, self.id + 1, self.successor.id + 1):
            return self.successor
        node = self._closest_preceding(key)
        if node == self:
            return self.successor
        return node.find_successor(key)
    
    def _closest_preceding(self, key):
        for i in range(self.bits - 1, -1, -1):
            if self.finger[i] != self and self._in_range(self.finger[i].id, self.id + 1, key):
                return self.finger[i]
        return self
    
    def _in_range(self, val, lo, hi):
        val %= self.ring_size
        lo %= self.ring_size
        hi %= self.ring_size
        if lo < hi:
            return lo <= val < hi
        return val >= lo or val < hi
    
    def join(self, existing):
        if existing:
            self.successor = existing.find_successor(self.id)
            self.predecessor = None
        else:
            self.successor = self
            self.predecessor = self
    
    def stabilize(self):
        x = self.successor.predecessor
        if x and x != self and self._in_range(x.id, self.id + 1, self.successor.id):
            self.successor = x
        self.successor.notify(self)
    
    def notify(self, node):
        if self.predecessor is None or \
           self._in_range(node.id, self.predecessor.id + 1, self.id):
            self.predecessor = node
    
    def fix_fingers(self):
        for i in range(self.bits):
            target = (self.id + 2**i) % self.ring_size
            self.finger[i] = self.find_successor(target)
    
    def put(self, key, value):
        h = hash_key(key, self.bits)
        node = self.find_successor(h)
        node.data[key] = value
    
    def get(self, key):
        h = hash_key(key, self.bits)
        node = self.find_successor(h)
        return node.data.get(key)

def create_ring(node_ids, bits=8):
    nodes = [ChordNode(nid, bits) for nid in sorted(node_ids)]
    nodes[0].join(None)
    for n in nodes[1:]:
        n.join(nodes[0])
    # Stabilize
    for _ in range(len(nodes) * 3):
        for n in nodes:
            n.stabilize()
    for n in nodes:
        n.fix_fingers()
    return nodes

def test():
    nodes = create_ring([0, 64, 128, 192], bits=8)
    
    # Ring structure
    assert nodes[0].successor.id == 64
    assert nodes[1].successor.id == 128
    
    # Lookup
    target = nodes[0].find_successor(100)
    assert target.id == 128  # 100 falls in (64, 128]
    
    # Put/get
    nodes[0].put("hello", "world")
    assert nodes[0].get("hello") == "world"
    # Get from different node
    assert nodes[2].get("hello") == "world"
    
    # Multiple keys
    for i in range(20):
        nodes[0].put(f"key_{i}", f"val_{i}")
    for i in range(20):
        assert nodes[1].get(f"key_{i}") == f"val_{i}"
    
    # Data distributed across nodes
    total_keys = sum(len(n.data) for n in nodes)
    assert total_keys == 21  # 20 + "hello"
    nodes_with_data = sum(1 for n in nodes if n.data)
    assert nodes_with_data >= 2  # Spread across nodes
    
    print(f"Keys per node: {[len(n.data) for n in nodes]}")
    print("All tests passed!")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test()
    else:
        print("Usage: chord_dht2.py test")
