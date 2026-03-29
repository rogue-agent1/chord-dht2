#!/usr/bin/env python3
"""Chord DHT (Distributed Hash Table) with finger tables."""
import sys, hashlib

def hash_key(key, bits=8): return int(hashlib.sha1(str(key).encode()).hexdigest(), 16) % (2**bits)

class ChordNode:
    def __init__(self, node_id, bits=8):
        self.id, self.bits, self.m = node_id, bits, 2**bits
        self.finger, self.data = [self] * bits, {}
        self.successor, self.predecessor = self, None
    def in_range(self, val, lo, hi):
        if lo < hi: return lo < val <= hi
        return val > lo or val <= hi
    def find_successor(self, key):
        if self.in_range(key, self.id, self.successor.id): return self.successor
        return self.closest_preceding(key).find_successor(key)
    def closest_preceding(self, key):
        for i in range(self.bits - 1, -1, -1):
            if self.in_range(self.finger[i].id, self.id, key) and self.finger[i].id != key:
                return self.finger[i]
        return self
    def put(self, key, value):
        h = hash_key(key, self.bits)
        node = self.find_successor(h); node.data[key] = value
    def get(self, key):
        h = hash_key(key, self.bits)
        node = self.find_successor(h); return node.data.get(key)

def build_ring(ids, bits=8):
    nodes = {i: ChordNode(i, bits) for i in sorted(ids)}
    sorted_ids = sorted(ids)
    for idx, nid in enumerate(sorted_ids):
        succ_idx = (idx + 1) % len(sorted_ids)
        nodes[nid].successor = nodes[sorted_ids[succ_idx]]
        nodes[nid].predecessor = nodes[sorted_ids[idx - 1]]
        for i in range(bits):
            target = (nid + 2**i) % (2**bits)
            for sid in sorted_ids + [sorted_ids[0] + 2**bits]:
                real_sid = sid % (2**bits)
                if sid >= target:
                    nodes[nid].finger[i] = nodes[real_sid]; break
    return nodes

def main():
    if len(sys.argv) < 2: print("Usage: chord_dht2.py <demo|test>"); return
    if sys.argv[1] == "test":
        nodes = build_ring([0, 64, 128, 192], bits=8)
        nodes[0].put("hello", "world")
        assert nodes[128].get("hello") == "world"  # any node can find it
        nodes[64].put("foo", "bar")
        assert nodes[192].get("foo") == "bar"
        for i in range(20):
            nodes[0].put(f"k{i}", f"v{i}")
        for i in range(20):
            assert nodes[128].get(f"k{i}") == f"v{i}"
        assert nodes[0].get("nonexistent") is None
        print("All tests passed!")
    else:
        nodes = build_ring([0, 64, 128, 192])
        nodes[0].put("test", "value")
        print(f"Lookup from node 128: {nodes[128].get('test')}")

if __name__ == "__main__": main()
