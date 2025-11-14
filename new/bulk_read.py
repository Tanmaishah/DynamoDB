#!/usr/bin/env python3
"""
bulk_read.py - Read test script
Usage: python bulk_read.py <num_keys> <node_address>
"""

import grpc
import dynamo_pb2
import dynamo_pb2_grpc
import sys
import time
import random

def bulk_read(num_keys, node_address, pattern="sequential"):
    """Read many keys to test read performance."""
    
    channel = grpc.insecure_channel(node_address)
    stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
    
    print(f"Reading {num_keys} keys from {node_address} ({pattern})...")
    
    found_count = 0
    not_found_count = 0
    total_time = 0
    
    # Generate key list based on pattern
    if pattern == "random":
        keys = [f"key_{random.randint(0, num_keys*2)}" for _ in range(num_keys)]
    else:  # sequential
        keys = [f"key_{i}" for i in range(num_keys)]
    
    for i, key in enumerate(keys):
        try:
            start = time.time()
            request = dynamo_pb2.GetRequest(key=key)
            response = stub.Get(request, timeout=5)
            elapsed = time.time() - start
            
            total_time += elapsed
            
            if response.found:
                found_count += 1
            else:
                not_found_count += 1
            
            if i % 100 == 0:
                avg_latency = (total_time / (i+1)) * 1000
                print(f"Progress: {i}/{num_keys} | "
                      f"Found: {found_count} | "
                      f"Not found: {not_found_count} | "
                      f"Avg latency: {avg_latency:.2f}ms")
                
        except Exception as e:
            print(f"✗ Error reading {key}: {e}")
            not_found_count += 1
    
    channel.close()
    
    # Summary
    print(f"\n{'='*60}")
    print(f"BULK READ SUMMARY")
    print(f"{'='*60}")
    print(f"Total keys read:      {num_keys}")
    print(f"Found:                {found_count}")
    print(f"Not found:            {not_found_count}")
    print(f"Hit rate:             {(found_count/num_keys)*100:.1f}%")
    print(f"Average latency:      {(total_time/num_keys)*1000:.2f}ms")
    print(f"Throughput:           {num_keys/total_time:.1f} ops/sec")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python bulk_read.py <num_keys> <node_address> [pattern]")
        print("Pattern: sequential (default) or random")
        print("Example: python bulk_read.py 1000 localhost:5000 random")
        sys.exit(1)
    
    num_keys = int(sys.argv[1])
    node_address = sys.argv[2]
    pattern = sys.argv[3] if len(sys.argv) > 3 else "sequential"
    
    bulk_read(num_keys, node_address, pattern)