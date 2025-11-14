import grpc
import dynamo_pb2
import dynamo_pb2_grpc
import sys
import time

def bulk_write(num_keys, node_address):
    """Write many keys to test the system."""
    
    channel = grpc.insecure_channel(node_address)
    stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
    
    print(f"Writing {num_keys} keys to {node_address}...")
    
    success_count = 0
    fail_count = 0
    total_time = 0
    
    for i in range(num_keys):
        key = f"key_{i}"
        value = f"value_{i}_{'x' * 50}"  # ~60 bytes per value
        
        try:
            start = time.time()
            request = dynamo_pb2.PutRequest(key=key, value=value)
            response = stub.Put(request, timeout=5)
            elapsed = time.time() - start
            
            if response.success:
                success_count += 1
                total_time += elapsed
                
                if i % 500 == 0:  # Progress every 100 keys
                    avg_latency = (total_time / success_count) * 1000
                    print(f"Progress: {i}/{num_keys} | "
                          f"Success: {success_count} | "
                          f"Avg latency: {avg_latency:.2f}ms")
            else:
                fail_count += 1
                print(f"✗ Failed to write {key}")
                
        except Exception as e:
            fail_count += 1
            print(f"✗ Error writing {key}: {e}")
    
    channel.close()
    
    # Summary
    print(f"\n{'='*60}")
    print(f"BULK WRITE SUMMARY")
    print(f"{'='*60}")
    print(f"Total keys attempted: {num_keys}")
    print(f"Successful writes:    {success_count}")
    print(f"Failed writes:        {fail_count}")
    print(f"Success rate:         {(success_count/num_keys)*100:.1f}%")
    print(f"Average latency:      {(total_time/success_count)*1000:.2f}ms")
    print(f"Throughput:           {success_count/total_time:.1f} ops/sec")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python bulk_write.py <num_keys> <node_address>")
        print("Example: python bulk_write.py 1000 localhost:5000")
        sys.exit(1)
    
    num_keys = int(sys.argv[1])
    node_address = sys.argv[2]
    
    bulk_write(num_keys, node_address)