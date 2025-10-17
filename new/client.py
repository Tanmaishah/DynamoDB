import grpc
import dynamo_pb2
import dynamo_pb2_grpc
import sys


def put(address, key, value):
    """Send PUT request to a node."""
    try:
        channel = grpc.insecure_channel(address)
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        request = dynamo_pb2.PutRequest(key=key, value=value)
        response = stub.Put(request, timeout=5)
        
        if response.success:
            print(f"✓ PUT successful: {key} = {value}")
        else:
            print(f"✗ PUT failed: {key} = {value}")
        
        channel.close()
        return response.success
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def get(address, key):
    """Send GET request to a node."""
    try:
        channel = grpc.insecure_channel(address)
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        request = dynamo_pb2.GetRequest(key=key)
        response = stub.Get(request, timeout=5)
        
        if response.found:
            print(f"✓ GET successful: {key} = {response.value}")
            return response.value
        else:
            print(f"✗ Key not found: {key}")
            return None
        
        channel.close()
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return None


def interactive_mode(address):
    """Interactive client for testing."""
    print(f"\n{'='*60}")
    print(f"DynamoDB Client - Connected to {address}")
    print(f"{'='*60}")
    print("Commands:")
    print("  put <key> <value>  - Store a key-value pair")
    print("  get <key>          - Retrieve a value")
    print("  quit               - Exit")
    print(f"{'='*60}\n")
    
    while True:
        try:
            cmd = input("dynamo> ").strip().split()
            
            if not cmd:
                continue
            
            operation = cmd[0].lower()
            
            if operation == "quit" or operation == "exit":
                print("Goodbye!")
                break
            
            elif operation == "put":
                if len(cmd) < 3:
                    print("Usage: put <key> <value>")
                    continue
                key = cmd[1]
                value = " ".join(cmd[2:])  # Allow values with spaces
                put(address, key, value)
            
            elif operation == "get":
                if len(cmd) < 2:
                    print("Usage: get <key>")
                    continue
                key = cmd[1]
                get(address, key)
            
            else:
                print(f"Unknown command: {operation}")
        
        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Interactive: python client.py <node_address>")
        print("  Single PUT:  python client.py <node_address> put <key> <value>")
        print("  Single GET:  python client.py <node_address> get <key>")
        print("\nExample:")
        print("  python client.py localhost:5000")
        print("  python client.py localhost:5000 put user123 Alice")
        print("  python client.py localhost:5000 get user123")
        sys.exit(1)
    
    address = sys.argv[1]
    
    if len(sys.argv) == 2:
        # Interactive mode
        interactive_mode(address)
    else:
        # Single command mode
        operation = sys.argv[2].lower()
        
        if operation == "put":
            if len(sys.argv) < 5:
                print("Usage: python client.py <address> put <key> <value>")
                sys.exit(1)
            key = sys.argv[3]
            value = " ".join(sys.argv[4:])
            put(address, key, value)
        
        elif operation == "get":
            if len(sys.argv) < 4:
                print("Usage: python client.py <address> get <key>")
                sys.exit(1)
            key = sys.argv[3]
            get(address, key)
        
        else:
            print(f"Unknown operation: {operation}")
            sys.exit(1)