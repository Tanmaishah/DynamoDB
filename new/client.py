import grpc
import dynamo_pb2
import dynamo_pb2_grpc
import sys


# Store vector clocks for keys (simulating client-side context)
client_context = {}  # key -> VectorClock


def put(address, key, value, context=None):
    """
    Send PUT request to a node.
    
    Args:
        address: Node address
        key: Key to store
        value: Value to store
        context: Optional VectorClock (from previous GET)
    """
    try:
        channel = grpc.insecure_channel(address)
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        # Build request with optional context
        request = dynamo_pb2.PutRequest(key=key, value=value)
        if context:
            request.context.CopyFrom(context)
        
        response = stub.Put(request, timeout=5)
        
        if response.success:
            print(f"✓ PUT successful: {key} = {value}")
            # Store updated vector clock for future PUTs
            if response.updated_clock:
                client_context[key] = response.updated_clock
                print(f"  Updated context: {dict(response.updated_clock.clock)}")
            return True
        else:
            print(f"✗ PUT failed: {key} = {value}")
            return False
        
        channel.close()
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def get(address, key):
    """
    Send GET request to a node.
    May return multiple values if there's a conflict.
    """
    try:
        channel = grpc.insecure_channel(address)
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        request = dynamo_pb2.GetRequest(key=key)
        response = stub.Get(request, timeout=5)
        
        if response.found:
            if len(response.values) == 1:
                # No conflict
                v = response.values[0]
                print(f"✓ GET successful: {key} = {v.value}")
                print(f"  Vector clock: {dict(v.vector_clock.clock)}")
                
                # Store vector clock for future PUTs
                client_context[key] = v.vector_clock
                
                return v.value
            else:
                # Conflict! Multiple versions
                print(f"⚠ CONFLICT: {len(response.values)} versions found for '{key}':")
                for i, v in enumerate(response.values):
                    print(f"  [{i+1}] {v.value} (VC: {dict(v.vector_clock.clock)})")
                
                # Ask user to resolve
                print("\nTo resolve conflict, use: resolve <key> <value>")
                print("This will write the chosen value with merged context.")
                
                return None
        else:
            print(f"✗ Key not found: {key}")
            return None
        
        channel.close()
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return None


def resolve_conflict(address, key, chosen_value):
    """
    Resolve a conflict by writing the chosen value with merged context.
    """
    if key not in client_context:
        print(f"✗ No context for key '{key}'. Do a GET first.")
        return
    
    print(f"Resolving conflict for '{key}' with value '{chosen_value}'")
    put(address, key, chosen_value, client_context[key])


def interactive_mode(address):
    """Interactive client for testing."""
    print(f"\n{'='*60}")
    print(f"DynamoDB Client with Vector Clocks")
    print(f"Connected to {address}")
    print(f"{'='*60}")
    print("Commands:")
    print("  put <key> <value>       - Store a key-value pair")
    print("  get <key>               - Retrieve a value")
    print("  resolve <key> <value>   - Resolve conflict with chosen value")
    print("  context                 - Show stored contexts")
    print("  quit                    - Exit")
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
                value = " ".join(cmd[2:])
                
                # Use stored context if available
                context = client_context.get(key)
                put(address, key, value, context)
            
            elif operation == "get":
                if len(cmd) < 2:
                    print("Usage: get <key>")
                    continue
                key = cmd[1]
                get(address, key)
            
            elif operation == "resolve":
                if len(cmd) < 3:
                    print("Usage: resolve <key> <value>")
                    continue
                key = cmd[1]
                value = " ".join(cmd[2:])
                resolve_conflict(address, key, value)
            
            elif operation == "context":
                if client_context:
                    print("Stored contexts:")
                    for k, vc in client_context.items():
                        print(f"  {k}: {dict(vc.clock)}")
                else:
                    print("No contexts stored yet")
            
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