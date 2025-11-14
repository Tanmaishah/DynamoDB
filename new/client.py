"""
Smart DynamoDB Client with:
- Cluster discovery from seed nodes
- Random node selection for load balancing
- Automatic retry on node failure
- Periodic refresh (every 60s) to discover new nodes
- Vector clock support
"""

import grpc
import dynamo_pb2
import dynamo_pb2_grpc
import sys
import time
import threading
import random


class DynamoClient:
    """
    Smart DynamoDB client with cluster awareness.
    """
    
    def __init__(self, seed_addresses):
        """
        Initialize client with seed node addresses.
        
        Args:
            seed_addresses: List of seed addresses ["localhost:5000", "localhost:5001"]
        """
        self.seeds = seed_addresses
        self.cluster_nodes = []
        self.cluster_lock = threading.Lock()
        self.context = {}  # Store vector clocks for keys
        
        # Refresh configuration
        self.refresh_interval = 60  # 60 seconds
        self.running = True
        self.refresh_thread = None
        
        # Bootstrap - discover cluster
        print(f"🔍 Bootstrapping from seeds: {self.seeds}")
        self.bootstrap()
        
        # Start background refresh
        self.start_refresh_thread()
    
    def bootstrap(self):
        """
        Initial cluster discovery from seed nodes.
        Tries each seed until one responds.
        """
        for seed in self.seeds:
            try:
                print(f"  Trying seed {seed}...", end=" ")
                members = self.get_cluster_from(seed)
                
                with self.cluster_lock:
                    self.cluster_nodes = members
                
                print(f"✓")
                print(f"✓ Discovered {len(members)} nodes: {members}")
                return  # Success!
                
            except Exception as e:
                print(f"✗ ({e})")
                continue
        
        # All seeds failed
        raise Exception(f"❌ All seed nodes unreachable! Tried: {self.seeds}")
    
    def get_cluster_from(self, node_address):
        """
        Query a node for cluster membership.
        
        Args:
            node_address: Address of node to query
            
        Returns:
            List of node addresses
        """
        channel = grpc.insecure_channel(node_address)
        stub = dynamo_pb2_grpc.NodeServiceStub(channel)
        
        request = dynamo_pb2.GetClusterRequest()
        response = stub.GetCluster(request, timeout=3)
        
        channel.close()
        
        # Extract addresses of alive nodes
        addresses = [member.address for member in response.members if member.is_alive]
        return addresses
    
    def start_refresh_thread(self):
        """Start background thread for periodic cluster refresh."""
        self.refresh_thread = threading.Thread(
            target=self.refresh_loop,
            daemon=True
        )
        self.refresh_thread.start()
        print(f"✓ Background refresh enabled (every {self.refresh_interval}s)")
    
    def refresh_loop(self):
        """Background thread - refreshes cluster view periodically."""
        while self.running:
            time.sleep(self.refresh_interval)
            self.refresh_cluster()
    
    def refresh_cluster(self):
        """
        Refresh cluster view from any alive node.
        Called periodically by background thread.
        """
        print(f"\n[REFRESH {time.strftime('%H:%M:%S')}] Updating cluster view...")
        
        with self.cluster_lock:
            current_nodes = self.cluster_nodes.copy()
        
        if not current_nodes:
            print(f"[REFRESH] No known nodes, trying seeds...")
            current_nodes = self.seeds
        
        # Try a few random nodes
        nodes_to_try = random.sample(current_nodes, min(3, len(current_nodes)))
        
        for node in nodes_to_try:
            try:
                new_members = self.get_cluster_from(node)
                
                with self.cluster_lock:
                    old_count = len(self.cluster_nodes)
                    old_nodes = set(self.cluster_nodes)
                    self.cluster_nodes = new_members
                    new_nodes = set(self.cluster_nodes)
                
                # Report changes
                added = new_nodes - old_nodes
                removed = old_nodes - new_nodes
                
                if added or removed:
                    print(f"[REFRESH] ✓ Cluster changed:")
                    if added:
                        print(f"  + Added: {added}")
                    if removed:
                        print(f"  - Removed: {removed}")
                    print(f"  Total: {len(new_members)} nodes")
                else:
                    print(f"[REFRESH] ✓ No changes ({len(new_members)} nodes)")
                
                return  # Success
                
            except Exception as e:
                continue
        
        # All nodes failed, try seeds as last resort
        print(f"[REFRESH] Current nodes failed, trying seeds...")
        for seed in self.seeds:
            try:
                new_members = self.get_cluster_from(seed)
                with self.cluster_lock:
                    self.cluster_nodes = new_members
                print(f"[REFRESH] ✓ Refreshed from seed ({len(new_members)} nodes)")
                return
            except:
                continue
        
        print(f"[REFRESH] ✗ Failed to refresh (keeping old view)")
    
    def put(self, key, value, context=None):
        """
        PUT key-value with automatic node selection and retry.
        
        Args:
            key: Key to store
            value: Value to store
            context: Optional vector clock context
            
        Returns:
            True if successful, False otherwise
        """
        with self.cluster_lock:
            nodes = self.cluster_nodes.copy()
        
        if not nodes:
            print("✗ No nodes available!")
            return False
        
        # Shuffle for random selection
        random.shuffle(nodes)
        
        # Try each node
        for i, node_addr in enumerate(nodes):
            try:
                print(f"  Trying {node_addr}...", end=" ", flush=True)
                
                channel = grpc.insecure_channel(node_addr)
                stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
                
                request = dynamo_pb2.PutRequest(key=key, value=value)
                if context:
                    request.context.CopyFrom(context)
                
                response = stub.Put(request, timeout=5)
                
                channel.close()
                
                if response.success:
                    print(f"✓")
                    print(f"✓ PUT successful: {key} = {value}")
                    
                    # Store updated vector clock
                    if response.updated_clock:
                        self.context[key] = response.updated_clock
                        print(f"  Updated context: {dict(response.updated_clock.clock)}")
                    
                    return True
                else:
                    print(f"✗ (write failed)")
                    
            except Exception as e:
                print(f"✗ (timeout/error)")
                continue
        
        # All nodes failed - try emergency refresh
        print(f"\n❌ All {len(nodes)} nodes failed!")
        print(f"🔄 Emergency refresh...")
        
        self.refresh_cluster()
        
        with self.cluster_lock:
            nodes = self.cluster_nodes.copy()
        
        # One more try with fresh view
        for node_addr in random.sample(nodes, min(2, len(nodes))):
            try:
                channel = grpc.insecure_channel(node_addr)
                stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
                
                request = dynamo_pb2.PutRequest(key=key, value=value)
                response = stub.Put(request, timeout=2)
                
                channel.close()
                
                if response.success:
                    print(f"✓ PUT successful (after refresh)")
                    return True
                    
            except:
                continue
        
        print("❌ PUT failed completely")
        return False
    
    def get(self, key):
        """
        GET key with automatic node selection and retry.
        
        Args:
            key: Key to retrieve
            
        Returns:
            List of (value, vector_clock) tuples or None
        """
        with self.cluster_lock:
            nodes = self.cluster_nodes.copy()
        
        if not nodes:
            print("✗ No nodes available!")
            return None
        
        random.shuffle(nodes)
        
        for node_addr in nodes:
            try:
                # print(f"  Trying {node_addr}...", end=" ", flush=True)
                
                channel = grpc.insecure_channel(node_addr)
                stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
                
                request = dynamo_pb2.GetRequest(key=key)
                response = stub.Get(request, timeout=2)
                
                channel.close()
                
                if response.found:
                    print(f"✓")
                    
                    if len(response.values) == 1:
                        v = response.values[0]
                        print(f"✓ GET successful: {key} = {v.value}")
                        print(f"  Vector clock: {dict(v.vector_clock.clock)}")
                        
                        # Store context
                        self.context[key] = v.vector_clock
                        
                        return [(v.value, dict(v.vector_clock.clock))]
                    else:
                        # Multiple versions (conflict!)
                        print(f"⚠ CONFLICT: {len(response.values)} versions found:")
                        results = []
                        for i, v in enumerate(response.values, 1):
                            vc = dict(v.vector_clock.clock)
                            print(f"  [{i}] {v.value} (VC: {vc})")
                            results.append((v.value, vc))
                        
                        print(f"\n💡 Use 'resolve <key> <value>' to resolve conflict")
                        return results
                else:
                    print(f"✗ (not found)")
                    
            except Exception as e:
                print(f"✗ (timeout/error)")
                continue
        
        print(f"❌ Key '{key}' not found or all nodes unreachable")
        return None
    
    def resolve(self, key, chosen_value):
        """
        Resolve conflict by writing chosen value with merged context.
        
        Args:
            key: Key with conflict
            chosen_value: Value chosen to resolve conflict
        """
        if key not in self.context:
            print(f"✗ No context for '{key}'. Do a GET first.")
            return
        
        print(f"🔧 Resolving conflict for '{key}' with value '{chosen_value}'")
        self.put(key, chosen_value, self.context[key])
    
    def close(self):
        """Cleanup - stop background thread."""
        self.running = False
        if self.refresh_thread:
            self.refresh_thread.join(timeout=2)
        print("\n👋 Client closed")


def interactive_mode(seeds):
    """Interactive shell for DynamoDB client."""
    try:
        client = DynamoClient(seeds)
    except Exception as e:
        print(f"\n❌ Failed to initialize client: {e}")
        return
    
    print(f"\n{'='*60}")
    print(f"  DynamoDB Interactive Client")
    print(f"{'='*60}")
    print("Commands:")
    print("  put <key> <value>       - Store a key-value pair")
    print("  get <key>               - Retrieve a value")
    print("  resolve <key> <value>   - Resolve conflict")
    print("  nodes                   - Show known nodes")
    print("  refresh                 - Force cluster refresh")
    print("  quit                    - Exit")
    print(f"{'='*60}\n")
    
    try:
        while True:
            try:
                cmd = input("dynamo> ").strip().split(maxsplit=2)
                
                if not cmd:
                    continue
                
                operation = cmd[0].lower()
                
                if operation in ["quit", "exit", "q"]:
                    break
                
                elif operation == "put":
                    if len(cmd) < 3:
                        print("Usage: put <key> <value>")
                        continue
                    key = cmd[1]
                    value = cmd[2]
                    context = client.context.get(key)
                    client.put(key, value, context)
                
                elif operation == "get":
                    if len(cmd) < 2:
                        print("Usage: get <key>")
                        continue
                    key = cmd[1]
                    client.get(key)
                
                elif operation == "resolve":
                    if len(cmd) < 3:
                        print("Usage: resolve <key> <value>")
                        continue
                    key = cmd[1]
                    value = cmd[2]
                    client.resolve(key, value)
                
                elif operation == "nodes":
                    with client.cluster_lock:
                        nodes = client.cluster_nodes.copy()
                    print(f"\nKnown nodes ({len(nodes)}):")
                    for i, node in enumerate(nodes, 1):
                        print(f"  {i}. {node}")
                
                elif operation == "refresh":
                    client.refresh_cluster()
                
                else:
                    print(f"Unknown command: {operation}")
                    
            except KeyboardInterrupt:
                print()  # New line
                continue
            except Exception as e:
                print(f"Error: {e}")
        
    finally:
        client.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python client.py <seed1> [seed2] [seed3] ...")
        print("\nExamples:")
        print("  python client.py localhost:5000")
        print("  python client.py localhost:5000 localhost:5001")
        print("  python client.py localhost:5000,localhost:5001,localhost:5002")
        sys.exit(1)
    
    # Parse seed addresses
    seeds = []
    for arg in sys.argv[1:]:
        # Support comma-separated or space-separated
        seeds.extend(arg.split(','))
    
    seeds = [s.strip() for s in seeds if s.strip()]
    
    if not seeds:
        print("Error: No seed addresses provided")
        sys.exit(1)
    
    print(f"\n🚀 Starting DynamoDB client...")
    interactive_mode(seeds)