import bisect
import hashlib
from concurrent import futures
import grpc
import dynamo_pb2
import dynamo_pb2_grpc
from vector_clock import VectorClock, VersionedValue, resolve_conflicts


def hash_string(input_string: str) -> int:
    """MD5 hash function returning integer."""
    return int(hashlib.md5(input_string.encode()).hexdigest(), 16)


class HashRing:
    def __init__(self, virtual_nodes=100):
        self.nodes = {}  # of the form node -> {capacity, address}
        self.ring = []   # of the form (pos, node)
        self.virtual_nodes = virtual_nodes

    def add_node(self, node: str, capacity: int, address: str):
        if node in self.nodes:
            raise ValueError("Node already exists")
        self.nodes[node] = {
            'capacity': capacity,
            'address': address
        }
        vnodes = int((capacity / 100) * self.virtual_nodes)
        for i in range(vnodes):
            vnode = f"{node}{i}"
            pos = hash_string(vnode)
            self.ring.append((pos, node))
        self.ring.sort(key=lambda x: x[0])
        print(f"Added node {node} with {vnodes} virtual nodes")
    
    def remove_node(self, node):
        if node not in self.nodes:
            raise ValueError("Node does not exist")
        del self.nodes[node]
        self.ring = [(pos, n) for pos, n in self.ring if n != node]
        print(f"Removed node {node}")
    
    def get_preference_list(self, key: str, N: int = 3):
        """Get N unique nodes responsible for this key."""
        if not self.ring:
            return []
        pos = hash_string(key)
        positions = [p for p, _ in self.ring]
        idx = bisect.bisect_right(positions, pos)
        if idx == len(self.ring):
            idx = 0
        preference_list = []
        seen = set()
        attempts = 0
        while len(preference_list) < N and attempts < len(self.ring):
            curr_index = (idx + attempts) % len(self.ring)
            node_id = self.ring[curr_index][1]
            if node_id not in seen:
                preference_list.append(node_id)
                seen.add(node_id)
            attempts += 1
        return preference_list
    
    def print_ring(self):
        """Print ring structure for debugging."""
        print("\n" + "="*50)
        print("RING STRUCTURE")
        print("="*50)
        print(f"Total physical nodes: {len(self.nodes)}")
        print(f"Total virtual nodes: {len(self.ring)}")
        print("\nNodes:")
        for node_id, info in self.nodes.items():
            vnode_count = sum(1 for _, nid in self.ring if nid == node_id)
            print(f"  {node_id}: capacity={info['capacity']}, "
                  f"address={info['address']}, vnodes={vnode_count}")
        print("="*50 + "\n")


class Node:
    def __init__(self, node_id, capacity, address):
        self.node_id = node_id
        self.capacity = capacity
        self.address = address
        self.ring = HashRing(virtual_nodes=100)
        
        # Storage now holds List[VersionedValue] for each key
        self.storage = {}  # key -> List[VersionedValue]
        
        self.server = None
        self.N = 3  # replication factor
        self.R = 2  # read quorum
        self.W = 2  # write quorum
        self.ring.add_node(self.node_id, self.capacity, self.address)
    
    def start(self, seed_address=None):
        """Start the gRPC server and optionally join via seed."""
        # Start gRPC server
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        
        # Register BOTH services (NodeService AND DynamoService)
        dynamo_pb2_grpc.add_NodeServiceServicer_to_server(
            NodeServicer(self), self.server
        )
        dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(
            DynamoServicer(self), self.server
        )
        
        self.server.add_insecure_port(self.address)
        self.server.start()
        print(f"✓ Node {self.node_id} gRPC server started on {self.address}")
        
        # Join cluster if seed provided
        if seed_address and seed_address != self.address:
            print(f"Joining ring via seed node at {seed_address}")
            self.send_join_request(seed_address)
    
    def stop(self, address=None):
        """Stop the node gracefully."""
        if address is None:
            address = self.address
        print(f"Node {self.node_id} at {address} stopping...")
        
        if self.server:
            self.server.stop(grace=2)
        
        print(f"Node {self.node_id} has left the ring.")
    
    # ============================================
    # JOIN Request Handling
    # ============================================
    
    def send_join_request(self, seed_address: str):
        try:
            channel = grpc.insecure_channel(seed_address)
            stub = dynamo_pb2_grpc.NodeServiceStub(channel)
            request = dynamo_pb2.JoinRequest(
                node_id=self.node_id,
                capacity=self.capacity,
                address=self.address
            )
            response = stub.Join(request)
            if response.success:
                print(f"✓ Successfully joined the ring via {seed_address}")
                for member in response.cluster_members:
                    if member.node_id != self.node_id:
                        self.ring.add_node(member.node_id, member.capacity, member.address)
                print("Current ring members after join:")
                self.ring.print_ring()
            else:
                print(f"✗ Failed to join the ring via {seed_address}")
            channel.close()
        except Exception as e:
            print(f"✗ Error joining ring via {seed_address}: {e}")

    def handle_join_request(self, node_id: str, capacity: int, address: str):
        self.ring.add_node(node_id, capacity, address)
        print(f"✓ Node {node_id} joined the ring.")
        
        # Fixed: Create NodeInfo protobuf objects, not dicts
        cluster_members = []
        for n, info in self.ring.nodes.items():
            cluster_members.append(
                dynamo_pb2.NodeInfo(
                    node_id=n,
                    capacity=info['capacity'],
                    address=info['address']
                )
            )
        
        return dynamo_pb2.JoinResponse(
            success=True,
            cluster_members=cluster_members
        )

    # ============================================
    # Local Storage Operations (with Vector Clocks)
    # ============================================
    
    def local_put(self, key: str, value: str, vector_clock: VectorClock):
        """
        Store key-value locally with vector clock.
        Handles conflict detection and keeps multiple versions if needed.
        """
        versioned = VersionedValue(key, value, vector_clock)
        
        if key not in self.storage:
            self.storage[key] = []
        
        # Add new version
        self.storage[key].append(versioned)
        
        # Remove dominated versions (keep only concurrent ones)
        self.storage[key] = resolve_conflicts(self.storage[key])
        
        print(f"  [LOCAL {self.node_id}] Stored {key} = {value} with VC={vector_clock}")
    
    def local_get(self, key: str) -> list:
        """Retrieve all versions of a key locally."""
        return self.storage.get(key, [])

    # ============================================
    # Coordinated PUT/GET with Replication and Vector Clocks
    # ============================================
    
    def coordinated_put(self, key: str, value: str, context: VectorClock = None):
        """
        Coordinate PUT operation with vector clocks.
        
        Args:
            key: Key to store
            value: Value to store
            context: Client's context (last known vector clock)
        
        Returns:
            (success, updated_vector_clock)
        """
        print(f"\n[COORDINATOR {self.node_id}] PUT {key} = {value}")
        
        preference_list = self.ring.get_preference_list(key, self.N)
        if not preference_list:
            print("✗ No nodes available to store the key.")
            return False, None
        
        print(f"  Preference list: {preference_list} (N={self.N})")
        
        # Create vector clock
        if context:
            # Client provided context, merge and increment
            vc = context.copy()
            vc.increment(self.node_id)
            print(f"  Context provided: {context}, updated to: {vc}")
        else:
            # New write, create fresh vector clock
            vc = VectorClock()
            vc.increment(self.node_id)
            print(f"  New vector clock: {vc}")
        
        # Replicate to N nodes
        success_count = 0
        for node_id in preference_list:
            try:
                if node_id == self.node_id:
                    # Local write
                    self.local_put(key, value, vc)
                    success_count += 1
                else:
                    # Remote write via gRPC
                    node_address = self.ring.nodes[node_id]['address']
                    channel = grpc.insecure_channel(node_address)
                    stub = dynamo_pb2_grpc.NodeServiceStub(channel)
                    
                    # Convert vector clock to proto
                    vc_proto = dynamo_pb2.VectorClock(clock=vc.to_dict())
                    request = dynamo_pb2.ReplicatePutRequest(
                        key=key,
                        value=value,
                        vector_clock=vc_proto
                    )
                    
                    response = stub.ReplicatePut(request, timeout=2)
                    
                    if response.success:
                        success_count += 1
                        print(f"  ✓ Replicated to {node_id}")
                    channel.close()
            except Exception as e:
                print(f"  ✗ Error replicating to node {node_id}: {e}")
        
        # Check write quorum
        if success_count >= self.W:
            print(f"  ✓ Write quorum achieved: {success_count}/{self.N} (W={self.W})")
            return True, vc
        else:
            print(f"  ✗ Write quorum failed: {success_count}/{self.N} (W={self.W})")
            return False, None
    
    def coordinated_get(self, key: str):
        """
        Coordinate GET operation with vector clocks.
        Returns all concurrent versions if conflicts exist.
        
        Returns:
            List of VersionedValue objects (may have multiple if conflict)
        """
        print(f"\n[COORDINATOR {self.node_id}] GET {key}")
        
        preference_list = self.ring.get_preference_list(key, self.N)
        if not preference_list:
            print("✗ No nodes available to retrieve the key.")
            return []
        
        print(f"  Preference list: {preference_list} (N={self.N})")
        
        # Collect all versions from replicas
        all_versions = []
        success_count = 0
        
        for node_id in preference_list:
            try:
                if node_id == self.node_id:
                    # Local read
                    versions = self.local_get(key)
                    if versions:
                        all_versions.extend(versions)
                        success_count += 1
                        print(f"  [LOCAL {self.node_id}] Found {len(versions)} version(s)")
                else:
                    # Remote read via gRPC
                    node_address = self.ring.nodes[node_id]['address']
                    channel = grpc.insecure_channel(node_address)
                    stub = dynamo_pb2_grpc.NodeServiceStub(channel)
                    
                    request = dynamo_pb2.ReplicateGetRequest(key=key)
                    response = stub.ReplicateGet(request, timeout=2)
                    
                    if response.found:
                        # Convert proto versions to VersionedValue objects
                        for vv_proto in response.values:
                            vc = VectorClock.from_dict(dict(vv_proto.vector_clock.clock))
                            all_versions.append(VersionedValue(key, vv_proto.value, vc))
                        success_count += 1
                        print(f"  ✓ {node_id} returned {len(response.values)} version(s)")
                    
                    channel.close()
                
                # Stop once we have R responses
                if success_count >= self.R:
                    break
                    
            except Exception as e:
                print(f"  ✗ Error retrieving from node {node_id}: {e}")
        
        # Check read quorum
        if success_count < self.R:
            print(f"  ✗ Read quorum failed: {success_count}/{self.R}")
            return []
        
        # Resolve conflicts (remove dominated versions)
        resolved = resolve_conflicts(all_versions)
        
        if len(resolved) > 1:
            print(f"  ⚠ CONFLICT DETECTED: {len(resolved)} concurrent versions!")
            for v in resolved:
                print(f"      - {v.value} with VC={v.vector_clock}")
        else:
            print(f"  ✓ No conflicts, returning 1 version")
        
        return resolved
    
    # ============================================
    # Handle Internal Replication Requests
    # ============================================
    
    def handle_replicate_put(self, key: str, value: str, vector_clock: VectorClock) -> bool:
        """Handle internal PUT replication request with vector clock."""
        try:
            self.local_put(key, value, vector_clock)
            return True
        except Exception as e:
            print(f"✗ Replication PUT failed: {e}")
            return False
    
    def handle_replicate_get(self, key: str):
        """Handle internal GET replication request. Returns list of versions."""
        return self.local_get(key)


# ============================================
# gRPC Service Implementations
# ============================================

class NodeServicer(dynamo_pb2_grpc.NodeServiceServicer):
    """Handles node-to-node gRPC requests."""
    
    def __init__(self, node: Node):
        self.node = node
    
    def Join(self, request, context):
        """Handle JOIN request from new node."""
        return self.node.handle_join_request(
            request.node_id,
            request.capacity,
            request.address
        )
    
    def ReplicatePut(self, request, context):
        """Handle internal PUT replication with vector clock."""
        # Convert proto vector clock to VectorClock object
        vc = VectorClock.from_dict(dict(request.vector_clock.clock))
        success = self.node.handle_replicate_put(request.key, request.value, vc)
        return dynamo_pb2.ReplicatePutResponse(success=success)
    
    def ReplicateGet(self, request, context):
        """Handle internal GET replication. Returns all versions."""
        versions = self.node.handle_replicate_get(request.key)
        
        if versions:
            # Convert VersionedValue objects to proto
            values_proto = []
            for v in versions:
                vc_proto = dynamo_pb2.VectorClock(clock=v.vector_clock.to_dict())
                vv_proto = dynamo_pb2.VersionedValue(
                    value=v.value,
                    vector_clock=vc_proto
                )
                values_proto.append(vv_proto)
            
            return dynamo_pb2.ReplicateGetResponse(found=True, values=values_proto)
        else:
            return dynamo_pb2.ReplicateGetResponse(found=False)


class DynamoServicer(dynamo_pb2_grpc.DynamoServiceServicer):
    """Handles client-facing gRPC requests."""
    
    def __init__(self, node: Node):
        self.node = node
    
    def Put(self, request, context):
        """Handle client PUT request with optional context."""
        # Convert context if provided
        vc_context = None
        if request.context and request.context.clock:
            vc_context = VectorClock.from_dict(dict(request.context.clock))
        
        success, updated_vc = self.node.coordinated_put(
            request.key,
            request.value,
            vc_context
        )
        
        if success and updated_vc:
            # Return updated vector clock to client
            vc_proto = dynamo_pb2.VectorClock(clock=updated_vc.to_dict())
            return dynamo_pb2.PutResponse(success=True, updated_clock=vc_proto)
        else:
            return dynamo_pb2.PutResponse(success=False)
    
    def Get(self, request, context):
        """Handle client GET request. May return multiple versions if conflict."""
        versions = self.node.coordinated_get(request.key)
        
        if versions:
            # Convert to proto
            values_proto = []
            for v in versions:
                vc_proto = dynamo_pb2.VectorClock(clock=v.vector_clock.to_dict())
                vv_proto = dynamo_pb2.VersionedValue(
                    value=v.value,
                    vector_clock=vc_proto
                )
                values_proto.append(vv_proto)
            
            return dynamo_pb2.GetResponse(found=True, values=values_proto)
        else:
            return dynamo_pb2.GetResponse(found=False)


# ============================================
# Main Entry Point
# ============================================

if __name__ == "__main__":
    import sys
    import time
    
    if len(sys.argv) < 4:
        print("\nUsage: python node.py <node_id> <capacity> <port> [seed_address]")
        print("\nExamples:")
        print("  Terminal 1 (seed): python node.py A 100 5000")
        print("  Terminal 2:        python node.py B 200 5001 localhost:5000")
        print("  Terminal 3:        python node.py C 150 5002 localhost:5000")
        sys.exit(1)
    
    node_id = sys.argv[1]
    capacity = int(sys.argv[2])
    port = int(sys.argv[3])
    seed_address = sys.argv[4] if len(sys.argv) > 4 else None
    
    address = f"localhost:{port}"
    
    # Create node
    node = Node(node_id, capacity, address)
    
    # Start node (and join if seed provided)
    node.start(seed_address)
    
    # Keep running
    print(f"\nNode {node_id} running... Press Ctrl+C to stop\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.stop()
        print("\nNode stopped")