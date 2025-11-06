import bisect
import hashlib
from concurrent import futures
import grpc
import dynamo_pb2
import dynamo_pb2_grpc
from vector_clock import VectorClock, VersionedValue, resolve_conflicts
import time
import threading
import random


def hash_string(input_string: str) -> int:
    """MD5 hash function returning integer."""
    return int(hashlib.md5(input_string.encode()).hexdigest(), 16)


class HashRing:
    def __init__(self, virtual_nodes=100):
        self.nodes = {}  # of the form node -> {capacity, address, last_seen, is_alive}
        self.ring = []   # of the form (pos, node)
        self.virtual_nodes = virtual_nodes
        self.lock = threading.RLock()  # Thread-safe for gossip updates

    def add_node(self, node: str, capacity: int, address: str):
        with self.lock:
            if node in self.nodes:
                # Update existing node
                self.nodes[node]['capacity'] = capacity
                self.nodes[node]['address'] = address
                self.nodes[node]['last_seen'] = time.time()
                self.nodes[node]['is_alive'] = True
                return
            
            self.nodes[node] = {
                'capacity': capacity,
                'address': address,
                'last_seen': time.time(),
                'is_alive': True
            }
            
            vnodes = int((capacity / 100) * self.virtual_nodes)
            for i in range(vnodes):
                vnode = f"{node}{i}"
                pos = hash_string(vnode)
                self.ring.append((pos, node))
            
            self.ring.sort(key=lambda x: x[0])
            print(f"Added node {node} with {vnodes} virtual nodes")
    
    def remove_node(self, node):
        with self.lock:
            if node not in self.nodes:
                raise ValueError("Node does not exist")
            del self.nodes[node]
            self.ring = [(pos, n) for pos, n in self.ring if n != node]
            print(f"Removed node {node}")
    
    def mark_node_dead(self, node):
        """Mark a node as dead (but don't remove from ring yet)."""
        with self.lock:
            if node in self.nodes:
                was_alive = self.nodes[node]['is_alive']
                self.nodes[node]['is_alive'] = False
                if was_alive:  # Only print if state changed
                    print(f"\n⚠️  [{time.strftime('%H:%M:%S')}] Node {node} marked as DEAD (no response for {self.FAILURE_THRESHOLD:.0f}s)")
    
    def mark_node_alive(self, node):
        """Mark a node as alive and update last_seen."""
        with self.lock:
            if node in self.nodes:
                was_dead = not self.nodes[node]['is_alive']
                self.nodes[node]['is_alive'] = True
                self.nodes[node]['last_seen'] = time.time()
                if was_dead:  # Only print if state changed (recovery!)
                    print(f"\n✅ [{time.strftime('%H:%M:%S')}] Node {node} is ALIVE again! (recovered)")
                    return True  # Signal that node recovered
        return False
    
    def update_from_gossip(self, remote_view: dict):
        """
        Merge remote ring view with local view.
        Take the most recent information for each node.
        """
        with self.lock:
            for node_id, remote_info in remote_view.items():
                if node_id not in self.nodes:
                    # Learn about new node
                    self.add_node(
                        node_id,
                        remote_info['capacity'],
                        remote_info['address']
                    )
                    print(f"  [GOSSIP] Learned about new node: {node_id}")
                else:
                    # Update if remote info is newer
                    local_info = self.nodes[node_id]
                    if remote_info['last_seen'] > local_info['last_seen']:
                        self.nodes[node_id].update(remote_info)
                        print(f"  [GOSSIP] Updated {node_id} from gossip")
    
    def get_preference_list(self, key: str, N: int = 3):
        """Get N unique ALIVE nodes responsible for this key."""
        with self.lock:
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
                # Only add alive nodes
                if node_id not in seen and self.nodes[node_id]['is_alive']:
                    preference_list.append(node_id)
                    seen.add(node_id)
                attempts += 1
            return preference_list
    
    def get_all_alive_nodes(self):
        """Get list of all alive node IDs."""
        with self.lock:
            return [nid for nid, info in self.nodes.items() if info['is_alive']]
    
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
        
        # Hints storage for temporary failures
        self.hints = {}    # key -> {'value': str, 'vector_clock': VC, 'intended_for': str, 'created_at': float}
        self.hints_lock = threading.RLock()
        
        self.server = None
        self.N = 3  # replication factor
        self.R = 2  # read quorum
        self.W = 2  # write quorum
        
        # Gossip configuration
        self.GOSSIP_INTERVAL = 1.0  # seconds
        self.FAILURE_THRESHOLD = 10.0  # seconds - mark dead if no response
        self.gossip_thread = None
        self.running = False
        
        self.ring.add_node(self.node_id, self.capacity, self.address)
    
    def start(self, seed_address=None):
        """Start the gRPC server and optionally join via seed."""
        self.running = True
        
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
        
        # Start gossip protocol
        self.gossip_thread = threading.Thread(target=self.gossip_routine, daemon=True)
        self.gossip_thread.start()
        print(f"✓ Gossip protocol started")
    
    def stop(self, address=None):
        """Stop the node gracefully."""
        self.running = False
        
        if address is None:
            address = self.address
        print(f"Node {self.node_id} at {address} stopping...")
        
        if self.server:
            self.server.stop(grace=2)
        
        print(f"Node {self.node_id} has left the ring.")
    
    # ============================================
    # Gossip Protocol
    # ============================================
    
    def gossip_routine(self):
        """
        Periodic gossip protocol to maintain ring consistency.
        Runs every GOSSIP_INTERVAL seconds.
        """
        print(f"[GOSSIP] Starting gossip routine for {self.node_id}")
        
        while self.running:
            try:
                # Get all alive peers (excluding self)
                peers = self.ring.get_all_alive_nodes()
                peers = [p for p in peers if p != self.node_id]
                
                if peers:
                    # Pick a random peer to gossip with
                    target = random.choice(peers)
                    target_addr = self.ring.nodes[target]['address']
                    
                    print(f"[GOSSIP {self.node_id}] Pinging {target}...", end=" ", flush=True)
                    
                    # Build my ring view
                    my_view = {}
                    with self.ring.lock:
                        for nid, info in self.ring.nodes.items():
                            my_view[nid] = dynamo_pb2.NodeInfo(
                                node_id=nid,
                                capacity=info['capacity'],
                                address=info['address'],
                                last_seen=int(info['last_seen']),
                                is_alive=info['is_alive']
                            )
                    
                    # Send gossip ping
                    channel = grpc.insecure_channel(target_addr)
                    stub = dynamo_pb2_grpc.NodeServiceStub(channel)
                    
                    request = dynamo_pb2.GossipPing(
                        sender_id=self.node_id,
                        ring_view=my_view
                    )
                    
                    response = stub.Gossip(request, timeout=0.5)
                    
                    print(f"✓")  # Success
                    
                    # Merge their view
                    remote_view = {}
                    for nid, node_info in response.ring_view.items():
                        remote_view[nid] = {
                            'capacity': node_info.capacity,
                            'address': node_info.address,
                            'last_seen': node_info.last_seen,
                            'is_alive': node_info.is_alive
                        }
                    
                    self.ring.update_from_gossip(remote_view)
                    self.ring.mark_node_alive(target)
                    
                    # Check if node recovered (was dead, now alive)
                    # If so, forward any hints we have for it
                    self.check_and_forward_hints(target)
                    
                    channel.close()
                else:
                    print(f"[GOSSIP {self.node_id}] No other nodes to gossip with")
                
                # Check for dead nodes (haven't heard from in FAILURE_THRESHOLD)
                with self.ring.lock:
                    current_time = time.time()
                    for nid, info in self.ring.nodes.items():
                        if nid != self.node_id and info['is_alive']:
                            time_since_seen = current_time - info['last_seen']
                            if time_since_seen > self.FAILURE_THRESHOLD:
                                self.ring.mark_node_dead(nid)
                
            except Exception as e:
                # Gossip errors are expected when nodes are down
                print(f"✗ (timeout/unreachable)")
            
            time.sleep(self.GOSSIP_INTERVAL)
    
    def handle_gossip_ping(self, sender_id: str, remote_view: dict):
        """
        Handle incoming gossip ping.
        Merge sender's view and return our view.
        """
        print(f"[GOSSIP {self.node_id}] Received ping from {sender_id}")
        
        # Merge sender's view into our ring
        self.ring.update_from_gossip(remote_view)
        self.ring.mark_node_alive(sender_id)
        
        # Build our ring view to send back
        my_view = {}
        with self.ring.lock:
            for nid, info in self.ring.nodes.items():
                my_view[nid] = dynamo_pb2.NodeInfo(
                    node_id=nid,
                    capacity=info['capacity'],
                    address=info['address'],
                    last_seen=int(info['last_seen']),
                    is_alive=info['is_alive']
                )
        
        return dynamo_pb2.GossipPong(ring_view=my_view)
    
    # ============================================
    # Hinted Handoff
    # ============================================
    
    def get_temp_replicas(self, key: str, count: int, exclude_nodes: list) -> list:
        """
        Get temporary replica nodes for hinted handoff.
        
        Args:
            key: The key being stored
            count: Number of temp replicas needed
            exclude_nodes: Nodes to exclude (primary preference list)
            
        Returns:
            List of (temp_node_id, intended_for_node_id) tuples
        """
        # Get extended preference list (walk further clockwise)
        extended_pref = self.ring.get_preference_list(key, N=10)
        
        temp_replicas = []
        exclude_set = set(exclude_nodes)
        
        for node_id in extended_pref:
            if node_id not in exclude_set and self.ring.nodes[node_id]['is_alive']:
                temp_replicas.append(node_id)
                exclude_set.add(node_id)  # Don't use same node twice
                
                if len(temp_replicas) >= count:
                    break
        
        return temp_replicas
    
    def store_hint(self, key: str, value: str, vector_clock: VectorClock, intended_for: str):
        """
        Store a hint for a temporarily down node.
        
        Args:
            key: Key to store
            value: Value to store
            vector_clock: Vector clock of the write
            intended_for: Node ID this hint is intended for
        """
        with self.hints_lock:
            self.hints[key] = {
                'value': value,
                'vector_clock': vector_clock,
                'intended_for': intended_for,
                'created_at': time.time()
            }
            print(f"  [HINT] Storing {key} as hint for node {intended_for}")
    
    def check_and_forward_hints(self, recovered_node_id: str):
        """
        Check if we have any hints for a recovered node and forward them.
        Called when gossip detects a node came back alive.
        
        Args:
            recovered_node_id: Node ID that just recovered
        """
        with self.hints_lock:
            hints_to_forward = []
            
            # Find all hints for this node
            for key, hint in self.hints.items():
                if hint['intended_for'] == recovered_node_id:
                    hints_to_forward.append((key, hint))
            
            if not hints_to_forward:
                return  # No hints for this node
            
            print(f"\n[HINT FORWARDING] Node {recovered_node_id} recovered, forwarding {len(hints_to_forward)} hint(s)...")
            
            # Forward each hint
            for key, hint in hints_to_forward:
                success = self.forward_hint_to_node(recovered_node_id, key, hint)
                if success:
                    # Delete hint after successful forward
                    del self.hints[key]
                    print(f"  ✓ Forwarded and deleted hint for {key}")
                else:
                    print(f"  ✗ Failed to forward hint for {key}, will retry later")
    
    def forward_hint_to_node(self, node_id: str, key: str, hint: dict) -> bool:
        """
        Forward a single hint to the recovered node.
        
        Args:
            node_id: Node to forward hint to
            key: Key of the hint
            hint: Hint data dict
            
        Returns:
            True if successful, False otherwise
        """
        try:
            node_address = self.ring.nodes[node_id]['address']
            channel = grpc.insecure_channel(node_address)
            stub = dynamo_pb2_grpc.NodeServiceStub(channel)
            
            # Build hint message
            vc_proto = dynamo_pb2.VectorClock(clock=hint['vector_clock'].to_dict())
            request = dynamo_pb2.HintedHandoff(
                key=key,
                value=hint['value'],
                vector_clock=vc_proto,
                intended_for=hint['intended_for']
            )
            
            response = stub.ReceiveHint(request, timeout=3)
            channel.close()
            
            return response.success
            
        except Exception as e:
            print(f"  ✗ Error forwarding hint to {node_id}: {e}")
            return False
    
    def receive_hint(self, key: str, value: str, vector_clock: VectorClock) -> bool:
        """
        Receive a hint from another node (I was down, now I'm back).
        Store it as regular data.
        
        Args:
            key: Key to store
            value: Value to store
            vector_clock: Vector clock of the value
            
        Returns:
            True if successful
        """
        try:
            print(f"[HINT RECEIVED] Storing {key} from hint")
            self.local_put(key, value, vector_clock)
            return True
        except Exception as e:
            print(f"✗ Failed to store hint: {e}")
            return False
    
    def get_cluster_members(self):
        """
        Return list of all cluster members.
        Used by clients to discover the cluster.
        """
        members = []
        with self.ring.lock:
            for node_id, info in self.ring.nodes.items():
                members.append(
                    dynamo_pb2.NodeInfo(
                        node_id=node_id,
                        capacity=info['capacity'],
                        address=info['address'],
                        last_seen=int(info['last_seen']),
                        is_alive=info['is_alive']
                    )
                )
        return members
    
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
    # WITH FORWARDING TO COORDINATOR
    # ============================================
    
    def coordinated_put(self, key: str, value: str, context: VectorClock = None):
        """
        Coordinate PUT operation with forwarding to coordinator.
        
        Design: Only the FIRST node in preference list coordinates.
        All other nodes forward to the coordinator.
        
        Args:
            key: Key to store
            value: Value to store
            context: Client's context (last known vector clock)
        
        Returns:
            (success, updated_vector_clock)
        """
        print(f"\n[{self.node_id}] Received PUT request: {key} = {value}")
        
        # Step 1: Calculate preference list
        preference_list = self.ring.get_preference_list(key, self.N)
        
        if not preference_list:
            print(f"✗ No nodes available to store the key.")
            return False, None
        
        print(f"  Preference list: {preference_list} (N={self.N})")
        
        # Step 2: Determine coordinator (first in preference list)
        coordinator = preference_list[0]
        
        # Step 3: Check if I am the coordinator
        if self.node_id != coordinator:
            # I'm NOT the coordinator - FORWARD to coordinator
            print(f"  [{self.node_id}] I'm not coordinator, forwarding to {coordinator}")
            return self.forward_put_to_coordinator(coordinator, key, value, context)
        
        # Step 4: I AM the coordinator - handle replication
        print(f"  [{self.node_id}] I AM coordinator, handling replication")
        
        # Create vector clock
        if context:
            vc = context.copy()
            vc.increment(self.node_id)
            print(f"  Context provided: {context}, updated to: {vc}")
        else:
            vc = VectorClock()
            vc.increment(self.node_id)
            print(f"  New vector clock: {vc}")
        
        # Replicate to N nodes (including myself)
        success_count = 0
        for node_id in preference_list:
            try:
                if node_id == self.node_id:
                    # Local write (I'm in preference list)
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
    
    def forward_put_to_coordinator(self, coordinator_id: str, key: str, value: str, context: VectorClock):
        """
        Forward PUT request to coordinator node.
        
        Args:
            coordinator_id: Node ID of coordinator
            key: Key to store
            value: Value to store
            context: Vector clock context
            
        Returns:
            (success, updated_vector_clock)
        """
        try:
            coordinator_address = self.ring.nodes[coordinator_id]['address']
            channel = grpc.insecure_channel(coordinator_address)
            stub = dynamo_pb2_grpc.NodeServiceStub(channel)
            
            # Build request
            request = dynamo_pb2.PutRequest(key=key, value=value)
            if context:
                vc_proto = dynamo_pb2.VectorClock(clock=context.to_dict())
                request.context.CopyFrom(vc_proto)
            
            # Forward to coordinator via CoordinatePut RPC
            response = stub.CoordinatePut(request, timeout=5)
            
            channel.close()
            
            if response.success:
                vc = VectorClock.from_dict(dict(response.updated_clock.clock)) if response.updated_clock else None
                print(f"  ✓ Coordinator {coordinator_id} handled successfully")
                return True, vc
            else:
                print(f"  ✗ Coordinator {coordinator_id} failed")
                return False, None
                
        except Exception as e:
            print(f"  ✗ Failed to forward to coordinator {coordinator_id}: {e}")
            return False, None
    
    def coordinated_get(self, key: str):
        """
        Coordinate GET operation with forwarding to coordinator.
        Returns all concurrent versions if conflicts exist.
        
        Returns:
            List of VersionedValue objects (may have multiple if conflict)
        """
        print(f"\n[{self.node_id}] Received GET request: {key}")
        
        # Step 1: Calculate preference list
        preference_list = self.ring.get_preference_list(key, self.N)
        
        if not preference_list:
            print("✗ No nodes available to retrieve the key.")
            return []
        
        print(f"  Preference list: {preference_list} (N={self.N})")
        
        # Step 2: Determine coordinator
        coordinator = preference_list[0]
        
        # Step 3: Check if I am the coordinator
        if self.node_id != coordinator:
            # Forward to coordinator
            print(f"  [{self.node_id}] I'm not coordinator, forwarding to {coordinator}")
            return self.forward_get_to_coordinator(coordinator, key)
        
        # Step 4: I AM the coordinator - handle read
        print(f"  [{self.node_id}] I AM coordinator, handling read")
        
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
    
    def forward_get_to_coordinator(self, coordinator_id: str, key: str):
        """
        Forward GET request to coordinator node.
        
        Args:
            coordinator_id: Node ID of coordinator
            key: Key to retrieve
            
        Returns:
            List of VersionedValue objects
        """
        try:
            coordinator_address = self.ring.nodes[coordinator_id]['address']
            channel = grpc.insecure_channel(coordinator_address)
            stub = dynamo_pb2_grpc.NodeServiceStub(channel)
            
            request = dynamo_pb2.GetRequest(key=key)
            response = stub.CoordinateGet(request, timeout=5)
            
            channel.close()
            
            if response.found:
                # Convert proto to VersionedValue objects
                results = []
                for vv_proto in response.values:
                    vc = VectorClock.from_dict(dict(vv_proto.vector_clock.clock))
                    results.append(VersionedValue(key, vv_proto.value, vc))
                
                print(f"  ✓ Coordinator {coordinator_id} returned {len(results)} version(s)")
                return results
            else:
                print(f"  ✗ Coordinator {coordinator_id}: key not found")
                return []
                
        except Exception as e:
            print(f"  ✗ Failed to forward to coordinator {coordinator_id}: {e}")
            return []
    
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
    
    def Gossip(self, request, context):
        """Handle gossip ping from another node."""
        # Convert proto to dict
        remote_view = {}
        for nid, node_info in request.ring_view.items():
            remote_view[nid] = {
                'capacity': node_info.capacity,
                'address': node_info.address,
                'last_seen': node_info.last_seen,
                'is_alive': node_info.is_alive
            }
        
        return self.node.handle_gossip_ping(request.sender_id, remote_view)
    
    def GetCluster(self, request, context):
        """Handle GetCluster request from clients."""
        members = self.node.get_cluster_members()
        return dynamo_pb2.GetClusterResponse(members=members)
    
    def CoordinatePut(self, request, context):
        """
        Handle forwarded PUT request (internal node-to-node).
        This node is the coordinator for this key.
        """
        # Convert context if provided
        vc_context = None
        if request.context and request.context.clock:
            vc_context = VectorClock.from_dict(dict(request.context.clock))
        
        # Coordinate the PUT (this node is coordinator)
        success, updated_vc = self.node.coordinated_put(
            request.key,
            request.value,
            vc_context
        )
        
        if success and updated_vc:
            vc_proto = dynamo_pb2.VectorClock(clock=updated_vc.to_dict())
            return dynamo_pb2.PutResponse(success=True, updated_clock=vc_proto)
        else:
            return dynamo_pb2.PutResponse(success=False)
    
    def CoordinateGet(self, request, context):
        """
        Handle forwarded GET request (internal node-to-node).
        This node is the coordinator for this key.
        """
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