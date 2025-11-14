import bisect
import hashlib
from concurrent import futures
import grpc
import dynamo_pb2
import dynamo_pb2_grpc
from vector_clock import VectorClock, VersionedValue, resolve_conflicts
from merkle_tree import MerkleTree
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
                    print(f"\n⚠️  [{time.strftime('%H:%M:%S')}] Node {node} marked as DEAD")
    
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
                    # Learned about new node (silent)
                else:
                    # Update if remote info is newer
                    local_info = self.nodes[node_id]
                    if remote_info['last_seen'] > local_info['last_seen']:
                        self.nodes[node_id].update(remote_info)
                        # Updated from gossip (silent)
    
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
    
    def get_preference_list_with_dead(self, key: str, N: int = 3):
        """
        Get N unique nodes responsible for this key, INCLUDING dead nodes.
        Used for hinted handoff - we want to try dead nodes and store hints.
        """
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
                # Add ALL nodes (dead or alive) for hinted handoff
                if node_id not in seen:
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
        self.GOSSIP_INTERVAL = 2.0  # 2 seconds - reduces network traffic for larger clusters
        self.FAILURE_THRESHOLD = 10.0  # 10 seconds - mark dead if no response
        self.gossip_thread = None
        self.running = False
        
        # Anti-entropy configuration
        self.ANTI_ENTROPY_INTERVAL = 180  # 10 seconds for testing (change to 3600 for production)
        self.anti_entropy_thread = None
        
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
        
        # Start anti-entropy protocol
        self.anti_entropy_thread = threading.Thread(target=self.anti_entropy_routine, daemon=True)
        self.anti_entropy_thread.start()
        print(f"✓ Anti-entropy (Merkle tree sync) started (runs every {self.ANTI_ENTROPY_INTERVAL/60:.0f} minutes)")
    
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
        
        gossip_count = 0
        LOG_EVERY_N_GOSSIPS = 10  # Print logs every 10 gossips (20 seconds if interval=2s)
        
        while self.running:
            try:
                gossip_count += 1
                should_log = (gossip_count % LOG_EVERY_N_GOSSIPS == 0)
                
                # Print alive nodes every 15 seconds
                if should_log:
                    alive = self.ring.get_all_alive_nodes()
                    print(f"\n[GOSSIP {self.node_id}] Alive nodes: {alive}\n")
                
                # Get all alive peers (excluding self)
                peers = self.ring.get_all_alive_nodes()
                peers = [p for p in peers if p != self.node_id]
                
                if peers:
                    # Pick a random peer to gossip with
                    target = random.choice(peers)
                    target_addr = self.ring.nodes[target]['address']
                    
                    if should_log:
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
                    
                    if should_log:
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
                    if should_log:
                        print(f"[GOSSIP {self.node_id}] No other nodes to gossip with")
                
            except Exception as e:
                # Gossip errors are expected when nodes are down
                if should_log:
                    print(f"✗ (timeout/unreachable)")
            
            # CRITICAL: Check for dead nodes (OUTSIDE try-except so it always runs)
            # This must run even if gossip ping fails!
            with self.ring.lock:
                current_time = time.time()
                for nid, info in self.ring.nodes.items():
                    if nid != self.node_id and info['is_alive']:
                        time_since_seen = current_time - info['last_seen']
                        if time_since_seen > self.FAILURE_THRESHOLD:
                            self.ring.mark_node_dead(nid)
            
            time.sleep(self.GOSSIP_INTERVAL)
    
    def handle_gossip_ping(self, sender_id: str, remote_view: dict):
        """
        Handle incoming gossip ping.
        Merge sender's view and return our view.
        """
        # Merge sender's view into our ring (silent, happens every second)
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
            for key, hint in list(self.hints.items()):
                if hint['intended_for'] == recovered_node_id:
                    hints_to_forward.append((key, hint))
            
            if not hints_to_forward:
                return  # No hints for this node
            
            print(f"  [HINT FORWARDING] Found {len(hints_to_forward)} hint(s) for {recovered_node_id}")
            
            # Forward hints to recovered node
            node_address = self.ring.nodes[recovered_node_id]['address']
            success_count = 0
            
            for key, hint in hints_to_forward:
                try:
                    channel = grpc.insecure_channel(node_address)
                    stub = dynamo_pb2_grpc.NodeServiceStub(channel)
                    
                    vc_proto = dynamo_pb2.VectorClock(clock=hint['vector_clock'].to_dict())
                    request = dynamo_pb2.ReplicatePutRequest(
                        key=key,
                        value=hint['value'],
                        vector_clock=vc_proto
                    )
                    
                    response = stub.ReplicatePut(request, timeout=1)
                    
                    if response.success:
                        success_count += 1
                        # Remove hint after successful forward
                        del self.hints[key]
                    
                    channel.close()
                    
                except Exception as e:
                    print(f"  ✗ Failed to forward hint {key} to {recovered_node_id}: {e}")
            
            if success_count > 0:
                print(f"  ✓ Successfully forwarded {success_count}/{len(hints_to_forward)} hints to {recovered_node_id}")
    
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
        
        # Silent - only log errors
    
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
        
        # Step 1: Calculate preference list (INCLUDING dead nodes for hinted handoff)
        preference_list = self.ring.get_preference_list_with_dead(key, self.N)
        
        if not preference_list:
            print(f"✗ No nodes available to store the key.")
            return False, None
        
        print(f"  Preference list: {preference_list} (N={self.N})")
        
        # Step 2: Determine coordinator (first ALIVE node in preference list)
        coordinator = None
        for node_id in preference_list:
            if self.ring.nodes[node_id]['is_alive']:
                coordinator = node_id
                break
        
        if not coordinator:
            print(f"✗ No alive coordinator found in preference list!")
            return False, None
        
        print(f"  Coordinator: {coordinator}")
        
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
        
        # Replicate to N nodes (including myself) - IN PARALLEL!
        success_count = 0
        failed_nodes = []
        
        def replicate_to_node(node_id, results):
            """Helper function to replicate to a single node."""
            try:
                if node_id == self.node_id:
                    # Local write (I'm in preference list)
                    self.local_put(key, value, vc)
                    results[node_id] = {'success': True, 'error': None}
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
                    
                    response = stub.ReplicatePut(request, timeout=0.1)
                    
                    if response.success:
                        results[node_id] = {'success': True, 'error': None}
                    else:
                        results[node_id] = {'success': False, 'error': 'Replication failed'}
                    
                    channel.close()
            except Exception as e:
                results[node_id] = {'success': False, 'error': str(e)}
        
        # Replicate in parallel using threads
        results = {}
        threads = []
        
        for node_id in preference_list:
            t = threading.Thread(
                target=replicate_to_node,
                args=(node_id, results),
                daemon=True
            )
            threads.append(t)
            t.start()
        
        # Wait for all threads to complete (with single timeout for all)
        start_wait = time.time()
        for t in threads:
            remaining_timeout = max(0.15 - (time.time() - start_wait), 0)
            t.join(timeout=remaining_timeout)  # Share timeout across all threads
        
        # Count successes and failures
        for node_id in preference_list:
            if node_id in results:
                if results[node_id]['success']:
                    success_count += 1
                    if node_id != self.node_id:
                        print(f"  ✓ Replicated to {node_id}")
                else:
                    failed_nodes.append(node_id)
                    print(f"  ✗ Failed to replicate to {node_id}: {results[node_id]['error']}")
            else:
                # Thread didn't complete in time
                failed_nodes.append(node_id)
                print(f"  ✗ Timeout replicating to {node_id}")
        
        # Hinted Handoff: Store hints for failed nodes
        if failed_nodes and success_count >= self.W:
            print(f"  [HINTED HANDOFF] {len(failed_nodes)} node(s) failed, storing hints")
            for failed_node in failed_nodes:
                self.store_hint(key, value, vc, failed_node)
        
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
            response = stub.CoordinatePut(request, timeout=0.3)  # Fast fail for dead coordinators
            
            channel.close()
            
            if response.success:
                vc = VectorClock.from_dict(dict(response.updated_clock.clock)) if response.updated_clock else None
                print(f"  ✓ Coordinator {coordinator_id} handled successfully")
                return True, vc
            else:
                print(f"  ✗ Coordinator {coordinator_id} failed")
                return False, None
                
        except Exception as e:
            # Coordinator is unreachable - mark as dead immediately and retry
            print(f"  ✗ Coordinator {coordinator_id} unreachable: {e}")
            print(f"  🔄 Marking {coordinator_id} as dead and retrying with new coordinator...")
            self.ring.mark_node_dead(coordinator_id)
            
            # Retry with new coordinator (will pick next alive node)
            return self.coordinated_put(key, value, context)
    
    def coordinated_get(self, key: str):
        """
        Coordinate GET operation with forwarding to coordinator and READ REPAIR.
        Returns all concurrent versions if conflicts exist.
        
        Read Repair:
        - Contacts ALL N replicas (not just R)
        - Detects inconsistencies between replicas
        - Asynchronously repairs outdated/missing replicas
        
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
        
        # Step 4: I AM the coordinator - handle read WITH READ REPAIR
        print(f"  [{self.node_id}] I AM coordinator, handling read with read repair")
        
        # Collect responses from ALL replicas (not just R) for read repair
        all_responses = []  # List of {node_id, versions, found}
        
        for node_id in preference_list:
            try:
                if node_id == self.node_id:
                    # Local read
                    versions = self.local_get(key)
                    all_responses.append({
                        'node_id': node_id,
                        'versions': versions,
                        'found': len(versions) > 0
                    })
                    if versions:
                        print(f"  [LOCAL {self.node_id}] Found {len(versions)} version(s)")
                    else:
                        print(f"  [LOCAL {self.node_id}] Key not found")
                else:
                    # Remote read via gRPC
                    node_address = self.ring.nodes[node_id]['address']
                    channel = grpc.insecure_channel(node_address)
                    stub = dynamo_pb2_grpc.NodeServiceStub(channel)
                    
                    request = dynamo_pb2.ReplicateGetRequest(key=key)
                    response = stub.ReplicateGet(request, timeout=2)
                    
                    if response.found:
                        # Convert proto versions to VersionedValue objects
                        versions = []
                        for vv_proto in response.values:
                            vc = VectorClock.from_dict(dict(vv_proto.vector_clock.clock))
                            versions.append(VersionedValue(key, vv_proto.value, vc))
                        
                        all_responses.append({
                            'node_id': node_id,
                            'versions': versions,
                            'found': True
                        })
                        print(f"  ✓ {node_id} returned {len(versions)} version(s)")
                    else:
                        all_responses.append({
                            'node_id': node_id,
                            'versions': [],
                            'found': False
                        })
                        print(f"  ✗ {node_id} does not have key")
                    
                    channel.close()
                    
            except Exception as e:
                print(f"  ✗ Error retrieving from node {node_id}: {e}")
                all_responses.append({
                    'node_id': node_id,
                    'versions': [],
                    'found': False
                })
        
        # Check read quorum (need R successful responses)
        successful_responses = [r for r in all_responses if r['found']]
        if len(successful_responses) < self.R:
            print(f"  ✗ Read quorum failed: {len(successful_responses)}/{self.R}")
            
            # STILL trigger read repair to fix missing replicas!
            if successful_responses:
                all_versions = []
                for response in successful_responses:
                    all_versions.extend(response['versions'])
                
                # Deduplicate
                seen = {}
                deduplicated = []
                for v in all_versions:
                    key_tuple = (v.value, tuple(sorted(v.vector_clock.clock.items())))
                    if key_tuple not in seen:
                        seen[key_tuple] = True
                        deduplicated.append(v)
                
                resolved = resolve_conflicts(deduplicated)
                
                # Trigger read repair (even though quorum failed)
                if resolved:
                    print(f"  [READ REPAIR] Triggering repair despite quorum failure")
                    threading.Thread(
                        target=self.trigger_read_repair,
                        args=(key, resolved, all_responses),
                        daemon=True
                    ).start()
            
            return []  # Still return empty to client (quorum not met)
        
        # Collect all versions from successful responses
        all_versions = []
        for response in successful_responses:
            all_versions.extend(response['versions'])
        
        # Deduplicate: Remove identical versions (same value AND same vector clock)
        seen = {}
        deduplicated = []
        for v in all_versions:
            key_tuple = (v.value, tuple(sorted(v.vector_clock.clock.items())))
            if key_tuple not in seen:
                seen[key_tuple] = True
                deduplicated.append(v)
        
        # Additional deduplication: If multiple versions have same VALUE but different VCs,
        # and those VCs are concurrent, keep only one (they're semantically identical)
        value_groups = {}
        for v in deduplicated:
            if v.value not in value_groups:
                value_groups[v.value] = []
            value_groups[v.value].append(v)
        
        final_deduplicated = []
        for value, versions in value_groups.items():
            if len(versions) == 1:
                # Only one version with this value
                final_deduplicated.append(versions[0])
            else:
                # Multiple versions with same value - check if any dominates
                dominated = set()
                for i, v1 in enumerate(versions):
                    for j, v2 in enumerate(versions):
                        if i != j and self.vclock_dominates(v1.vector_clock, v2.vector_clock):
                            dominated.add(j)
                
                # Keep non-dominated versions
                for i, v in enumerate(versions):
                    if i not in dominated:
                        final_deduplicated.append(v)
                        break  # Only keep first non-dominated with this value
        
        # Resolve conflicts (remove dominated versions)
        resolved = resolve_conflicts(final_deduplicated)
        
        if len(resolved) > 1:
            print(f"  ⚠ CONFLICT DETECTED: {len(resolved)} concurrent versions!")
            for v in resolved:
                print(f"      - {v.value} with VC={v.vector_clock}")
        else:
            print(f"  ✓ No conflicts, returning 1 version")
        
        # CRITICAL: Trigger read repair ASYNCHRONOUSLY (non-blocking)
        # This happens AFTER we return to the client
        if resolved:
            threading.Thread(
                target=self.trigger_read_repair,
                args=(key, resolved, all_responses),
                daemon=True
            ).start()
        
        return resolved
    
    # ============================================
    # READ REPAIR Implementation
    # ============================================
    
    def trigger_read_repair(self, key: str, winning_versions: list, all_responses: list):
        """
        Asynchronous read repair - pushes winning versions to outdated replicas.
        
        Args:
            key: The key that was read
            winning_versions: List of VersionedValue objects that are current
            all_responses: List of all responses from replicas
        """
        print(f"\n[READ REPAIR {self.node_id}] Starting repair for key: {key}")
        
        for response in all_responses:
            node_id = response['node_id']
            node_versions = response['versions']
            node_found = response['found']
            
            # Don't skip self! We might need to repair ourselves if we just recovered
            # (e.g., we crashed, restarted empty, and are now coordinator)
            
            # Case 1: Node is missing the key entirely
            if not node_found:
                print(f"  [REPAIR] Node {node_id} is missing key, sending all versions")
                for winner in winning_versions:
                    if node_id == self.node_id:
                        # Repair ourselves locally (don't send over network)
                        self.local_put(winner.key, winner.value, winner.vector_clock)
                        print(f"  ✓ Repaired SELF: {winner.key} = {winner.value}")
                    else:
                        # Repair remote node
                        self.send_repair_to_node(node_id, key, winner)
                continue
            
            # Case 2: Node has outdated or different versions
            # Check if node needs repair by comparing with winners
            needs_repair = self.node_needs_repair(node_versions, winning_versions)
            
            if needs_repair:
                print(f"  [REPAIR] Node {node_id} has outdated version, updating")
                for winner in winning_versions:
                    # Only send if node doesn't already have this exact version
                    if not self.node_has_version(node_versions, winner):
                        if node_id == self.node_id:
                            # Repair ourselves locally
                            self.local_put(winner.key, winner.value, winner.vector_clock)
                            print(f"  ✓ Repaired SELF: {winner.key} = {winner.value}")
                        else:
                            # Repair remote node
                            self.send_repair_to_node(node_id, key, winner)
            else:
                # Silent if up-to-date
                pass
    
    def node_needs_repair(self, node_versions: list, winning_versions: list) -> bool:
        """
        Check if a node needs repair by comparing its versions with winners.
        
        Returns True if:
        - Node is missing any winning version
        - Node has a version that is dominated by a winner
        """
        # If node has no versions but there are winners, needs repair
        if not node_versions and winning_versions:
            return True
        
        # Check if node is missing any winning version
        for winner in winning_versions:
            if not self.node_has_version(node_versions, winner):
                return True
        
        # Check if node has any dominated (outdated) versions
        for node_ver in node_versions:
            for winner_ver in winning_versions:
                if node_ver.vector_clock != winner_ver.vector_clock:
                    # Check if winner dominates node's version
                    if self.vclock_dominates(winner_ver.vector_clock, node_ver.vector_clock):
                        return True
        
        return False
    
    def node_has_version(self, node_versions: list, target: VersionedValue) -> bool:
        """Check if node has a specific version (same vector clock and value)."""
        for v in node_versions:
            if v.vector_clock == target.vector_clock and v.value == target.value:
                return True
        return False
    
    def vclock_dominates(self, vc1: VectorClock, vc2: VectorClock) -> bool:
        """
        Check if vc1 dominates vc2 (vc1 is strictly newer).
        
        vc1 dominates vc2 if:
        - For all nodes, vc1[node] >= vc2[node]
        - For at least one node, vc1[node] > vc2[node]
        """
        at_least_one_greater = False
        
        all_nodes = set(vc1.clock.keys()) | set(vc2.clock.keys())
        
        for node in all_nodes:
            v1 = vc1.clock.get(node, 0)
            v2 = vc2.clock.get(node, 0)
            
            if v1 < v2:
                return False  # vc2 has something vc1 doesn't
            elif v1 > v2:
                at_least_one_greater = True
        
        return at_least_one_greater
    
    def send_repair_to_node(self, node_id: str, key: str, versioned_value: VersionedValue) -> bool:
        """
        Send a repair (PUT) to a specific node.
        
        Args:
            node_id: Node to repair
            key: Key to repair
            versioned_value: The correct version to send
            
        Returns:
            True if successful, False otherwise
        """
        try:
            node_address = self.ring.nodes[node_id]['address']
            channel = grpc.insecure_channel(node_address)
            stub = dynamo_pb2_grpc.NodeServiceStub(channel)
            
            # Send repair as a ReplicatePut
            vc_proto = dynamo_pb2.VectorClock(clock=versioned_value.vector_clock.to_dict())
            request = dynamo_pb2.ReplicatePutRequest(
                key=key,
                value=versioned_value.value,
                vector_clock=vc_proto
            )
            
            response = stub.ReplicatePut(request, timeout=2)
            channel.close()
            
            if response.success:
                print(f"  ✓ Repaired {node_id}: {key} = {versioned_value.value}")
                return True
            else:
                print(f"  ✗ Failed to repair {node_id}")
                return False
                
        except Exception as e:
            print(f"  ✗ Error repairing {node_id}: {e}")
            return False
    
    # ============================================
    # ANTI-ENTROPY (Merkle Tree Sync)
    # ============================================
    
    def anti_entropy_routine(self):
        """
        Periodic anti-entropy using Merkle trees.
        Runs every ANTI_ENTROPY_INTERVAL seconds (default: 1 hour).
        """
        print(f"[ANTI-ENTROPY] Starting anti-entropy routine for {self.node_id}")
        print(f"[ANTI-ENTROPY] Will run every {self.ANTI_ENTROPY_INTERVAL/60:.0f} minutes")
        
        while self.running:
            try:
                time.sleep(self.ANTI_ENTROPY_INTERVAL)
                
                print(f"\n{'='*60}")
                print(f"[ANTI-ENTROPY {self.node_id}] Starting sync round at {time.strftime('%H:%M:%S')}")
                print(f"{'='*60}")
                
                # Get my N-1 successors to sync with
                alive_nodes = self.ring.get_all_alive_nodes()
                if len(alive_nodes) <= 1:
                    print(f"[ANTI-ENTROPY {self.node_id}] No other nodes to sync with")
                    continue
                
                # Sort nodes by position in ring
                sorted_nodes = sorted(alive_nodes, key=lambda n: self.ring.nodes[n]['address'])
                my_idx = sorted_nodes.index(self.node_id)
                
                # Sync with next N-1 nodes
                successors_to_sync = []
                for i in range(1, min(self.N, len(sorted_nodes))):
                    successor_idx = (my_idx + i) % len(sorted_nodes)
                    successor = sorted_nodes[successor_idx]
                    if successor != self.node_id:
                        successors_to_sync.append(successor)
                
                print(f"[ANTI-ENTROPY {self.node_id}] Syncing with {len(successors_to_sync)} successor(s): {successors_to_sync}")
                
                # Sync with each successor
                for successor in successors_to_sync:
                    try:
                        self.compare_and_sync_with_node(successor)
                    except Exception as e:
                        print(f"[ANTI-ENTROPY {self.node_id}] ✗ Failed to sync with {successor}: {e}")
                
                print(f"[ANTI-ENTROPY {self.node_id}] Sync round complete")
                print(f"{'='*60}\n")
                
            except Exception as e:
                print(f"[ANTI-ENTROPY {self.node_id}] Error in anti-entropy routine: {e}")
    
    def get_all_responsible_ranges(self) -> list:
        """
        Get all hash ranges this node is responsible for (as coordinator or replica).
        
        Returns:
            List of (start, end) tuples representing hash ranges
        """
        responsible_ranges = []
        
        # Get all unique node positions
        node_positions = {}
        for pos, node_id in self.ring.ring:
            if node_id not in node_positions:
                node_positions[node_id] = pos
        
        # Sort by position
        sorted_positions = sorted(node_positions.items(), key=lambda x: x[1])
        
        # For each position, check if we're in the preference list
        for i, (node_id, pos) in enumerate(sorted_positions):
            # Calculate range for this position
            start_pos = sorted_positions[i-1][1] if i > 0 else sorted_positions[-1][1]
            end_pos = pos
            
            # Get preference list for this position
            # We need to walk clockwise from start_pos to find N nodes
            pref_list = self._get_preference_list_for_range(start_pos, end_pos)
            
            # If we're in the preference list, we're responsible for this range
            if self.node_id in pref_list:
                responsible_ranges.append((start_pos, end_pos))
        
        return responsible_ranges
    
    def _get_preference_list_for_range(self, start_pos: int, end_pos: int) -> list:
        """
        Get preference list for keys that hash to a specific range.
        This is similar to get_preference_list but for a position range.
        """
        # Find the node at end_pos (coordinator for this range)
        coordinator_idx = None
        for i, (pos, node_id) in enumerate(self.ring.ring):
            if pos == end_pos:
                coordinator_idx = i
                break
        
        if coordinator_idx is None:
            return []
        
        # Walk clockwise from coordinator to get N nodes
        pref_list = []
        seen = set()
        attempts = 0
        
        with self.ring.lock:
            while len(pref_list) < self.N and attempts < len(self.ring.ring):
                curr_idx = (coordinator_idx + attempts) % len(self.ring.ring)
                node_id = self.ring.ring[curr_idx][1]
                
                if node_id not in seen and self.ring.nodes[node_id]['is_alive']:
                    pref_list.append(node_id)
                    seen.add(node_id)
                
                attempts += 1
        
        return pref_list
    
    def get_shared_token_ranges(self, other_node_id: str) -> list:
        """
        Find hash ranges where both this node and other_node should have replicas.
        
        Args:
            other_node_id: ID of the other node
            
        Returns:
            List of (start, end) tuples representing shared hash ranges
        """
        shared_ranges = []
        
        # Get all unique node positions
        node_positions = {}
        for pos, node_id in self.ring.ring:
            if node_id not in node_positions:
                node_positions[node_id] = pos
        
        # Sort by position
        sorted_positions = sorted(node_positions.items(), key=lambda x: x[1])
        
        # For each position, check if BOTH nodes are in the preference list
        for i, (node_id, pos) in enumerate(sorted_positions):
            # Calculate range for this position
            start_pos = sorted_positions[i-1][1] if i > 0 else sorted_positions[-1][1]
            end_pos = pos
            
            # Get preference list for this range
            pref_list = self._get_preference_list_for_range(start_pos, end_pos)
            
            # If BOTH nodes are in the preference list, this is a shared range
            if self.node_id in pref_list and other_node_id in pref_list:
                shared_ranges.append((start_pos, end_pos))
        
        return shared_ranges
    
    def is_hash_in_ranges(self, key_hash: int, ranges: list) -> bool:
        """
        Check if a key hash falls within any of the given ranges.
        
        Args:
            key_hash: Hash value of the key
            ranges: List of (start, end) tuples
            
        Returns:
            True if key_hash is in any range
        """
        for start, end in ranges:
            if start < end:
                # Normal range
                if start <= key_hash < end:
                    return True
            else:
                # Wrap-around range (e.g., [400, 100] wraps around 0)
                if key_hash >= start or key_hash < end:
                    return True
        return False
    
    def build_merkle_tree_for_comparison(self, other_node_id: str) -> MerkleTree:
        """
        Build a Merkle tree containing only keys shared with another node.
        
        Args:
            other_node_id: ID of the node to compare with
            
        Returns:
            MerkleTree containing only shared keys
        """
        # Get shared token ranges
        shared_ranges = self.get_shared_token_ranges(other_node_id)
        
        # Filter keys to only those in shared ranges
        shared_keys = []
        for key, versions in self.storage.items():
            key_hash = hash_string(key)
            if self.is_hash_in_ranges(key_hash, shared_ranges):
                # This key should be on both nodes
                shared_keys.extend(versions)
        
        # Build Merkle tree from shared keys
        merkle_tree = MerkleTree(num_buckets=256)
        merkle_tree.build(shared_keys)
        
        return merkle_tree
    
    def compare_and_sync_with_node(self, other_node_id: str):
        """
        Compare Merkle trees with another node and sync differences.
        
        Args:
            other_node_id: ID of the node to sync with
        """
        print(f"\n[MERKLE SYNC {self.node_id} ↔ {other_node_id}] Starting comparison...")
        
        try:
            # Check if other node is alive
            if not self.ring.nodes[other_node_id]['is_alive']:
                print(f"  Skipping {other_node_id} (node is dead)")
                return
            
            # Get shared ranges
            shared_ranges = self.get_shared_token_ranges(other_node_id)
            
            if not shared_ranges:
                print(f"  No shared ranges with {other_node_id}, skipping")
                return
            
            print(f"  Shared ranges: {len(shared_ranges)} range(s)")
            
            # Build our Merkle tree for shared keys
            my_tree = self.build_merkle_tree_for_comparison(other_node_id)
            print(f"  Built local tree: {my_tree}")
            
            # Get the other node's tree
            # For now, we'll build it ourselves by requesting all keys in shared ranges
            # In production, we'd exchange tree structures, but for simplicity we'll
            # just compare keys directly
            
            # Request all keys in shared ranges from other node
            other_keys = self.request_keys_in_ranges(other_node_id, shared_ranges)
            
            # Build tree from other node's keys
            other_tree = MerkleTree(num_buckets=256)
            other_tree.build(other_keys)
            print(f"  Built remote tree: {other_tree}")
            
            # Compare trees
            if my_tree.get_root_hash() == other_tree.get_root_hash():
                print(f"  ✓ Trees match! No sync needed.")
                return
            
            print(f"  ✗ Trees differ! Finding differences...")
            
            # Find differing buckets
            differing_buckets = my_tree.find_differing_buckets(other_tree)
            print(f"  Found {len(differing_buckets)} differing bucket(s)")
            
            # Sync each differing bucket
            keys_synced = 0
            for bucket_idx in differing_buckets:
                my_keys = my_tree.get_keys_in_bucket(bucket_idx)
                other_keys_in_bucket = other_tree.get_keys_in_bucket(bucket_idx)
                
                # Compare and sync this bucket
                synced = self.sync_bucket(other_node_id, my_keys, other_keys_in_bucket)
                keys_synced += synced
            
            if keys_synced > 0:
                print(f"  ✓ Synced {keys_synced} key(s) with {other_node_id}")
            else:
                print(f"  ✓ No keys needed syncing (versions matched)")
            
        except Exception as e:
            print(f"  ✗ Error during Merkle sync: {e}")
            import traceback
            traceback.print_exc()
    
    def request_keys_in_ranges(self, node_id: str, ranges: list) -> list:
        """
        Request all keys in specific hash ranges from another node.
        
        Args:
            node_id: Node to request from
            ranges: List of (start, end) hash ranges
            
        Returns:
            List of VersionedValue objects from that node
        """
        all_keys = []
        
        try:
            node_address = self.ring.nodes[node_id]['address']
            channel = grpc.insecure_channel(node_address)
            stub = dynamo_pb2_grpc.NodeServiceStub(channel)
            
            # For simplicity, we'll request ALL keys and filter locally
            # In production, we'd send the ranges and have the other node filter
            request = dynamo_pb2.GetAllKeysRequest()
            response = stub.GetAllKeys(request, timeout=10)
            
            # Convert proto to VersionedValue objects
            for key_data in response.keys:
                for vv_proto in key_data.versions:
                    vc = VectorClock.from_dict(dict(vv_proto.vector_clock.clock))
                    vv = VersionedValue(key_data.key, vv_proto.value, vc)
                    
                    # Check if this key is in our shared ranges
                    key_hash = hash_string(vv.key)
                    if self.is_hash_in_ranges(key_hash, ranges):
                        all_keys.append(vv)
            
            channel.close()
            
        except Exception as e:
            # Simplified error (node likely dead)
            print(f"  ✗ Cannot reach {node_id} (connection failed)")
        
        return all_keys
    
    def sync_bucket(self, other_node_id: str, my_keys: list, other_keys: list) -> int:
        """
        Sync a single bucket of keys with another node.
        Compares vector clocks and updates both nodes as needed.
        
        Args:
            other_node_id: Node to sync with
            my_keys: VersionedValues I have in this bucket
            other_keys: VersionedValues other node has in this bucket
            
        Returns:
            Number of keys synced
        """
        synced_count = 0
        
        # Build maps for easier lookup
        my_map = {vv.key: vv for vv in my_keys}
        other_map = {vv.key: vv for vv in other_keys}
        
        # All unique keys
        all_keys = set(my_map.keys()) | set(other_map.keys())
        
        for key in all_keys:
            my_version = my_map.get(key)
            other_version = other_map.get(key)
            
            # Case 1: I have it, they don't
            if my_version and not other_version:
                self.send_repair_to_node(other_node_id, key, my_version)
                synced_count += 1
            
            # Case 2: They have it, I don't
            elif other_version and not my_version:
                self.local_put(other_version.key, other_version.value, other_version.vector_clock)
                synced_count += 1
            
            # Case 3: Both have it - compare vector clocks
            elif my_version and other_version:
                if my_version.vector_clock != other_version.vector_clock:
                    # Determine which version is newer
                    if self.vclock_dominates(my_version.vector_clock, other_version.vector_clock):
                        # My version is newer, send to them
                        self.send_repair_to_node(other_node_id, key, my_version)
                        synced_count += 1
                    elif self.vclock_dominates(other_version.vector_clock, my_version.vector_clock):
                        # Their version is newer, update myself
                        self.local_put(other_version.key, other_version.value, other_version.vector_clock)
                        synced_count += 1
                    # else: concurrent versions - both keep their versions (conflict)
        
        return synced_count
    
    def handle_get_all_keys(self):
        """
        Handle GetAllKeys request - return all keys this node stores.
        Used for Merkle tree comparison.
        
        Returns:
            List of all keys with their versions
        """
        all_keys = []
        for key, versions in self.storage.items():
            all_keys.append({
                'key': key,
                'versions': versions
            })
        return all_keys
    
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
            response = stub.CoordinateGet(request, timeout=1)  # Fast timeout for quick failover
            
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
            # Coordinator is unreachable - mark as dead immediately and retry
            print(f"  ✗ Coordinator {coordinator_id} unreachable: {e}")
            print(f"  🔄 Marking {coordinator_id} as dead and retrying with new coordinator...")
            self.ring.mark_node_dead(coordinator_id)
            
            # Retry with new coordinator (will pick next alive node)
            return self.coordinated_get(key)
    
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
    
    def GetAllKeys(self, request, context):
        """
        Handle GetAllKeys request for Merkle tree comparison.
        Returns all keys this node stores.
        """
        all_keys_data = self.node.handle_get_all_keys()
        
        # Convert to proto
        keys_proto = []
        for key_data in all_keys_data:
            # Convert versions to proto
            versions_proto = []
            for vv in key_data['versions']:
                vc_proto = dynamo_pb2.VectorClock(clock=vv.vector_clock.to_dict())
                vv_proto = dynamo_pb2.VersionedValue(
                    value=vv.value,
                    vector_clock=vc_proto
                )
                versions_proto.append(vv_proto)
            
            # Create KeyData proto
            key_proto = dynamo_pb2.KeyData(
                key=key_data['key'],
                versions=versions_proto
            )
            keys_proto.append(key_proto)
        
        return dynamo_pb2.GetAllKeysResponse(keys=keys_proto)
    
    def ReceiveHint(self, request, context):
        """
        Handle hinted handoff from another node.
        This node was down and is now receiving data it missed.
        """
        # Convert proto vector clock to VectorClock object
        vc = VectorClock.from_dict(dict(request.vector_clock.clock))
        
        # Store the hint as regular data
        success = self.node.receive_hint(request.key, request.value, vc)
        
        return dynamo_pb2.HintResponse(success=success)


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