import hashlib
import bisect
# from "./node.py" import Node
def hash_string(input_string:str)->int:
    return int(hashlib.md5(input_string.encode()).hexdigest(),16)
class HashRing:
    def __init__(self,virtual_nodes=100):  
        self.ring = []  # list of (position, node_id) including virtual nodes (list)
        self.nodes = {} # node_id -> capacity mapping (dictionary)
        self.virtual_nodes = virtual_nodes

    def add_node(self, node_id,capacity):
        vnodes=(capacity/100)*self.virtual_nodes
        node=Node(node_id,capacity)

        self.nodes[node_id]=node
        for i in range(int(vnodes)):
            vnode_id=f"{node_id}:{i}"
            pos=hash_string(vnode_id)
            self.ring.append((pos,node_id))
        self.ring.sort(key=lambda x:x[0])

    def get_node_object(self, node_id):
        return self.nodes.get(node_id)    
    
    # Adding put method to store key-value pairs with replication
    def put(self,key,value,N=3,W=2):
        preference_list=self.get_preference_list(key,N)
        if not preference_list:
            print(f"Error: No nodes available")
            return 0
        successful_puts=0
        for node_id in preference_list:
            node=self.get_node_object(node_id)
            if node and node.put(key,value):
                successful_puts+=1
        if successful_puts>=W:
            return True
        print(f"Error: Only {successful_puts} puts succeeded, required {W}")
        return False

    def get(self,key,N=3,R=2):
        preference_list=self.get_preference_list(key,N)
        if not preference_list:
            print(f"Error: No nodes available")
            return None
        values=[]
        for node_id in preference_list:
            node=self.get_node_object(node_id)
            if node:
                value=node.get(key)
                if value is not None:
                    values.append(value)
            if len(values)>=R:
            
            else:
                print("Error: Not enough replicas found")
                
        # print(f"Key '{key}' not found in any of the preference nodes")
        
    def get_node(self,key):
        key_hash=hash_string(key)
        
        positions=[pos for pos,_ in self.ring]
        index=bisect.bisect_right(positions,key_hash)
        if index==len(self.ring):
            index=0
        return self.ring[index][1]
    
    def remove_node(self,node_id):
        if node_id in self.nodes:
            del self.nodes[node_id]
        self.ring=[(pos,nid) for pos,nid in self.ring if nid!=node_id]
    def get_node_distribution(self, num_keys=1000):
            """
            Test function: Check how keys are distributed across nodes.
            Returns a dict of {node_id: count}
            """
            distribution = {node_id: 0 for node_id in self.nodes}
            
            for i in range(num_keys):
                test_key = f"key_{i}"
                node = self.get_node(test_key)
                if node:
                    distribution[node] += 1
            
            return distribution
    
    def get_preference_list(self,key,N=3):
        keyhash=hash_string(key)
        positions=[pos for pos,_ in self.ring]
        index=bisect.bisect_right(positions,keyhash)
        if index == len(self.ring):
            index = 0
        attempts=0
        seen=set()
        preference_list=[]
        while len(preference_list)<N and attempts<len(self.ring):
            curr_index=(index+attempts)%len(self.ring)
            node_id=self.ring[curr_index][1]
            if node_id not in seen:
                seen.add(node_id)
                preference_list.append(node_id)
            attempts+=1
        return preference_list

    
if __name__ == "__main__":
    ring=HashRing(virtual_nodes=100)
    ring.add_node("node_A", capacity=100)  # Gets 10 virtual nodes
    ring.add_node("node_B", capacity=250)  # Gets 20 virtual nodes
    ring.add_node("node_C", capacity=100)   # Gets 5 virtual nodes
    ring.add_node("node_D", capacity=200)
    ring.add_node("node_E", capacity=50)

    test_keys = ["cart_123", "user_456", "session_789", "order_101", "product_202", "item_303", "category_404", "review_505", "wishlist_606", "coupon_707"]

# distribution test
    # for key in test_keys:
    #     assigned_node = ring.get_node(key)
    #     print(f"Key '{key}' is assigned to node '{assigned_node}'")
    # print("\n--- Distribution of 1000 keys ---")
    # distribution = ring.get_node_distribution(1000)
    # for node_id, count in distribution.items():
    #     capacity = ring.nodes[node_id]
    #     percentage = (count / 1000) * 100
    #     print(f"{node_id} (capacity {capacity}): {count} keys ({percentage:.1f}%)")

    # for num_keys in [100, 1000, 10000]:
    #     distribution = ring.get_node_distribution(num_keys)
    #     print(f"\n--- {num_keys} keys ---")
    #     for node_id, count in distribution.items():
    #         capacity = ring.nodes[node_id]
    #         percentage = (count / num_keys) * 100
    #         expected = (capacity / 350) * 100
    #         print(f"{node_id}: {percentage:.1f}% (expected {expected:.1f}%)")
    
    print("\n--- Preference Lists ---")
    for key in ["cart_123", "user_456", "session_789"]:
        pref = ring.get_preference_list(key, N=3)
        print(f"Key '{key}' → replicas: {pref}")